#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "port/thread/ripple_thread.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "catalog/ripple_control.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "cache/ripple_fpwcache.h"

/*
 * FPW 缓存由 PARSER 与 写线程共同维护，需要锁
*/
HTAB *ripple_fpwcache_init(ripple_transcache *transcache)
{
    HTAB *result = NULL;
    HASHCTL hashCtl = {'\0'};

    transcache->fpwtupleslist = NULL;

    hashCtl.keysize = sizeof(ReorderBufferFPWKey);
    hashCtl.entrysize = sizeof(ReorderBufferFPWEntry);

    result = hash_create("HASH_FPW_TUPLE",
                          128,
                         &hashCtl,
                          HASH_ELEM | HASH_BLOBS);

    return result;
}

/* 该函数中, 所有指针都是malloc的, 因此调用完该函数后, 应释放无用的入参 */
void ripple_fpwcache_add(ripple_transcache *transcache,
                            ReorderBufferFPWKey *key,
                            ReorderBufferFPWEntry *entry)
{
    bool find = false;
    ReorderBufferFPWKey nodekey = { 0 };
    ReorderBufferFPWEntry *search_result = NULL;
    ReorderBufferFPWNode *tuplenode = rmalloc1(sizeof(ReorderBufferFPWNode));
    if(NULL == tuplenode)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tuplenode, 0, '\0', sizeof(ReorderBufferFPWNode));

    nodekey.blcknum = key->blcknum;
    nodekey.itemoffset = key->itemoffset;
    nodekey.relfilenode = key->relfilenode;

    /* 设置key信息 */
    rmemcpy1(&tuplenode->key, 0, &nodekey, sizeof(ReorderBufferFPWKey));
    rmemcpy1(&tuplenode->key, 0, &nodekey, sizeof(ReorderBufferFPWKey));
    tuplenode->lsn = entry->lsn;

    elog(RLOG_DEBUG, "add2fpwcache relfilenode:%u, %u. %u, lsn:%X/%X",
                    nodekey.relfilenode,
                    nodekey.blcknum,
                    nodekey.itemoffset,
                    (uint32)(entry->lsn>>32), (uint32)(entry->lsn));

    search_result = hash_search(transcache->by_fpwtuples, key, HASH_ENTER, &find);
    if (find)
    {
        /* 做更新操作, 只需要更新len, lsn, data*/
        if(search_result->len < entry->len)
        {
            rfree(search_result->data);
            search_result->data = NULL;
            search_result->data = rmalloc0(entry->len);
            if(NULL == search_result->data)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(search_result->data, 0, 0, entry->len);
        }
        else
        {
            rmemset0(search_result->data, 0, 0, search_result->len);
        }

        /* 值复制 */
        search_result->len = entry->len;
        search_result->lsn = entry->lsn;
        rmemcpy0(search_result->data, 0, entry->data, entry->len);
    }
    else
    {
        /* 插入操作 */
        search_result->blcknum = key->blcknum;
        search_result->itemoffset = key->itemoffset;
        search_result->relfilenode = key->relfilenode;
        search_result->lsn = entry->lsn;
        search_result->len = entry->len;
        search_result->data = rmalloc0(entry->len);
        if(NULL == search_result->data)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(search_result->data, 0, 0, entry->len);
        rmemcpy0(search_result->data, 0, entry->data, entry->len);
    }
    transcache->fpwtupleslist = lappend(transcache->fpwtupleslist, tuplenode);
}

/* 删除 */
static void ripple_fpwcache_removebyredolsn(ripple_transcache *transcache, XLogRecPtr redolsn)
{
    ListCell* lc = NULL;
    List* newlist = NULL;
    ReorderBufferFPWNode *tuplenode = NULL;
    ReorderBufferFPWEntry *fpwentry = NULL;

    elog(RLOG_DEBUG, "fpwcache_removebyredolsn, redolsn:%X/%X",
                        (uint32)(redolsn>>32), (uint32)redolsn);

    foreach(lc, transcache->fpwtupleslist)
    {
        tuplenode = (ReorderBufferFPWNode *)lfirst(lc);
        if(tuplenode->lsn < redolsn)
        {
            fpwentry = hash_search(transcache->by_fpwtuples, &tuplenode->key, HASH_FIND, NULL);
            if(NULL != fpwentry)
            {
                if(fpwentry->lsn < redolsn)
                {
                    if(NULL != fpwentry->data)
                    {
                        rfree(fpwentry->data);
                    }

                    elog(RLOG_DEBUG, "removefromfpwcache relfilenode:%u, %u. %u, lsn:%X/%X, redolsn:%X/%X",
                                    tuplenode->key.relfilenode,
                                    tuplenode->key.blcknum,
                                    tuplenode->key.itemoffset,
                                    (uint32)(tuplenode->lsn>>32), (uint32)tuplenode->lsn,
                                    (uint32)(redolsn>>32), (uint32)redolsn);
                    hash_search(transcache->by_fpwtuples, &tuplenode->key, HASH_REMOVE, NULL);
                }
            }
            else
            {
                elog(RLOG_DEBUG, "removefromfpwcache relfilenode NULL:%u, %u. %u,lsn:%X/%X, redolsn:%X/%X",
                                    tuplenode->key.relfilenode,
                                    tuplenode->key.blcknum,
                                    tuplenode->key.itemoffset,
                                    (uint32)(tuplenode->lsn>>32), (uint32)tuplenode->lsn,
                                    (uint32)(redolsn>>32), (uint32)redolsn);
            }

            rfree(tuplenode);
            continue;
        }
        else
        {
            
        }

        newlist = lappend(newlist, tuplenode);
    }

    list_free(transcache->fpwtupleslist);
    transcache->fpwtupleslist = newlist;
    return;
}

/* 遍历transcache->chkpts查看小于restartlsn清理fpw并更新redo */
void ripple_fpwcache_calcredolsnbyrestartlsn(ripple_transcache *transcache,
                                            XLogRecPtr restartlsn,
                                            XLogRecPtr* redolsn)
{
    XLogRecPtr curredolsn = InvalidXLogRecPtr;
    ripple_checkpointnode* chkptnode = NULL;                /* 大于 restartlsn 的数据 */
    ripple_checkpointnode* chkptnodeprev = NULL;            /* 小于 restartlsn 的数据 */

    curredolsn = *redolsn;
    for(chkptnode = chkptnodeprev = transcache->chkpts->head; NULL != chkptnode; chkptnode= chkptnode->next)
    {
        if(restartlsn > chkptnode->redolsn)
        {
            chkptnodeprev = chkptnode;
            continue;
        }
        break;
    }

    /* 首个，不处理 */
    if(chkptnode == chkptnodeprev)
    {
        return;
    }

    /* 是否与记录的相同，相同则不处理 */
    if(curredolsn == chkptnodeprev->redolsn)
    {
        return;
    }

    /* 清理 chkpts 数据 */
    /* 将 prev 之前的数据从数据中摘除 */
    chkptnode = transcache->chkpts->head;
    transcache->chkpts->head = chkptnodeprev;
    chkptnodeprev->prev->next = NULL;
    chkptnodeprev->prev = NULL;

    for(chkptnodeprev = chkptnode; NULL != chkptnodeprev; chkptnodeprev = chkptnode)
    {
        chkptnode = chkptnodeprev->next;
        rfree(chkptnodeprev);
    }

    /* 清理fpw数据  */
    ripple_fpwcache_removebyredolsn(transcache, transcache->chkpts->head->redolsn);

    /* 设置新的 redolsn */
    *redolsn = transcache->chkpts->head->redolsn;
}
