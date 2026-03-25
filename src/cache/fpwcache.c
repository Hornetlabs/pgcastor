#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "port/thread/thread.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "catalog/control.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "cache/fpwcache.h"

/*
 * FPW cache is maintained jointly by PARSER and write thread, needs locking
 */
HTAB* fpwcache_init(transcache* transcache)
{
    HTAB*   result = NULL;
    HASHCTL hashCtl = {'\0'};

    transcache->fpwtupleslist = NULL;

    hashCtl.keysize = sizeof(ReorderBufferFPWKey);
    hashCtl.entrysize = sizeof(ReorderBufferFPWEntry);

    result = hash_create("HASH_FPW_TUPLE", 128, &hashCtl, HASH_ELEM | HASH_BLOBS);

    return result;
}

/* In this function, all pointers are malloc'd, so after calling this function, unused input
 * parameters should be freed */
void fpwcache_add(transcache* transcache, ReorderBufferFPWKey* key, ReorderBufferFPWEntry* entry)
{
    bool                   find = false;
    ReorderBufferFPWKey    nodekey = {0};
    ReorderBufferFPWEntry* search_result = NULL;
    ReorderBufferFPWNode*  tuplenode = rmalloc1(sizeof(ReorderBufferFPWNode));
    if (NULL == tuplenode)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tuplenode, 0, '\0', sizeof(ReorderBufferFPWNode));

    nodekey.blcknum = key->blcknum;
    nodekey.itemoffset = key->itemoffset;
    nodekey.relfilenode = key->relfilenode;

    /* Set key information */
    rmemcpy1(&tuplenode->key, 0, &nodekey, sizeof(ReorderBufferFPWKey));
    rmemcpy1(&tuplenode->key, 0, &nodekey, sizeof(ReorderBufferFPWKey));
    tuplenode->lsn = entry->lsn;

    elog(RLOG_DEBUG, "add2fpwcache relfilenode:%u, %u. %u, lsn:%X/%X", nodekey.relfilenode,
         nodekey.blcknum, nodekey.itemoffset, (uint32)(entry->lsn >> 32), (uint32)(entry->lsn));

    search_result = hash_search(transcache->by_fpwtuples, key, HASH_ENTER, &find);
    if (find)
    {
        /* Update operation, only need to update len, lsn, data*/
        if (search_result->len < entry->len)
        {
            rfree(search_result->data);
            search_result->data = NULL;
            search_result->data = rmalloc0(entry->len);
            if (NULL == search_result->data)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(search_result->data, 0, 0, entry->len);
        }
        else
        {
            rmemset0(search_result->data, 0, 0, search_result->len);
        }

        /* Value copy */
        search_result->len = entry->len;
        search_result->lsn = entry->lsn;
        rmemcpy0(search_result->data, 0, entry->data, entry->len);
    }
    else
    {
        /* Insert operation */
        search_result->blcknum = key->blcknum;
        search_result->itemoffset = key->itemoffset;
        search_result->relfilenode = key->relfilenode;
        search_result->lsn = entry->lsn;
        search_result->len = entry->len;
        search_result->data = rmalloc0(entry->len);
        if (NULL == search_result->data)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(search_result->data, 0, 0, entry->len);
        rmemcpy0(search_result->data, 0, entry->data, entry->len);
    }
    transcache->fpwtupleslist = lappend(transcache->fpwtupleslist, tuplenode);
}

/* Delete */
static void fpwcache_removebyredolsn(transcache* transcache, XLogRecPtr redolsn)
{
    ListCell*              lc = NULL;
    List*                  newlist = NULL;
    ReorderBufferFPWNode*  tuplenode = NULL;
    ReorderBufferFPWEntry* fpwentry = NULL;

    elog(RLOG_DEBUG, "fpwcache_removebyredolsn, redolsn:%X/%X", (uint32)(redolsn >> 32),
         (uint32)redolsn);

    foreach (lc, transcache->fpwtupleslist)
    {
        tuplenode = (ReorderBufferFPWNode*)lfirst(lc);
        if (tuplenode->lsn < redolsn)
        {
            fpwentry = hash_search(transcache->by_fpwtuples, &tuplenode->key, HASH_FIND, NULL);
            if (NULL != fpwentry)
            {
                if (fpwentry->lsn < redolsn)
                {
                    if (NULL != fpwentry->data)
                    {
                        rfree(fpwentry->data);
                    }

                    elog(RLOG_DEBUG,
                         "removefromfpwcache relfilenode:%u, %u. %u, lsn:%X/%X, redolsn:%X/%X",
                         tuplenode->key.relfilenode, tuplenode->key.blcknum,
                         tuplenode->key.itemoffset, (uint32)(tuplenode->lsn >> 32),
                         (uint32)tuplenode->lsn, (uint32)(redolsn >> 32), (uint32)redolsn);
                    hash_search(transcache->by_fpwtuples, &tuplenode->key, HASH_REMOVE, NULL);
                }
            }
            else
            {
                elog(RLOG_DEBUG,
                     "removefromfpwcache relfilenode NULL:%u, %u. %u,lsn:%X/%X, redolsn:%X/%X",
                     tuplenode->key.relfilenode, tuplenode->key.blcknum, tuplenode->key.itemoffset,
                     (uint32)(tuplenode->lsn >> 32), (uint32)tuplenode->lsn,
                     (uint32)(redolsn >> 32), (uint32)redolsn);
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

/* Traverse transcache->chkpts to check less than restartlsn, clean up fpw and update redo */
void fpwcache_calcredolsnbyrestartlsn(transcache* transcache, XLogRecPtr restartlsn,
                                      XLogRecPtr* redolsn)
{
    XLogRecPtr      curredolsn = InvalidXLogRecPtr;
    checkpointnode* chkptnode = NULL;     /* Data greater than restartlsn */
    checkpointnode* chkptnodeprev = NULL; /* Data less than restartlsn */

    curredolsn = *redolsn;
    for (chkptnode = chkptnodeprev = transcache->chkpts->head; NULL != chkptnode;
         chkptnode = chkptnode->next)
    {
        if (restartlsn > chkptnode->redolsn)
        {
            chkptnodeprev = chkptnode;
            continue;
        }
        break;
    }

    /* First one, not processed */
    if (chkptnode == chkptnodeprev)
    {
        return;
    }

    /* Check if same as recorded, if same not processed */
    if (curredolsn == chkptnodeprev->redolsn)
    {
        return;
    }

    /* Clean up chkpts data */
    /* Remove data before prev from data */
    chkptnode = transcache->chkpts->head;
    transcache->chkpts->head = chkptnodeprev;
    chkptnodeprev->prev->next = NULL;
    chkptnodeprev->prev = NULL;

    for (chkptnodeprev = chkptnode; NULL != chkptnodeprev; chkptnodeprev = chkptnode)
    {
        chkptnode = chkptnodeprev->next;
        rfree(chkptnodeprev);
    }

    /* Clean up fpw data */
    fpwcache_removebyredolsn(transcache, transcache->chkpts->head->redolsn);

    /* Set new redolsn */
    *redolsn = transcache->chkpts->head->redolsn;
}
