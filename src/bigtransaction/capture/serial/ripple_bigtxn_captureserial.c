#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_smgr.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "serial/ripple_serial.h"
#include "works/ripple_workthreadmgr.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/ripple_bigtxn.h"
#include "bigtransaction/capture/serial/ripple_bigtxn_captureserial.h"
#include "misc/ripple_misc_control.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "catalog/ripple_catalog.h"
#include "stmts/ripple_txnstmt.h"

/*---------------------------callback begin----------------------------------*/

/* 获取数据库的名称, 先查找事务内的缓存, 再查找全局缓存 */
static char* ripple_bigtxn_captureserial_getdbname(void* serial, Oid oid)
{
    ripple_bigtxn_captureserial* cserial = NULL;
    xk_pg_parser_sysdict_pgdatabase *database = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;

    database = ripple_catalog_get_database_sysdict(cserial->dicts->by_database,
                                                   NULL,
                                                   cserial->lasttxn->txndicts,
                                                   oid);
    if (!database)
    {
        elog(RLOG_ERROR, "can't find database by oid: %u", oid);
    }
    return database->datname.data;
}

/* 根据 oid 获取模式 */
static void* ripple_bigtxn_captureserial_getnamespace(void* serial, Oid oid)
{
    /* 
     * 首先在当前事务中获取，当前事务中找不到，在 dicts 中获取
     */
    ripple_bigtxn_captureserial* cserial = NULL;
    void *namespace = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;

    namespace = ripple_catalog_get_namespace_sysdict(cserial->dicts->by_namespace,
                                                     NULL,
                                                     cserial->lasttxn->txndicts,
                                                     oid);

    if (!namespace)
    {
        elog(RLOG_ERROR, "can't find namespace by oid: %u", oid);
    }

    return namespace;
}

/* 根据 oid 获取表 */
static void* ripple_bigtxn_captureserial_getclass(void* serial, Oid oid)
{
    /* 
     * 首先在当前事务中获取，当前事务中找不到，在 dicts 中获取
     */

    ripple_bigtxn_captureserial* cserial = NULL;
    void *class = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;

    class = ripple_catalog_get_class_sysdict(cserial->dicts->by_class,
                                                     NULL,
                                                     cserial->lasttxn->txndicts,
                                                     oid);

    if (!class)
    {
        elog(RLOG_ERROR, "can't find class by oid: %u", oid);
    }

    return class;
}

/* 大事务根据 oid 获取索引信息, 返回为链表 */
static void* ripple_bigtxn_captureserial_getindex(void* serial, Oid oid)
{
    ripple_bigtxn_captureserial* cserial = NULL;
    void *index = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;

    index = ripple_catalog_get_index_sysdict_list(cserial->dicts->by_index,
                                                  NULL,
                                                  cserial->lasttxn->txndicts,
                                                  oid);

    return index;
}

/* 根据 oid 获取列属性 */
static void* ripple_bigtxn_captureserial_getatrrs(void* serial, Oid oid)
{
    /* 
     * 为了确保准确性, his优先级大于全局缓存, 返回值list用完后需要释放
     */
    ripple_bigtxn_captureserial* cserial = NULL;
    xk_pg_parser_sysdict_pgclass *class = NULL;
    int index_attrs = 0;
    int natts = 0;
    List *result = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;

    /* 查找pg_class */
    class = ripple_bigtxn_captureserial_getclass(serial, oid);
    natts = class->relnatts;

    for (index_attrs = 0; index_attrs < natts; index_attrs++)
    {
        void *temp_att = NULL;
        temp_att = ripple_catalog_get_attribute_sysdict(cserial->dicts->by_attribute,
                                                      NULL,
                                                      cserial->lasttxn->txndicts,
                                                      oid,
                                                      index_attrs + 1);
        if (!temp_att)
        {
            elog(RLOG_ERROR, "can't find pg_attribute relation");
        }
        result = lappend(result, temp_att);
    }

    return result;
}

/* 根据 oid 获取类型 */
static void* ripple_bigtxn_captureserial_gettype(void* serial, Oid oid)
{
    /* 
     * 首先在当前事务中获取，当前事务中找不到，在 dicts 中获取
     */

    ripple_bigtxn_captureserial* cserial = NULL;
    void *type = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;

    type = ripple_catalog_get_type_sysdict(cserial->dicts->by_type,
                                           NULL,
                                           cserial->lasttxn->txndicts,
                                           oid);

    if (!type)
    {
        elog(RLOG_ERROR, "can't find type by oid: %u", oid);
    }

    return type;
}

/* txnmetadata系统字典应用 */
static void ripple_bigtxn_captureserial_transcatalog2transcache(void* serial, void* catalog)
{
    /* 这里维护大事务的事务内sysdict链表 */
    List *dict                              = NULL;
    ripple_catalogdata *catalogdata         = NULL;
    ripple_bigtxn_captureserial* cserial    = NULL;

    cserial = (ripple_bigtxn_captureserial*)serial;
    catalogdata = (ripple_catalogdata*)lfirst((ListCell *)catalog);
    dict = cserial->lasttxn->txndicts;

    /* 拷贝系统表 */
    dict = lappend(dict, ripple_catalog_copy(catalogdata));

    cserial->lasttxn->txndicts = dict;
}

/* sysdicthis系统字典应用 */
static void ripple_bigtxn_captureserial_sysdicthis2sysdict(ripple_bigtxn_captureserial* cserial, List *his)
{
    if (NULL == his)
    {
        return;
    }

    /* 可以简单套一层, 内部基本只用了sysdict和relfilenode, 且relfilenode有空值判断 */
    ripple_cache_sysdicts_txnsysdicthis2cache(cserial->dicts, his);
}

/*---------------------------callback end -----------------------------------*/

/* 初始化 */
ripple_bigtxn_captureserial* ripple_bigtxn_captureserial_init(void)
{
    int mbytes                          = 0;
    uint64 bytes                        = 0;
    ripple_bigtxn_captureserial* cserial = NULL;
    HASHCTL hash_ctl;

    cserial= (ripple_bigtxn_captureserial*)rmalloc0(sizeof(ripple_bigtxn_captureserial));
    if(NULL == cserial)
    {
        elog(RLOG_ERROR, "big transaction capture serial out of memory, %s", strerror(errno));
    }
    rmemset0(cserial, 0, '\0', sizeof(ripple_bigtxn_captureserial));
    cserial->lasttxn = NULL;
    cserial->bigtxn2serial = ripple_cache_txn_init();
    cserial->by_txns = NULL;
    cserial->dicts = NULL;

    /* 序列化初始化 */
    ripple_serialstate_init(&cserial->base);

    cserial->base.txn2filebuffer = ripple_file_buffer_init();

    /* 创建事务 hash */
    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(FullTransactionId);
    hash_ctl.entrysize = sizeof(ripple_bigtxn);
    cserial->by_txns = hash_create("transaction hash",
                                    8192,
                                    &hash_ctl,
                                    HASH_ELEM | HASH_BLOBS);

    cserial->base.ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
    cserial->base.ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    cserial->base.ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);
    ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, cserial->base.ffsmgrstate);
    return cserial;
}

/* 在系统字典获取dboid */
static Oid ripple_bigtxn_captureserial_getdboid(void* inserial)
{
    return ripple_misc_controldata_database_get(inserial);
}

static void ripple_bigtxn_captureserial_freeattributes(void* attrs)
{
    List *list = (List *)attrs;

    /* 仅释放list, 不关注内容 */
    list_free(list);
}

/* 将 entry 数据落盘 */
static void ripple_bigtxn_captureserial_txn2disk(ripple_serialstate* serialstate, ripple_txn* txn)
{
    bool first = true;
    bool txnformetadata = true; /* 用于标识当前事务中只含有metadata */
    ListCell* lc = NULL;
    ripple_ff_txndata       txndata = { {0} };

     /* 
      * 组装事务信息
     */
    if(NULL == txn->stmts)
    {
        return;
    }

    /* 调用格式化接口，进行格式化处理 */
    /* 当一个事务中只有metadata时,那么此事务不需要落盘 */
    foreach(lc, txn->stmts)
    {
        ripple_txnstmt* rstmt = (ripple_txnstmt*)lfirst(lc);
        rmemset1(&txndata, 0, '\0', sizeof(ripple_ff_txndata));
        txndata.data = rstmt;
        rstmt->database = serialstate->database;
        txndata.header.type = RIPPLE_FF_DATA_TYPE_TXN;
        txndata.header.transid = txn->xid;

ripple_bigtxn_captureserial_txn2disk_reset:
        if(false == txnformetadata)
        {
            if(1 == list_length(txn->stmts))
            {
                if( RIPPLE_TXN_TYPE_BIGTXN_BEGIN == txn->type)
                {
                    first = false;
                    txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_START;
                }
                else if (RIPPLE_FROZEN_TXNID == txn->xid)
                {
                    txndata.header.transind = (RIPPLE_FF_DATA_TRANSIND_START | RIPPLE_FF_DATA_TRANSIND_IN );
                }
                else
                {
                    txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_IN;
                }
            }
            else
            {
                if(true == first && RIPPLE_TXN_TYPE_BIGTXN_BEGIN == txn->type)
                {
                    first = false;
                    txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_START;
                }
                else
                {
                    txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_IN;
                }
            }
        }
        else
        {
            if(RIPPLE_TXNSTMT_TYPE_METADATA == rstmt->type)
            {
                /* metadata 标识为开始,后面就不会产生 commit 了 */
                txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_START;
            }
            else
            {
                txnformetadata = false;
                goto ripple_bigtxn_captureserial_txn2disk_reset;
            }
        }
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_DATA, (void*)&txndata, serialstate->ffsmgrstate);
    }
}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static void ripple_capture_serial_buffer2waitflush(ripple_bigtxn_captureserial *cserial, ripple_txn* txn)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */
    int             oldflag = 0;
    int             bufid = 0;
    int             timeout = 0;

    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* foldbuffer = NULL;
    ripple_serialstate* serialstate = NULL;
    serialstate = &cserial->base;

    foldbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if(0 == foldbuffer->start)
    {
        return;
    }
    oldflag = foldbuffer->flag;

    /* 获取新的 buffer 缓存 */
    while(1)
    {
        bufid = ripple_file_buffer_get(serialstate->txn2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            return;
        }
        break;
    }

    fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, bufid);
    if(NULL == fbuffer->privdata)
    {
        finfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
        if(NULL == finfo)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(finfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ripple_ff_fileinfo*)fbuffer->privdata;
    }

    rmemcpy0(fbuffer->data, 0, foldbuffer->data, foldbuffer->start);
    fbuffer->start = foldbuffer->start;

    /* 设置新 buffer 的其它信息 */
    rmemcpy0(finfo, 0, (ripple_ff_fileinfo*)foldbuffer->privdata, sizeof(ripple_ff_fileinfo));

    /* 设置 oldbuffer 的信息 */
    foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
    foldbuffer->extra.rewind.fileaddr.trail.offset = (((finfo->blknum - 1) * RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start);
    foldbuffer->extra.rewind.curtlid = cserial->callback.bigtxn_parserstat_curtlid_get(cserial->privdata);
    /* 将 oldbuffer 放入到待刷新缓存中 */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

    fbuffer->flag = oldflag;
    serialstate->ffsmgrstate->bufid = bufid;
    return;
}

static void ripple_bigtxn_captureserial_set_callback(ripple_bigtxn_captureserial *cserial)
{
    cserial->base.ffsmgrstate->callback.getdboid = ripple_bigtxn_captureserial_getdboid;
    cserial->base.ffsmgrstate->callback.getdbname = ripple_bigtxn_captureserial_getdbname;
    cserial->base.ffsmgrstate->callback.getfilebuffer = ripple_serialstate_getfilebuffer;
    cserial->base.ffsmgrstate->callback.getclass = ripple_bigtxn_captureserial_getclass;
    cserial->base.ffsmgrstate->callback.getindex = ripple_bigtxn_captureserial_getindex;
    cserial->base.ffsmgrstate->callback.getnamespace = ripple_bigtxn_captureserial_getnamespace;
    cserial->base.ffsmgrstate->callback.getattributes = ripple_bigtxn_captureserial_getatrrs;
    cserial->base.ffsmgrstate->callback.gettype = ripple_bigtxn_captureserial_gettype;
    cserial->base.ffsmgrstate->callback.catalog2transcache = ripple_bigtxn_captureserial_transcatalog2transcache;
    cserial->base.ffsmgrstate->callback.freeattributes = ripple_bigtxn_captureserial_freeattributes;
    cserial->base.ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    cserial->base.ffsmgrstate->callback.setredosysdicts = NULL;
    cserial->base.ffsmgrstate->callback.setdboid = NULL;
    cserial->base.ffsmgrstate->callback.getrecords = NULL;
    cserial->base.ffsmgrstate->callback.getparserstate = NULL;
}

/* ffsmgrstate信息填充 */
static void ripple_bigtxn_captureserial_initserial(ripple_serialstate* serialstate, int serialtype)
{
    int             mbytes = 0;
    uint64          bytes = 0;

    serialstate->ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
    serialstate->ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    serialstate->ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);
}

/* 在 hash 中初始化大事务 */
static bool ripple_bigtxn_captureserial_initbigtxn(ripple_bigtxn_captureserial* cserial, ripple_txn* txn)
{
    bool found                      = false;
    ripple_bigtxn* htxn             = NULL;
    ripple_file_buffer* fbuffer     = NULL;

    /* 大事务的开始, 那么查看是否含有 */
    htxn = hash_search(cserial->by_txns, &txn->xid, HASH_ENTER, &found);
    if(true == found)
    {
        /* 需要重置标志 */
        elog(RLOG_WARNING, "big transaction capture serial txn already in the hash, %lu", htxn->xid);
        return false;
    }
    htxn->xid = txn->xid;
    if(false == ripple_bigtxn_reset(htxn))
    {
        /* 需要重置标志 */
        elog(RLOG_WARNING, "big transaction capture reset error, %lu", htxn->xid);
        return false;
    }

    /* 调用初始化接口 */
    cserial->base.ffsmgrstate->fdata = NULL;
    cserial->base.ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL,
                                                    cserial->base.ffsmgrstate);
    htxn->fdata = cserial->base.ffsmgrstate->fdata;

    /* 大事务开始时重置buffer, 首先将旧buffer放入空闲队列 */
    if (RIPPLE_INVALID_BUFFERID != cserial->base.ffsmgrstate->bufid)
    {
        /* 做切换 */
        fbuffer = ripple_file_buffer_getbybufid(cserial->base.txn2filebuffer, cserial->base.ffsmgrstate->bufid);

        /* 做 copy */
        if(NULL != cserial->lasttxn)
        {
            ripple_file_buffer_copy(fbuffer, &cserial->lasttxn->fbuffer);
            ripple_file_buffer_free(cserial->base.txn2filebuffer, fbuffer);
            cserial->lasttxn = NULL;
        }
    }

    /* 设置从0, 0开始, 在该函数中重置了 ffsmgrstate->bufid */
    ripple_serialstate_fbuffer_set(&cserial->base, 0, 0, txn->xid);
    cserial->lasttxn = htxn;
    return true;
}

/* 切换事务 */
static bool ripple_bigtxn_captureserial_shiftbigtxn(ripple_bigtxn_captureserial* cserial, ripple_txn* txn)
{
    bool found                      = false;
    int timeout                     = 0;
    ripple_bigtxn* htxn             = NULL;
    ripple_file_buffer* fbuffer     = NULL;

    if(NULL != cserial->lasttxn)
    {
        if(txn->xid == cserial->lasttxn->xid)
        {
            htxn = cserial->lasttxn;
            return true;
        }

        /* 保留上个事务的信息 */
        fbuffer = ripple_file_buffer_getbybufid(cserial->base.txn2filebuffer, cserial->base.ffsmgrstate->bufid);

        /* 做 copy */
        ripple_file_buffer_copy(fbuffer, &cserial->lasttxn->fbuffer);
        ripple_file_buffer_free(cserial->base.txn2filebuffer, fbuffer);
        cserial->lasttxn = NULL;
    }

    /* 在大事务中查找 */
    htxn = hash_search(cserial->by_txns, &txn->xid, HASH_FIND, &found);
    if(false == found)
    {
        /* 需要重置标志 */
        elog(RLOG_WARNING, "big transaction capture serial txn %lu not in the hash", txn->xid);
        return false;
    }

    /* 
     * 设置为新的
     *  获取一个新的空闲bufferid
     *  设置该 buffer 信息
     */
    while(1)
    {
        htxn->fbuffer.bufid = ripple_file_buffer_get(cserial->base.txn2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == htxn->fbuffer.bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "capture big txn serial get buffer error");
            return false;
        }
        break;
    }

    /* 获取新的 fbuffer */
    fbuffer = ripple_file_buffer_getbybufid(cserial->base.txn2filebuffer, htxn->fbuffer.bufid);
    ripple_file_buffer_copy(&htxn->fbuffer, fbuffer);
    cserial->base.ffsmgrstate->bufid = htxn->fbuffer.bufid;
    cserial->base.ffsmgrstate->fdata = htxn->fdata;

    /* htxn保存到lasttxn */
    cserial->lasttxn = htxn;

    return true;
}

/* 大事务结束处理 */
static bool ripple_bigtxn_captureserial_endbigtxn(ripple_bigtxn_captureserial* cserial, ripple_txn* txn)
{
    int flag = 0;
    ripple_file_buffer* fbuffer = NULL;
    ripple_serialstate* serialstate = NULL;
    serialstate = &cserial->base;
    if(RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT != txn->type && RIPPLE_TXN_TYPE_BIGTXN_END_ABORT != txn->type)
    {
        return true;
    }

    /* 调用释放接口 */
    cserial->base.ffsmgrstate->ffsmgr->ffsmgr_free(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL,
                                                    cserial->base.ffsmgrstate);

    if(RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT == txn->type)
    {
        /* 大事务结束, commit, 系统表兑换 */
        ripple_bigtxn_captureserial_sysdicthis2sysdict(cserial, cserial->lasttxn->txndicts);
    }
    flag = RIPPLE_FILE_BUFFER_FLAG_BIGTXNEND;

    /* 获取 fbuffer */
    fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    fbuffer->flag |= flag;
    ripple_file_buffer_waitflush_add(serialstate->txn2filebuffer, fbuffer);
    ripple_bigtxn_clean(cserial->lasttxn);
    hash_search(cserial->by_txns, &txn->xid, HASH_REMOVE, NULL);
    cserial->lasttxn = NULL;
    serialstate->ffsmgrstate->bufid = InvalidFullTransactionId;
    return true;
}

/*
 * 逻辑处理主函数
*/
void* ripple_bigtxn_captureserial_main(void* args)
{
    int timeout                             = 0;
    ripple_txn* txn                         = NULL;
    ripple_bigtxn* htxn                     = NULL;
    ripple_thrnode* thrnode                 = NULL;
    ripple_bigtxn_captureserial* cserial    = NULL;

    thrnode = (ripple_thrnode*)args;
    cserial = (ripple_bigtxn_captureserial*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "capture bigtxn serial stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 加载数据字典 */
    ripple_cache_sysdictsload((void**)&cserial->dicts);

    /* 设置回调函数 */
    ripple_bigtxn_captureserial_set_callback(cserial);

    /* 序列化内容设置 */
    ripple_bigtxn_captureserial_initserial(&cserial->base, RIPPLE_FFSMG_IF_TYPE_TRAIL);
    
    /* 设置回调时使用的主结构体 */
    cserial->base.ffsmgrstate->privdata = cserial;

    while(1)
    {
        /* 
         * 处理流程
         *  1、在队列中获取事务
         *  2、判断事务是否为大事务
         *      2.1 不是大事务，那么只应用系统表到 dicts 中
         *      2.2 是大事务， 序列化大事务
         */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        txn = ripple_cache_txn_get(cserial->bigtxn2serial, &timeout);
        if(NULL == txn)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            elog(RLOG_WARNING, "capture big transaction get txn error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 非大事务,那么只应用系统表数据 */
        if(!RIPPLE_TXN_ISBIGTXN(txn->flag))
        {
            /* 只应用系统表数据 */
            if (txn->sysdictHis)
            {
                ripple_bigtxn_captureserial_sysdicthis2sysdict(cserial, txn->sysdictHis);
            }

            /* txn 内容释放 */
            ripple_txn_free(txn);
            rfree(txn);
            continue;
        }

        /* 大事务数据 */
        if(RIPPLE_TXN_TYPE_BIGTXN_BEGIN == txn->type)
        {
            if(false == ripple_bigtxn_captureserial_initbigtxn(cserial, txn))
            {
                elog(RLOG_WARNING, "capture big txn init big txn error");
                goto ripple_bigtxn_captureserial_main_done;
            }
        }
        else
        {
            if(false == ripple_bigtxn_captureserial_shiftbigtxn(cserial, txn))
            {
                elog(RLOG_WARNING, "capture big txn shift lasttxn error");
                goto ripple_bigtxn_captureserial_main_done;
            }
        }
        htxn = cserial->lasttxn;

        /* 序列化 */
        ripple_bigtxn_captureserial_txn2disk(&cserial->base, txn);

        /* 序列化结束, 强制刷盘 */
        ripple_capture_serial_buffer2waitflush(cserial, txn);

        /* 保存sysdicthis */
        if (htxn->txndicts)
        {
            ripple_cache_sysdicts_txnsysdicthisfree(htxn->txndicts);
            list_free(htxn->txndicts);
            htxn->txndicts = NULL;
        }
        htxn->txndicts = txn->sysdictHis;
        txn->sysdictHis = NULL;

        if(false == ripple_bigtxn_captureserial_endbigtxn(cserial, txn))
        {
            elog(RLOG_WARNING, "big txn capture serial end big transaction error");
            goto ripple_bigtxn_captureserial_main_done;
        }

        /* 结束, txn 内容释放 */
        ripple_txn_free(txn);
        rfree(txn);
    }

ripple_bigtxn_captureserial_main_done:
    ripple_pthread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void ripple_bigtxn_captureserial_destroy(void* args)
{
    HASH_SEQ_STATUS status;
    ListCell* lc = NULL;
    ripple_bigtxn_captureserial* cserial    = NULL;

    cserial = (ripple_bigtxn_captureserial*) args;
    if(NULL == cserial)
    {
        return;
    }

    ripple_file_buffer_destroy(cserial->base.txn2filebuffer);

    ripple_cache_txn_destroy(cserial->bigtxn2serial);

    ripple_serialstate_destroy((ripple_serialstate*)cserial);

    if(NULL != cserial->dicts)
    {
        if(NULL != cserial->dicts->by_class)
        {
            ripple_catalog_class_value *catalogclassentry;
            hash_seq_init(&status,cserial->dicts->by_class);
            while (NULL != (catalogclassentry = hash_seq_search(&status)))
            {
                if(NULL != catalogclassentry->ripple_class)
                {
                    rfree(catalogclassentry->ripple_class);
                }
            }

            hash_destroy(cserial->dicts->by_class);
        }

        /* attributes 表删除 */
        if(NULL != cserial->dicts->by_attribute)
        {
            ripple_catalog_attribute_value* catalogattrentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_attribute);
            while(NULL != (catalogattrentry = hash_seq_search(&status)))
            {
                if(NULL != catalogattrentry->attrs)
                {
                    foreach(lc, catalogattrentry->attrs)
                    {
                        rfree(lfirst(lc));
                    }
                    list_free(catalogattrentry->attrs);
                }
            }

            hash_destroy(cserial->dicts->by_attribute);
        }

        /* type 表删除 */
        if(NULL != cserial->dicts->by_type)
        {
            ripple_catalog_type_value* catalogtypeentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_type);
            while(NULL != (catalogtypeentry = hash_seq_search(&status)))
            {
                if(NULL != catalogtypeentry->ripple_type)
                {
                    rfree(catalogtypeentry->ripple_type);
                }
            }

            hash_destroy(cserial->dicts->by_type);
        }

        /* proc 表删除 */
        if(NULL != cserial->dicts->by_proc)
        {
            ripple_catalog_proc_value* catalogprocentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_proc);
            while(NULL != (catalogprocentry = hash_seq_search(&status)))
            {
                if(NULL != catalogprocentry->ripple_proc)
                {
                    rfree(catalogprocentry->ripple_proc);
                }
            }

            hash_destroy(cserial->dicts->by_proc);
        }

        /* tablespace 表删除 */
        if(NULL != cserial->dicts->by_tablespace)
        {
            /* tablespace 表在当前程序中没有用到 */
            hash_destroy(cserial->dicts->by_tablespace);
        }

        /* namespace 表删除 */
        if(NULL != cserial->dicts->by_namespace)
        {
            ripple_catalog_namespace_value* catalognamespaceentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_namespace);
            while(NULL != (catalognamespaceentry = hash_seq_search(&status)))
            {
                if(NULL != catalognamespaceentry->ripple_namespace)
                {
                    rfree(catalognamespaceentry->ripple_namespace);
                }
            }
            hash_destroy(cserial->dicts->by_namespace);
        }

        /* range 表删除 */
        if(NULL != cserial->dicts->by_range)
        {
            ripple_catalog_range_value* catalograngeentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_range);
            while(NULL != (catalograngeentry = hash_seq_search(&status)))
            {
                if(NULL != catalograngeentry->ripple_range)
                {
                    rfree(catalograngeentry->ripple_range);
                }
            }
            hash_destroy(cserial->dicts->by_range);
        }

        /* enum 表删除 */
        if(NULL != cserial->dicts->by_enum)
        {
            ripple_catalog_enum_value* catalogenumentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_enum);
            while(NULL != (catalogenumentry = hash_seq_search(&status)))
            {
                if(NULL != catalogenumentry->enums)
                {
                    foreach(lc, catalogenumentry->enums)
                    {
                        rfree(lfirst(lc));
                    }
                    list_free(catalogenumentry->enums);
                }
            }

            hash_destroy(cserial->dicts->by_enum);
        }

        /* operator 表删除 */
        if(NULL != cserial->dicts->by_operator)
        {
            ripple_catalog_operator_value* catalogoperatorentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_operator);
            while(NULL != (catalogoperatorentry = hash_seq_search(&status)))
            {
                if(NULL != catalogoperatorentry->ripple_operator)
                {
                    rfree(catalogoperatorentry->ripple_operator);
                }
            }

            hash_destroy(cserial->dicts->by_operator);
        }

        /* by_authid */
        if(NULL != cserial->dicts->by_authid)
        {
            ripple_catalog_authid_value* catalogauthidentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_authid);
            while(NULL != (catalogauthidentry = hash_seq_search(&status)))
            {
                if(NULL != catalogauthidentry->ripple_authid)
                {
                    rfree(catalogauthidentry->ripple_authid);
                }
            }

            hash_destroy(cserial->dicts->by_authid);
        }

        if(NULL != cserial->dicts->by_constraint)
        {
            ripple_catalog_constraint_value *catalogconentry;
            hash_seq_init(&status,cserial->dicts->by_constraint);
            while (NULL != (catalogconentry = hash_seq_search(&status)))
            {
                if(NULL != catalogconentry->constraint)
                {
                    if (0 != catalogconentry->constraint->conkeycnt)
                    {
                        rfree(catalogconentry->constraint->conkey);
                    }
                    rfree(catalogconentry->constraint);
                }
            }

            hash_destroy(cserial->dicts->by_constraint);
        }

        /*by_database*/
        if(NULL != cserial->dicts->by_database)
        {
            ripple_catalog_database_value* catalogdatabaseentry = NULL;
            hash_seq_init(&status,cserial->dicts->by_database);
            while(NULL != (catalogdatabaseentry = hash_seq_search(&status)))
            {
                if(NULL != catalogdatabaseentry->ripple_database)
                {
                    rfree(catalogdatabaseentry->ripple_database);
                }
            }

            hash_destroy(cserial->dicts->by_database);
        }

        /* by_datname2oid */
        if(NULL != cserial->dicts->by_datname2oid)
        {
            hash_destroy(cserial->dicts->by_datname2oid);
            cserial->dicts->by_datname2oid = NULL;
        }

        /* by_index */
        if(NULL != cserial->dicts->by_index)
        {
            ripple_catalog_index_value* index = NULL;
            ripple_catalog_index_hash_entry* catalogindexentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_index);
            while(NULL != (catalogindexentry = hash_seq_search(&status)))
            {
                if(NULL != catalogindexentry->ripple_index_list)
                {
                    foreach(lc, catalogindexentry->ripple_index_list)
                    {
                        index = (ripple_catalog_index_value*)lfirst(lc);
                        if (index->ripple_index)
                        {
                            if (index->ripple_index->indkey)
                            {
                                rfree(index->ripple_index->indkey);
                            }
                            rfree(index->ripple_index);
                        }
                        rfree(index);
                    }
                    list_free(catalogindexentry->ripple_index_list);
                }
            }
            hash_destroy(cserial->dicts->by_index);
        }
        
        rfree(cserial->dicts);
        cserial->dicts = NULL;
    }

    /* 大事务 表删除 */
    if(NULL != cserial->by_txns)
    {
        ripple_bigtxn *txnentry = NULL;
        hash_seq_init(&status,cserial->by_txns);
        while(NULL != (txnentry = hash_seq_search(&status)))
        {
            ripple_bigtxn_clean(txnentry);
        }
        hash_destroy(cserial->by_txns);
    }

    cserial->privdata = NULL;

    rfree(cserial);
    cserial = NULL;
}
