#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "serial/ripple_serial.h"
#include "increment/pump/serial/ripple_increment_pumpserial.h"

/* 设置时间戳 */
static void ripple_increment_pumpserial_settimestamp(ripple_increment_pumpserialstate* wstate, ripple_txn* txn)
{
    ripple_file_buffer*     fbuffer = NULL;

    if(NULL == txn || NULL == wstate)
    {
        return;
    }

    fbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, wstate->base.ffsmgrstate->bufid);

    fbuffer->extra.timestamp = txn->endtimestamp;

    return;
}

/* 向后传递文件编号 */
static void  ripple_increment_pumpserial_setsegon(ripple_increment_pumpserialstate* wstate, ripple_txn* txn, int transind, XLogRecPtr endlsn)
{
    ripple_file_buffer*     fbuffer = NULL;
    if (RIPPLE_FROZEN_TXNID == txn->xid || RIPPLE_REFRESH_TXNID == txn->xid)
    {
        elog(RLOG_DEBUG,"Invalid transaction not updated");
        return;
    }
    fbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, wstate->base.ffsmgrstate->bufid);

    if(RIPPLE_TXNSTMT_TYPE_COMMIT == transind)
    {
        if (txn->segno < fbuffer->extra.chkpoint.segno.trail.fileid)
        {
            elog(RLOG_WARNING,"Invalid segno fileid ");
        }
        else if(txn->segno != fbuffer->extra.chkpoint.segno.trail.fileid)
        {
            fbuffer->extra.chkpoint.segno.trail.fileid = txn->segno;
        }

        if (InvalidXLogRecPtr != endlsn)
        {
            fbuffer->extra.rewind.confirmlsn.wal.lsn = endlsn;
        }
    }

    return;
}

/* 事务数据序列化 */
static void  ripple_increment_pumpserial_txn2serial(ripple_increment_pumpserialstate* wstate, ripple_txn* txn)
{
    bool first = true;
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
    foreach(lc, txn->stmts)
    {
        ripple_txnstmt* rstmt = (ripple_txnstmt*)lfirst(lc);
        rmemset1(&txndata, 0, '\0', sizeof(ripple_ff_txndata));
        txndata.data = rstmt;
        rstmt->database = wstate->base.database;
        txndata.header.type = RIPPLE_FF_DATA_TYPE_TXN;
        txndata.header.transid = txn->xid;

        if(1 == list_length(txn->stmts))
        {
            /* 即是开始也是结束 */
            txndata.header.transind = (RIPPLE_FF_DATA_TRANSIND_START | RIPPLE_FF_DATA_TRANSIND_IN );
        }
        else
        {
            if(true == first)
            {
                first = false;
                txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_START;
            }
            else
            {
                txndata.header.transind = RIPPLE_FF_DATA_TRANSIND_IN;
            }
        }

        wstate->base.ffsmgrstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_DATA, (void*)&txndata, wstate->base.ffsmgrstate);

        ripple_increment_pumpserial_setsegon(wstate, txn, rstmt->type, rstmt->extra0.wal.lsn);
    }
    ripple_increment_pumpserial_settimestamp(wstate, txn);
}

/* 
 * 检查buffer中数据位置是否变化
*/
static bool ripple_increment_pumpserial_trailposcheck(ripple_increment_pumpserialstate* wstate, uint64* fileid, uint64* blknum, uint64* start)
{
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;

    fbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, wstate->base.ffsmgrstate->bufid);
    if(0 == fbuffer->start)
    {
        return false;
    }

    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    if (*fileid < finfo->fileid)
    {
        *fileid = finfo->fileid;
        *blknum = finfo->blknum;
        *start = fbuffer->start;
        return true;
    }
    else if (*fileid == finfo->fileid)
    {
        if (*blknum < finfo->blknum)
        {
            *blknum = finfo->blknum;
            *start = fbuffer->start;
            return true;
        }
        else if (*blknum == finfo->blknum)
        {
            if (*start < fbuffer->start)
            {
                *start = fbuffer->start;
                return true;
            }
        }
    }
    return false;
}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static void ripple_increment_pumpserial_buffer2waitflush(ripple_increment_pumpserialstate* wstate)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */

    int bufid = 0;
    int timeout = 0;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* foldbuffer = NULL;

    foldbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, wstate->base.ffsmgrstate->bufid);
    if(0 == foldbuffer->start)
    {
        return;
    }

    /* 获取新的 buffer 缓存 */
    while(1)
    {
        bufid = ripple_file_buffer_get(wstate->base.txn2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return;
        }
        break;
    }

    fbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, bufid);
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
    foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REWIND;
    foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
    foldbuffer->extra.rewind.fileaddr.trail.offset = (((finfo->blknum - 1) * RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start);

    /* 将 oldbuffer 放入到待刷新缓存中 */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(wstate->base.txn2filebuffer, foldbuffer);

    wstate->base.ffsmgrstate->bufid = bufid;
    return;
}

static void ripple_bigtxn_pumpserial_clean(ripple_increment_pumpserialstate* pumpserialstate)
{
    ripple_serialstate *serialstate = NULL;

    serialstate = (ripple_serialstate *)pumpserialstate;

    if (serialstate->ffsmgrstate)
    {
        uint64          bytes = 0;
        int             mbytes = 0;

        mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
        bytes = RIPPLE_MB2BYTE(mbytes);

        serialstate->ffsmgrstate->ffsmgr->ffsmgr_free(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL, serialstate->ffsmgrstate);
        rmemset0(serialstate->ffsmgrstate, 0, '\0', sizeof(ripple_ffsmgr_state));
        serialstate->database = InvalidOid;
        serialstate->ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
        serialstate->ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);
        serialstate->ffsmgrstate->privdata = serialstate;
        serialstate->ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

        ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, serialstate->ffsmgrstate);

        /* 调用初始化接口 */
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL,
                                                    serialstate->ffsmgrstate);

        ripple_increment_pumpserial_ffsmgr_setcallback(pumpserialstate);
        serialstate->ffsmgrstate->fdata->ffdata2 = pumpserialstate->dictcache;
    }

}

static void ripple_increment_pumpserial_set_status(ripple_increment_pumpserialstate* wstate, int state)
{
    if (!wstate)
    {
        elog(RLOG_ERROR, "Pump serial state exception, state point is NULL");
    }
    wstate->state = state;
}

static bool ripple_increment_pumpserial_check_work_state(ripple_increment_pumpserialstate *wstate,
                                                ripple_file_buffer** fbuffer,
                                                ripple_ff_fileinfo* finfo)
{
    int timeout = 0;
    if(wstate->state == RIPPLE_PUMP_STATUS_SERIAL_WORKING)
    {
        return true;
    }
    if(wstate->state == RIPPLE_PUMP_STATUS_SERIAL_INIT)
    {
        ripple_ff_fileinfo* finfo = NULL;
        int                 bufid = 0;

        /* 清理缓存 */
        ripple_bigtxn_pumpserial_clean(wstate);
        ripple_cache_txn_clean(wstate->parser2serialtxns);
        ripple_file_buffer_clean(wstate->base.txn2filebuffer);

        /* 重新获取 bufid */
        while(1)
        {
            bufid = ripple_file_buffer_get(wstate->base.txn2filebuffer, &timeout);
            if(RIPPLE_INVALID_BUFFERID == bufid)
            {
                if(RIPPLE_ERROR_TIMEOUT == timeout)
                {
                    usleep(10000);
                    continue;
                }
                elog(RLOG_WARNING, "get file buffer error");
                return false;
            }
            break;
        }
        *fbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, bufid);

        if(NULL == (*fbuffer)->privdata)
        {
            finfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
            if(NULL == finfo)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(finfo, 0, '\0', sizeof(ripple_ff_fileinfo));
            (*fbuffer)->privdata = (void*)finfo;
        }
        else
        {
            finfo = (ripple_ff_fileinfo*)(*fbuffer)->privdata;
        }
        wstate->base.ffsmgrstate->bufid = bufid;
        wstate->base.database = InvalidOid;

        finfo->fileid = wstate->callback.networkclientstate_cfileid_get(wstate->privdata);
        finfo->blknum = PUMP_BLKNUM_START;
        (*fbuffer)->start = PUMP_OFFSET_START;

        ripple_increment_pumpserial_set_status(wstate, RIPPLE_PUMP_STATUS_SERIAL_READY);
        wstate->callback.clientstat_state_set(wstate->privdata, RIPPLE_INCREMENT_PUMPNETSTATE_STATE_READY);
    }
    else if (wstate->state == RIPPLE_PUMP_STATUS_SERIAL_WORK)
    {
        wstate->callback.parserstat_state_set(wstate->privdata, RIPPLE_PUMP_STATUS_PARSER_WORK);
        ripple_increment_pumpserial_set_status(wstate, RIPPLE_PUMP_STATUS_SERIAL_WORKING);
    }
    return false;
}

/* 在系统字典获取dbname */
char* ripple_increment_pumpserial_getdbname(void* pumpserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;

    return ripple_transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取dboid */
Oid ripple_increment_pumpserial_getdboid(void* pumpserial)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;

    return ripple_transcache_getdboid((void*)serialstate->dictcache);
}

/* 设置ripple_serialstate的database */
void ripple_increment_pumpserial_setdboid(void* pumpserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;

    serialstate->base.database = oid;
}

/* 在系统字典获取namespace */
void* ripple_increment_pumpserial_getnamespace(void* pumpserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;
    return ripple_transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取class */
void* ripple_increment_pumpserial_getclass(void* pumpserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;
    return ripple_transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* 根据 oid 获取索引信息, 返回为链表 */
static void* ripple_increment_pumpserial_getindex(void* captureserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    void *index = NULL;

    serialstate = (ripple_increment_pumpserialstate*)captureserial;

    index = ripple_transcache_getindex(oid, (void*)serialstate->dictcache);


    return index;
}

/* 在系统字典获取attribute */
void* ripple_increment_pumpserial_getattributes(void* pumpserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;
    return ripple_transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取type */
void* ripple_increment_pumpserial_gettype(void* pumpserial, Oid oid)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;
    return ripple_transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* 应用系统字典 */
void ripple_increment_pumpserial_transcatalog2transcache(void* pumpserial, void* catalog)
{
    ripple_increment_pumpserialstate* serialstate = NULL;
    serialstate = (ripple_increment_pumpserialstate*)pumpserial;
    ripple_cache_sysdicts_clearsysdicthisbyclass(serialstate->dictcache->sysdicts, (ListCell*)catalog);
    ripple_cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* 设置ffsmgrstate的回调函数 */
void ripple_increment_pumpserial_ffsmgr_setcallback(ripple_increment_pumpserialstate* wstate)
{
    wstate->base.ffsmgrstate->callback.getdboid = ripple_increment_pumpserial_getdboid;
    wstate->base.ffsmgrstate->callback.getdbname = ripple_increment_pumpserial_getdbname;
    wstate->base.ffsmgrstate->callback.setdboid = ripple_increment_pumpserial_setdboid;
    wstate->base.ffsmgrstate->callback.getfilebuffer = ripple_serialstate_getfilebuffer;
    wstate->base.ffsmgrstate->callback.getclass = ripple_increment_pumpserial_getclass;
    wstate->base.ffsmgrstate->callback.getindex = ripple_increment_pumpserial_getindex;
    wstate->base.ffsmgrstate->callback.getnamespace = ripple_increment_pumpserial_getnamespace;
    wstate->base.ffsmgrstate->callback.getattributes = ripple_increment_pumpserial_getattributes;
    wstate->base.ffsmgrstate->callback.gettype = ripple_increment_pumpserial_gettype;
    wstate->base.ffsmgrstate->callback.catalog2transcache = ripple_increment_pumpserial_transcatalog2transcache;
    wstate->base.ffsmgrstate->callback.getrecords = NULL;
    wstate->base.ffsmgrstate->callback.getparserstate = NULL;
    wstate->base.ffsmgrstate->callback.setredosysdicts = NULL;
    wstate->base.ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    wstate->base.ffsmgrstate->callback.freeattributes = NULL;
}

/* 设置 fdata内容和privdata */
static void ripple_increment_pumpserialstate_ffsmgr_set(ripple_increment_pumpserialstate* serialstate)
{
    serialstate->base.ffsmgrstate->privdata = (void *)serialstate;
    serialstate->base.ffsmgrstate->fdata->ffdata2 = serialstate->dictcache;
}


ripple_increment_pumpserialstate* ripple_increment_pumpserialstate_init(void)
{
    ripple_increment_pumpserialstate* serialstate = NULL;

    serialstate = (ripple_increment_pumpserialstate*)rmalloc0(sizeof(ripple_increment_pumpserialstate));
    if(NULL == serialstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(serialstate, 0, '\0', sizeof(ripple_increment_pumpserialstate));

    ripple_serialstate_init(&serialstate->base);

    serialstate->dictcache = (ripple_transcache*)rmalloc0(sizeof(ripple_transcache));
    if(NULL == serialstate->dictcache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(serialstate->dictcache, 0, '\0', sizeof(ripple_transcache));

    return serialstate;
}

/*
 * 格式化主进程
*/
void* ripple_increment_pumpserial_main(void *args)
{
    int timeout                                 = 0;                           /* 获取缓存中的事务时，附加出参 */
    uint64 fileid                               = 0;
    uint64 blknum                               = 0;
    uint64 start                                = 0;
    ripple_txn* entry                           = NULL;
    ripple_ff_fileinfo* finfo                   = NULL;
    ripple_file_buffer* fbuffer                 = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_serialstate* serialstate             = NULL;
    ripple_increment_pumpserialstate* wstate    = NULL;

    thrnode = (ripple_thrnode*)args;
    wstate = (ripple_increment_pumpserialstate*)thrnode->data;
    serialstate = (ripple_serialstate*)wstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump increment serial stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    wstate->state = RIPPLE_PUMP_STATUS_SERIAL_RESET;

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    serialstate->database = InvalidOid;

    wstate->dictcache->sysdicts = ripple_cache_sysdicts_pump_init();

    ripple_serialstate_fbuffer_set(serialstate, PUMP_INFO_FILEID, PUMP_OFFSET_START, 0);

    /* 设置ffsmgrstate回调函数 */
    ripple_increment_pumpserial_ffsmgr_setcallback(wstate);

    /* 序列化内容设置 */
    ripple_serialstate_ffsmgr_set(serialstate, RIPPLE_FFSMG_IF_TYPE_TRAIL);

    /* 设置 fdata内容和privdata */
    ripple_increment_pumpserialstate_ffsmgr_set(wstate);

    while(1)
    {
        entry = NULL;
        if (RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        if (!ripple_increment_pumpserial_check_work_state(wstate, &fbuffer, finfo))
        {
            /* 睡眠 10 毫秒 */
            usleep(10000);
            continue;
        }

        /* 获取数据 */
        entry = ripple_cache_txn_get(wstate->parser2serialtxns, &timeout);
        if(NULL == entry)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 
                 * 超时，查看是否需要将待写的 buffer 刷新到磁盘中 
                 * 文件位置发生变化才刷盘
                 */
                if (true == ripple_increment_pumpserial_trailposcheck(wstate, &fileid, &blknum, &start))
                {
                    ripple_increment_pumpserial_buffer2waitflush(wstate);
                }
                continue;
            }

            elog(RLOG_WARNING, "pump increment serial get txn from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 先加载，在应用 */
        /* 系统表应用 */
        /* 事务类型为提交 */
        /* 将事务数据序列化 */
        ripple_increment_pumpserial_txn2serial(wstate, entry);

        /* txn 内存释放 */
        ripple_txn_free(entry);
        rfree(entry);
        entry = NULL;
    }

    return NULL;
}

/* 资源回收 */
void ripple_increment_pumpserial_destroy(ripple_increment_pumpserialstate* serialpumpstate)
{
    if(NULL == serialpumpstate)
    {
        return;
    }

    ripple_serialstate_destroy((ripple_serialstate*)serialpumpstate);

    if(NULL != serialpumpstate->dictcache)
    {
        ripple_transcache_free(serialpumpstate->dictcache);
        rfree(serialpumpstate->dictcache);
        serialpumpstate->dictcache = NULL;
    }

    serialpumpstate->privdata = NULL;
    serialpumpstate->parser2serialtxns = NULL;

    rfree(serialpumpstate);
    serialpumpstate = NULL;
}

