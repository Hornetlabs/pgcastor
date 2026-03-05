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
#include "bigtransaction/pump/serial/ripple_bigtxn_pumpserial.h"

/* 向后传递文件编号 */
static void  ripple_bigtxn_pumpserial_setsegon(ripple_increment_pumpserialstate* wstate, ripple_txn* txn, int transind, XLogRecPtr endlsn)
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
/* 向后传递文件编号 */
static void  ripple_bigtxn_pumpserial_setbigtxnend(ripple_increment_pumpserialstate* wstate)
{
    ripple_file_buffer*     fbuffer = NULL;

    fbuffer = ripple_file_buffer_getbybufid(wstate->base.txn2filebuffer, wstate->base.ffsmgrstate->bufid);
    
    fbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_BIGTXNEND;

    return;
}

/* 事务数据序列化 */
static void  ripple_bigtxn_pumpserial_txn2serial(ripple_increment_pumpserialstate* wstate, ripple_txn* txn)
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

//        elog(RLOG_DEBUG, "txn->xid:%lu", txn->xid);
        if(1 == list_length(txn->stmts))
        {
            if(!RIPPLE_TXN_ISBIGTXN(txn->flag)
                && RIPPLE_FROZEN_TXNID != txn->xid)
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
            if(true == first 
                && !RIPPLE_TXN_ISBIGTXN(txn->flag))
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

        ripple_bigtxn_pumpserial_setsegon(wstate, txn, rstmt->type, rstmt->extra0.wal.lsn);

    }

    if (true == txn->commit)
    {
        ripple_bigtxn_pumpserial_setbigtxnend(wstate);
    }
}


/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static void ripple_bigtxn_pumpserial_buffer2waitflush(ripple_increment_pumpserialstate* wstate)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */

    int bufid               = 0;
    int timeout             = 0;

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

#if 0
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

static void ripple_bigtxn_pumpserial_set_status(ripple_increment_pumpserialstate* wstate, int state)
{
    if (!wstate)
    {
        elog(RLOG_ERROR, "Pump serial state exception, state point is NULL");
    }
    wstate->state = state;
}
#endif

/* 设置 fdata内容和privdata */
static void ripple_bigtxn_pumpserialstate_ffsmgr_set(ripple_increment_pumpserialstate* serialstate)
{
    serialstate->base.ffsmgrstate->privdata = (void *)serialstate;
    serialstate->base.ffsmgrstate->fdata->ffdata2 = serialstate->dictcache;
}


ripple_bigtxn_pumpserial* ripple_bigtxn_pumpserial_init(void)
{
    ripple_bigtxn_pumpserial* serialstate = NULL;

    serialstate = (ripple_bigtxn_pumpserial*)rmalloc0(sizeof(ripple_bigtxn_pumpserial));
    if(NULL == serialstate)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(serialstate, 0, 0, sizeof(ripple_bigtxn_pumpserial));

    serialstate->serialstate = ripple_increment_pumpserialstate_init();

    return serialstate;
}

/*
 * 格式化主进程
*/
void *ripple_bigtxn_pumpserial_main(void *args)
{
    int timeout                                     = 0;
    ripple_txn* entry                               = NULL;
    ripple_thrnode* thrnode                         = NULL;
    ripple_serialstate* serialstate                 = NULL;
    ripple_bigtxn_pumpserial *pumpserial            = NULL;
    ripple_increment_pumpserialstate* wstate        = NULL;

    thrnode = (ripple_thrnode*)args;
    pumpserial = (ripple_bigtxn_pumpserial*)thrnode->data;
    wstate = (ripple_increment_pumpserialstate*)pumpserial->serialstate;
    serialstate = (ripple_serialstate*)wstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "bigtxn pump serial trail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

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
    ripple_bigtxn_pumpserialstate_ffsmgr_set(wstate);

    // wstate->state = RIPPLE_PUMP_STATUS_SERIAL_WORKING;

    while(1)
    {
        entry = NULL;
        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        entry = ripple_cache_txn_get(wstate->parser2serialtxns, &timeout);
        if(NULL == entry)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 超时，查看是否需要将待写的 buffer 刷新到磁盘中 */
                ripple_bigtxn_pumpserial_buffer2waitflush(wstate);
                continue;
            }

            elog(RLOG_WARNING, "bigtxn pump serial get txn from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 先加载，在应用 */
        /* 系统表应用 */
        /* 事务类型为提交 */
        /* 将事务数据序列化 */
        ripple_bigtxn_pumpserial_txn2serial(wstate, entry);

        /* txn 内存释放 */
        ripple_txn_free(entry);
        rfree(entry);
        entry = NULL;
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void ripple_bigtxn_pumpserial_free(void *args)
{
    ripple_bigtxn_pumpserial *pumpserial            = NULL;
    pumpserial = (ripple_bigtxn_pumpserial *)args;

    if (NULL == pumpserial)
    {
        return;
    }

    if (pumpserial->serialstate)
    {
        ripple_increment_pumpserial_destroy(pumpserial->serialstate);
    }

    rfree(pumpserial);
}

