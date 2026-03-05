#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "stmts/ripple_txnstmt.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "serial/ripple_serial.h"
#include "onlinerefresh/pump/serial/ripple_onlinerefresh_pumpserial.h"

static ripple_file_buffers* ripple_onlinerefresh_pumpserial_getfilebuffer(void* serial)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "onlinerefresh captureserial getfilebuffer exception, serial point is NULL");
    }

    serialstate = (ripple_onlinerefresh_pumpserial*)serial;


    return serialstate->serialstate->txn2filebuffer;
}

/* 在系统字典获取dbname */
static char* ripple_onlinerefresh_pumpserial_getdbname(void* pumpserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;

    return ripple_transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取dboid */
static Oid ripple_onlinerefresh_pumpserial_getdboid(void* pumpserial)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;

    return ripple_transcache_getdboid((void*)serialstate->dictcache);
}

/* 设置序列化结构的databaseoid */
static void ripple_onlinerefresh_pumpserial_setdboid(void* pumpserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;

    serialstate->serialstate->database = oid;
}

/* 在系统字典获取namespace */
static void* ripple_onlinerefresh_pumpserial_getnamespace(void* pumpserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;
    return ripple_transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取class */
static void* ripple_onlinerefresh_pumpserial_getclass(void* pumpserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;
    return ripple_transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* 根据 oid 获取索引信息, 返回为链表 */
static void* ripple_onlinerefresh_pumpserial_getindex(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    void *index = NULL;

    serialstate = (ripple_onlinerefresh_pumpserial*)captureserial;

    index = ripple_transcache_getindex(oid, (void*)serialstate->dictcache);

    return index;
}

/* 在系统字典获取attributes */
static void* ripple_onlinerefresh_pumpserial_getattributes(void* pumpserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;
    return ripple_transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取type */
static void* ripple_onlinerefresh_pumpserial_gettype(void* pumpserial, Oid oid)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;
    return ripple_transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* 系统字典应用 */
static void ripple_onlinerefresh_pumpserial_transcatalog2transcache(void* pumpserial, void* catalog)
{
    ripple_onlinerefresh_pumpserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_pumpserial*)pumpserial;
    ripple_cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* 设置ffsmgrstate的回调函数 */
static void ripple_onlinerefresh_pumpserial_ffsmgr_setcallback(ripple_onlinerefresh_pumpserial *serial_task)
{
    serial_task->serialstate->ffsmgrstate->callback.getdboid = ripple_onlinerefresh_pumpserial_getdboid;
    serial_task->serialstate->ffsmgrstate->callback.getdbname = ripple_onlinerefresh_pumpserial_getdbname;
    serial_task->serialstate->ffsmgrstate->callback.setdboid = ripple_onlinerefresh_pumpserial_setdboid;
    serial_task->serialstate->ffsmgrstate->callback.getfilebuffer = ripple_onlinerefresh_pumpserial_getfilebuffer;
    serial_task->serialstate->ffsmgrstate->callback.getclass = ripple_onlinerefresh_pumpserial_getclass;
    serial_task->serialstate->ffsmgrstate->callback.getindex = ripple_onlinerefresh_pumpserial_getindex;
    serial_task->serialstate->ffsmgrstate->callback.getnamespace = ripple_onlinerefresh_pumpserial_getnamespace;
    serial_task->serialstate->ffsmgrstate->callback.getattributes = ripple_onlinerefresh_pumpserial_getattributes;
    serial_task->serialstate->ffsmgrstate->callback.gettype = ripple_onlinerefresh_pumpserial_gettype;
    serial_task->serialstate->ffsmgrstate->callback.catalog2transcache = ripple_onlinerefresh_pumpserial_transcatalog2transcache;
    serial_task->serialstate->ffsmgrstate->callback.getrecords = NULL;
    serial_task->serialstate->ffsmgrstate->callback.getparserstate = NULL;
    serial_task->serialstate->ffsmgrstate->callback.setredosysdicts = NULL;
    serial_task->serialstate->ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    serial_task->serialstate->ffsmgrstate->callback.freeattributes = NULL;
}

/* 设置 fdata内容和privdata修改后不统一设置 */
static void ripple_onlinerefresh_pumpserial_ffsmgr_set(ripple_onlinerefresh_pumpserial* serial_task)
{
    serial_task->serialstate->ffsmgrstate->privdata = (void *)serial_task;
    serial_task->serialstate->ffsmgrstate->fdata->ffdata2 = serial_task->dictcache;
}

ripple_onlinerefresh_pumpserial *ripple_onlinerefresh_pumpserial_init(void)
{
    ripple_onlinerefresh_pumpserial *result = NULL;

    result = rmalloc0(sizeof(ripple_onlinerefresh_pumpserial));
    if (!result)
    {
        elog(RLOG_WARNING, "oom");
        return NULL;
    }
    result = rmemset0(result, 0, 0, sizeof(ripple_onlinerefresh_pumpserial));
    result->serialstate = rmalloc0(sizeof(ripple_serialstate ));
    if (!result->serialstate)
    {
        elog(RLOG_WARNING, "oom");
        return NULL;
    }
    rmemset0(result->serialstate, 0, 0, sizeof(ripple_serialstate ));
    ripple_serialstate_init(result->serialstate);

    result->dictcache = (ripple_transcache*)rmalloc0(sizeof(ripple_transcache));
    if(NULL == result->dictcache)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(result->dictcache, 0, '\0', sizeof(ripple_transcache));

    return result;
}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
 * 入参：
 * txn 要解析的事务
 * put_end 解析到end标识，设置buffer->flag
*/
static void ripple_onlinerefresh_pumpserial_buffer2waitflush(ripple_serialstate* serialstate, ripple_txn* txn, bool put_end)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */

    int             bufid = 0;
    bool            flush = true;
    int timeout = 0;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* foldbuffer = NULL;

    foldbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if(0 == foldbuffer->start)
    {
        return;
    }

    if (flush)
    {
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
                elog(RLOG_WARNING, "get file buffer error");
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

        if (put_end)
        {
            foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND;
        }

        /* 设置新 buffer 的其它信息 */
        rmemcpy0(finfo, 0, (ripple_ff_fileinfo*)foldbuffer->privdata, sizeof(ripple_ff_fileinfo));

        /* 设置 oldbuffer 的信息 */
        foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
        foldbuffer->extra.rewind.fileaddr.trail.offset = (((finfo->blknum - 1) * RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start);
        /* 将 oldbuffer 放入到待刷新缓存中 */
        rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(ripple_file_buffer_extra));
        ripple_file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

        serialstate->ffsmgrstate->bufid = bufid;
    }

    return;
}

/* 向后传递文件编号 */
static void ripple_onlinerefresh_pumpserial_setsegon(ripple_serialstate* serialstate, ripple_txn* txn, int transind, XLogRecPtr endlsn)
{
    ripple_file_buffer*  fbuffer = NULL;
    if (RIPPLE_FROZEN_TXNID == txn->xid || RIPPLE_REFRESH_TXNID == txn->xid)
    {
        elog(RLOG_DEBUG,"Invalid transaction not updated");
        return;
    }

    if(RIPPLE_TXNSTMT_TYPE_COMMIT == transind)
    {
        fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);

        if (txn->segno < fbuffer->extra.chkpoint.segno.trail.fileid)
        {
            elog(RLOG_WARNING,"Invalid segno fileid ");
        }
        else if(txn->segno != fbuffer->extra.chkpoint.segno.trail.fileid)
        {
            fbuffer->extra.chkpoint.segno.trail.fileid = txn->segno;
        }
    }

    return;
}

/* 将 entry 数据落盘 */
static bool ripple_onlinerefresh_pumpserial_txn2disk(ripple_serialstate* serialstate, ripple_txn* txn)
{
    bool first = true;
    ListCell* lc = NULL;
    ripple_ff_txndata       txndata = { {0} };
    bool find_end = false;

     /* 
      * 组装事务信息
     */
    if(NULL == txn->stmts)
    {
        return find_end;
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
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_DATA, (void*)&txndata, serialstate->ffsmgrstate);
        if (rstmt->type == RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END)
        {
            find_end = true;
        }
        
        ripple_onlinerefresh_pumpserial_setsegon(serialstate, txn, rstmt->type, rstmt->extra0.wal.lsn);
    }
    return find_end;
}

void* ripple_onlinerefresh_pumpserial_main(void* args)
{
    bool online_end                                 = false;
    int timeout                                     = 0;                           /* 获取缓存中的事务时，附加出参 */
    ripple_txn* entry                               = NULL;
    ripple_thrnode* thrnode                         = NULL;
    ripple_serialstate *serialstate                 = NULL;
    ripple_onlinerefresh_pumpserial * serial_task   = NULL;

    thrnode = (ripple_thrnode *) args;
    serial_task = (ripple_onlinerefresh_pumpserial*)thrnode->data;
    serialstate = serial_task->serialstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh pump incrment serial stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;
    serialstate->database = InvalidOid;

    /* 加载字典表 */
    serial_task->dictcache->sysdicts = ripple_cache_sysdicts_pump_init();
    if(NULL == serial_task->dictcache->sysdicts)
    {
        elog(RLOG_WARNING, "onlinerefresh pump incrment serial load sysdicts error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        return NULL;
    }

    /* onlinerefresh 设置为0 */
    ripple_serialstate_fbuffer_set(serialstate, PUMP_INFO_FILEID, PUMP_OFFSET_START, 0);

    /* 设置ffsmgrstate回调函数 */
    ripple_onlinerefresh_pumpserial_ffsmgr_setcallback(serial_task);

    /* 序列化内容设置 */
    ripple_serialstate_ffsmgr_set(serialstate, RIPPLE_FFSMG_IF_TYPE_TRAIL);

    /* 设置 fdata内容和privdata */
    ripple_onlinerefresh_pumpserial_ffsmgr_set(serial_task);

    while(1)
    {
        entry = NULL;
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        entry = ripple_cache_txn_get(serial_task->parser2serialtxns, &timeout);
        if(NULL == entry)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 超时，查看是否需要将待写的 buffer 刷新到磁盘中 */
                ripple_onlinerefresh_pumpserial_buffer2waitflush(serialstate, NULL, false);
                continue;
            }

            elog(RLOG_WARNING, "pump onlinerefresh get txn from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 将 entry 数据落盘 */
        online_end = ripple_onlinerefresh_pumpserial_txn2disk(serialstate, entry);

        /* txn 内存释放 */
        ripple_txn_free(entry);

        rfree(entry);

        entry = NULL;

        if(false == online_end)
        {
            continue;
        }

        /* onlinerefresh serial 结束了 */
        ripple_onlinerefresh_pumpserial_buffer2waitflush(serialstate, NULL, true);
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        break;
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_pumpserial_free(void* args)
{
    ripple_onlinerefresh_pumpserial* serial = NULL;

    serial = (ripple_onlinerefresh_pumpserial*)args;

    if (serial->serialstate)
    {
        ripple_serialstate_destroy(serial->serialstate);
        rfree(serial->serialstate);
    }

    if(NULL != serial->dictcache)
    {
        ripple_transcache_free(serial->dictcache);
        rfree(serial->dictcache);
        serial->dictcache = NULL;
    }

    serial->parser2serialtxns = NULL;

    rfree(serial);
}