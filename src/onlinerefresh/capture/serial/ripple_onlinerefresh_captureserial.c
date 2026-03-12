#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "stmts/ripple_txnstmt.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "misc/ripple_misc_control.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "serial/ripple_serial.h"
#include "onlinerefresh/capture/serial/ripple_onlinerefresh_captureserial.h"

ripple_onlinerefresh_captureserial *ripple_onlinerefresh_captureserial_init(void)
{
    ripple_onlinerefresh_captureserial *result = NULL;

    result = rmalloc0(sizeof(ripple_onlinerefresh_captureserial));
    if (!result)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init error, out of memory");
        return NULL;
    }
    result = rmemset0(result, 0, 0, sizeof(ripple_onlinerefresh_captureserial));
    result->serialstate = rmalloc0(sizeof(ripple_serialstate ));
    if (!result->serialstate)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result->serialstate, 0, 0, sizeof(ripple_serialstate ));
    ripple_serialstate_init(result->serialstate);

    result->dictcache = (ripple_transcache*)rmalloc0(sizeof(ripple_transcache));
    if(NULL == result->dictcache)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init error, out of memory");
        return NULL;
    }
    rmemset0(result->dictcache, 0, '\0', sizeof(ripple_transcache));

    return result;
}

/* 从文件加载系统字典 */
static bool ripple_onlinerefresh_captureserial_loadsysdictsfromfile(ripple_transcache* dictcache)
{
    if (NULL == dictcache)
    {
        return false;
    }

    ripple_cache_sysdictsload((void**)&dictcache->sysdicts);

    return true;

}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static bool ripple_onlinerefresh_captureserial_buffer2waitflush(ripple_onlinerefresh_captureserial *serial_task , ripple_txn* txn, bool put_end)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */
    int             oldflag = 0;
    int             bufid = 0;
    int timeout = 0;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* foldbuffer = NULL;
    ripple_serialstate *serialstate = NULL;

    serialstate = serial_task->serialstate;

    foldbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if(0 == foldbuffer->start)
    {
        return true;
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
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, bufid);
    if(NULL == fbuffer->privdata)
    {
        finfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
        if(NULL == finfo)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
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
    if(false == serial_task->callback.parserstat_curtlid_get(serial_task->privdata, &foldbuffer->extra.rewind.curtlid))
    {
        elog(RLOG_WARNING, "can not get timelineid");
        return false;
    }
    /* 将 oldbuffer 放入到待刷新缓存中 */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

    fbuffer->flag = oldflag;
    serialstate->ffsmgrstate->bufid = bufid;

    return true;
}

/* 将 entry 数据落盘 */
static bool ripple_onlinerefresh_captureserial_txn2disk(ripple_serialstate* serialstate, ripple_txn* txn)
{
    bool first = true;
    bool txnformetadata = true;                     /* 用于标识当前事务中只含有metadata */
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

ripple_trfwork_serial_txn2disk_reset:
        if(false == txnformetadata)
        {
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
                goto ripple_trfwork_serial_txn2disk_reset;
            }
        }
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_DATA, (void*)&txndata, serialstate->ffsmgrstate);

        if (rstmt->type == RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END)
        {
            find_end = true;
        }
    }
    return find_end;
}

/* onlinerefresh序列化获取buffers */
static ripple_file_buffers* ripple_onlinerefresh_captureserial_getfilebuffer(void* serial)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "onlinerefresh captureserial getfilebuffer exception, serial point is NULL");
    }

    serialstate = (ripple_onlinerefresh_captureserial*)serial;


    return serialstate->serialstate->txn2filebuffer;
}

/* 在系统字典获取dbname */
static char* ripple_onlinerefresh_captureserial_getdbname(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;

    return ripple_transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取dbid */
static Oid ripple_onlinerefresh_captureserial_getdboid(void* captureserial)
{
    return ripple_misc_controldata_database_get(captureserial);
}

/* 在系统字典获取namespace */
static void* ripple_onlinerefresh_captureserial_getnamespace(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;
    return ripple_transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取class */
static void* ripple_onlinerefresh_captureserial_getclass(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;
    return ripple_transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* 根据 oid 获取索引信息, 返回为链表 */
static void* ripple_onlinerefresh_captureserial_getindex(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    void *index = NULL;

    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;

    index = ripple_transcache_getindex(oid, (void*)serialstate->dictcache);

    return index;
}

/* 在系统字典获取attributes */
static void* ripple_onlinerefresh_captureserial_getattributes(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;
    return ripple_transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取type */
static void* ripple_onlinerefresh_captureserial_gettype(void* captureserial, Oid oid)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;
    return ripple_transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* 系统字典应用 */
static void ripple_onlinerefresh_captureserial_transcatalog2transcache(void* captureserial, void* catalog)
{
    ripple_onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (ripple_onlinerefresh_captureserial*)captureserial;
    ripple_cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* 设置ffsmgrstate回调函数 */
static void ripple_onlinerefresh_captureserial_setffsmgrcallback(ripple_onlinerefresh_captureserial* wstate)
{
    wstate->serialstate->ffsmgrstate->callback.getdboid = ripple_onlinerefresh_captureserial_getdboid;
    wstate->serialstate->ffsmgrstate->callback.getdbname = ripple_onlinerefresh_captureserial_getdbname;
    wstate->serialstate->ffsmgrstate->callback.getfilebuffer = ripple_onlinerefresh_captureserial_getfilebuffer;
    wstate->serialstate->ffsmgrstate->callback.getclass = ripple_onlinerefresh_captureserial_getclass;
    wstate->serialstate->ffsmgrstate->callback.getindex = ripple_onlinerefresh_captureserial_getindex;
    wstate->serialstate->ffsmgrstate->callback.getnamespace = ripple_onlinerefresh_captureserial_getnamespace;
    wstate->serialstate->ffsmgrstate->callback.getattributes = ripple_onlinerefresh_captureserial_getattributes;
    wstate->serialstate->ffsmgrstate->callback.gettype = ripple_onlinerefresh_captureserial_gettype;
    wstate->serialstate->ffsmgrstate->callback.catalog2transcache = ripple_onlinerefresh_captureserial_transcatalog2transcache;
    wstate->serialstate->ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    wstate->serialstate->ffsmgrstate->callback.setredosysdicts = NULL;
    wstate->serialstate->ffsmgrstate->callback.setdboid = NULL;
    wstate->serialstate->ffsmgrstate->callback.getrecords = NULL;
    wstate->serialstate->ffsmgrstate->callback.getparserstate = NULL;
    wstate->serialstate->ffsmgrstate->callback.freeattributes = NULL;
}

/* 设置 fdata内容和privdata修改之后不统一设置 */
static void ripple_onlinerefresh_captureserial_setffsmgr(ripple_onlinerefresh_captureserial* serial_task)
{
    serial_task->serialstate->ffsmgrstate->privdata = (void *)serial_task;
    serial_task->serialstate->ffsmgrstate->fdata->ffdata2 = serial_task->dictcache;
}

void *ripple_onlinerefresh_captureserial_main(void *args)
{
    /* 获取缓存中的事务时，附加出参 */
    bool online_end = false;
    int timeout                                         = 0;
    ripple_txn* entry                                   = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_serialstate* serial                     = NULL;
    ripple_onlinerefresh_captureserial* cserial     = NULL;

    thrnode = (ripple_thrnode *)args;
    cserial = (ripple_onlinerefresh_captureserial*)thrnode->data;
    serial = cserial->serialstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh capture serial stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 获取数据库信息 */
    serial->database = ripple_misc_controldata_database_get(NULL);

    /* 加载字典表 */
    ripple_onlinerefresh_captureserial_loadsysdictsfromfile(cserial->dictcache);

    /* onlinerefresh 设置为0 */
    ripple_serialstate_fbuffer_set(serial, 0, 0, 0);

    /* 设置ffsmgrstate回调函数 */
    ripple_onlinerefresh_captureserial_setffsmgrcallback(cserial);

    /* 序列化内容设置 */
    ripple_serialstate_ffsmgr_set(serial, RIPPLE_FFSMG_IF_TYPE_TRAIL);

    /* 设置 fdata内容和privdata */
    ripple_onlinerefresh_captureserial_setffsmgr(cserial);

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
        entry = ripple_cache_txn_get(cserial->parser2serialtxns, &timeout);
        if(NULL == entry)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 超时，查看是否需要将待写的 buffer 刷新到磁盘中 */
                if(false == ripple_onlinerefresh_captureserial_buffer2waitflush(cserial, NULL, false))
                {
                    elog(RLOG_WARNING, "add buffer 2 wait flush error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
                continue;
            }

            /* 执行过程中异常了, 退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 系统表应用 */
        /* 先加载，在应用 */
        /* 事务类型为提交 */

        /* 将 entry 数据落盘 */
        online_end = ripple_onlinerefresh_captureserial_txn2disk(serial, entry);

        /* txn 内存释放 */
        ripple_txn_free(entry);
        rfree(entry);
        entry = NULL;

        if (false == online_end)
        {
            continue;
        }
        
        if(false == ripple_onlinerefresh_captureserial_buffer2waitflush(cserial, NULL, true))
        {
            elog(RLOG_WARNING, "add buffer 2 wait flush error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        break;
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_captureserial_free(void *args)
{
    ripple_onlinerefresh_captureserial* serial = NULL;

    serial = (ripple_onlinerefresh_captureserial*)args;
    if (serial->serialstate)
    {
        ripple_serialstate_destroy(serial->serialstate);
    }

    if(NULL != serial->dictcache)
    {
        ripple_transcache_free(serial->dictcache);
        rfree(serial->dictcache);
        serial->dictcache = NULL;
    }
    rfree(serial->serialstate);
    rfree(serial);
}
