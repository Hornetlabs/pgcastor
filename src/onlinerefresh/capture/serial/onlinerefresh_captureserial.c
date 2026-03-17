#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "stmts/txnstmt.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "misc/misc_control.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "serial/serial.h"
#include "onlinerefresh/capture/serial/onlinerefresh_captureserial.h"

onlinerefresh_captureserial *onlinerefresh_captureserial_init(void)
{
    onlinerefresh_captureserial *result = NULL;

    result = rmalloc0(sizeof(onlinerefresh_captureserial));
    if (!result)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init error, out of memory");
        return NULL;
    }
    result = rmemset0(result, 0, 0, sizeof(onlinerefresh_captureserial));
    result->serialstate = rmalloc0(sizeof(serialstate ));
    if (!result->serialstate)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result->serialstate, 0, 0, sizeof(serialstate ));
    serialstate_init(result->serialstate);

    result->dictcache = (transcache*)rmalloc0(sizeof(transcache));
    if(NULL == result->dictcache)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init error, out of memory");
        return NULL;
    }
    rmemset0(result->dictcache, 0, '\0', sizeof(transcache));

    return result;
}

/* 从文件加载系统字典 */
static bool onlinerefresh_captureserial_loadsysdictsfromfile(transcache* dictcache)
{
    if (NULL == dictcache)
    {
        return false;
    }

    cache_sysdictsload((void**)&dictcache->sysdicts);

    return true;

}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static bool onlinerefresh_captureserial_buffer2waitflush(onlinerefresh_captureserial *serial_task , txn* txn, bool put_end)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */
    int             oldflag = 0;
    int             bufid = 0;
    int timeout = 0;
    ff_fileinfo* finfo = NULL;
    file_buffer* fbuffer = NULL;
    file_buffer* foldbuffer = NULL;
    serialstate *serialstate = NULL;

    serialstate = serial_task->serialstate;

    foldbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if(0 == foldbuffer->start)
    {
        return true;
    }
    oldflag = foldbuffer->flag;

    /* 获取新的 buffer 缓存 */
    while(1)
    {
        bufid = file_buffer_get(serialstate->txn2filebuffer, &timeout);
        if(INVALID_BUFFERID == bufid)
        {
            if(ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    fbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, bufid);
    if(NULL == fbuffer->privdata)
    {
        finfo = (ff_fileinfo*)rmalloc0(sizeof(ff_fileinfo));
        if(NULL == finfo)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        rmemset0(finfo, 0, '\0', sizeof(ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ff_fileinfo*)fbuffer->privdata;
    }

    rmemcpy0(fbuffer->data, 0, foldbuffer->data, foldbuffer->start);
    fbuffer->start = foldbuffer->start;

    if (put_end)
    {
        foldbuffer->flag |= FILE_BUFFER_FLAG_ONLINREFRESHEND;
    }

    /* 设置新 buffer 的其它信息 */
    rmemcpy0(finfo, 0, (ff_fileinfo*)foldbuffer->privdata, sizeof(ff_fileinfo));

    /* 设置 oldbuffer 的信息 */
    foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
    foldbuffer->extra.rewind.fileaddr.trail.offset = (((finfo->blknum - 1) * FILE_BUFFER_SIZE) + fbuffer->start);
    if(false == serial_task->callback.parserstat_curtlid_get(serial_task->privdata, &foldbuffer->extra.rewind.curtlid))
    {
        elog(RLOG_WARNING, "can not get timelineid");
        return false;
    }
    /* 将 oldbuffer 放入到待刷新缓存中 */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(file_buffer_extra));
    file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

    fbuffer->flag = oldflag;
    serialstate->ffsmgrstate->bufid = bufid;

    return true;
}

/* 将 entry 数据落盘 */
static bool onlinerefresh_captureserial_txn2disk(serialstate* serialstate, txn* txn)
{
    bool first = true;
    bool txnformetadata = true;                     /* 用于标识当前事务中只含有metadata */
    ListCell* lc = NULL;
    ff_txndata       txndata = { {0} };
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
        txnstmt* rstmt = (txnstmt*)lfirst(lc);
        rmemset1(&txndata, 0, '\0', sizeof(ff_txndata));
        txndata.data = rstmt;
        rstmt->database = serialstate->database;
        txndata.header.type = FF_DATA_TYPE_TXN;
        txndata.header.transid = txn->xid;

trfwork_serial_txn2disk_reset:
        if(false == txnformetadata)
        {
            if(1 == list_length(txn->stmts))
            {
                /* 即是开始也是结束 */
                txndata.header.transind = (FF_DATA_TRANSIND_START | FF_DATA_TRANSIND_IN );
            }
            else
            {
                if(true == first)
                {
                    first = false;
                    txndata.header.transind = FF_DATA_TRANSIND_START;
                }
                else
                {
                    txndata.header.transind = FF_DATA_TRANSIND_IN;
                }
            }
        }
        else
        {
            if(TXNSTMT_TYPE_METADATA == rstmt->type)
            {
                /* metadata 标识为开始,后面就不会产生 commit 了 */
                txndata.header.transind = FF_DATA_TRANSIND_START;
            }
            else
            {
                txnformetadata = false;
                goto trfwork_serial_txn2disk_reset;
            }
        }
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_DATA, (void*)&txndata, serialstate->ffsmgrstate);

        if (rstmt->type == TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END)
        {
            find_end = true;
        }
    }
    return find_end;
}

/* onlinerefresh序列化获取buffers */
static file_buffers* onlinerefresh_captureserial_getfilebuffer(void* serial)
{
    onlinerefresh_captureserial* serialstate = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "onlinerefresh captureserial getfilebuffer exception, serial point is NULL");
    }

    serialstate = (onlinerefresh_captureserial*)serial;


    return serialstate->serialstate->txn2filebuffer;
}

/* 在系统字典获取dbname */
static char* onlinerefresh_captureserial_getdbname(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;

    return transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取dbid */
static Oid onlinerefresh_captureserial_getdboid(void* captureserial)
{
    return misc_controldata_database_get(captureserial);
}

/* 在系统字典获取namespace */
static void* onlinerefresh_captureserial_getnamespace(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取class */
static void* onlinerefresh_captureserial_getclass(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* 根据 oid 获取索引信息, 返回为链表 */
static void* onlinerefresh_captureserial_getindex(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    void *index = NULL;

    serialstate = (onlinerefresh_captureserial*)captureserial;

    index = transcache_getindex(oid, (void*)serialstate->dictcache);

    return index;
}

/* 在系统字典获取attributes */
static void* onlinerefresh_captureserial_getattributes(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取type */
static void* onlinerefresh_captureserial_gettype(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* 系统字典应用 */
static void onlinerefresh_captureserial_transcatalog2transcache(void* captureserial, void* catalog)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* 设置ffsmgrstate回调函数 */
static void onlinerefresh_captureserial_setffsmgrcallback(onlinerefresh_captureserial* wstate)
{
    wstate->serialstate->ffsmgrstate->callback.getdboid = onlinerefresh_captureserial_getdboid;
    wstate->serialstate->ffsmgrstate->callback.getdbname = onlinerefresh_captureserial_getdbname;
    wstate->serialstate->ffsmgrstate->callback.getfilebuffer = onlinerefresh_captureserial_getfilebuffer;
    wstate->serialstate->ffsmgrstate->callback.getclass = onlinerefresh_captureserial_getclass;
    wstate->serialstate->ffsmgrstate->callback.getindex = onlinerefresh_captureserial_getindex;
    wstate->serialstate->ffsmgrstate->callback.getnamespace = onlinerefresh_captureserial_getnamespace;
    wstate->serialstate->ffsmgrstate->callback.getattributes = onlinerefresh_captureserial_getattributes;
    wstate->serialstate->ffsmgrstate->callback.gettype = onlinerefresh_captureserial_gettype;
    wstate->serialstate->ffsmgrstate->callback.catalog2transcache = onlinerefresh_captureserial_transcatalog2transcache;
    wstate->serialstate->ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    wstate->serialstate->ffsmgrstate->callback.setredosysdicts = NULL;
    wstate->serialstate->ffsmgrstate->callback.setdboid = NULL;
    wstate->serialstate->ffsmgrstate->callback.getrecords = NULL;
    wstate->serialstate->ffsmgrstate->callback.getparserstate = NULL;
    wstate->serialstate->ffsmgrstate->callback.freeattributes = NULL;
}

/* 设置 fdata内容和privdata修改之后不统一设置 */
static void onlinerefresh_captureserial_setffsmgr(onlinerefresh_captureserial* serial_task)
{
    serial_task->serialstate->ffsmgrstate->privdata = (void *)serial_task;
    serial_task->serialstate->ffsmgrstate->fdata->ffdata2 = serial_task->dictcache;
}

void *onlinerefresh_captureserial_main(void *args)
{
    /* 获取缓存中的事务时，附加出参 */
    bool online_end = false;
    int timeout                                         = 0;
    txn* entry                                   = NULL;
    thrnode* thr_node                             = NULL;
    serialstate* serial                     = NULL;
    onlinerefresh_captureserial* cserial     = NULL;

    thr_node = (thrnode *)args;
    cserial = (onlinerefresh_captureserial*)thr_node->data;
    serial = cserial->serialstate;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh capture serial stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    /* 获取数据库信息 */
    serial->database = misc_controldata_database_get(NULL);

    /* 加载字典表 */
    onlinerefresh_captureserial_loadsysdictsfromfile(cserial->dictcache);

    /* onlinerefresh 设置为0 */
    serialstate_fbuffer_set(serial, 0, 0, 0);

    /* 设置ffsmgrstate回调函数 */
    onlinerefresh_captureserial_setffsmgrcallback(cserial);

    /* 序列化内容设置 */
    serialstate_ffsmgr_set(serial, FFSMG_IF_TYPE_TRAIL);

    /* 设置 fdata内容和privdata */
    onlinerefresh_captureserial_setffsmgr(cserial);

    while(1)
    {
        entry = NULL;
        /* 首先判断是否接收到退出信号 */
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        entry = cache_txn_get(cserial->parser2serialtxns, &timeout);
        if(NULL == entry)
        {
            if(ERROR_TIMEOUT == timeout)
            {
                /* 超时，查看是否需要将待写的 buffer 刷新到磁盘中 */
                if(false == onlinerefresh_captureserial_buffer2waitflush(cserial, NULL, false))
                {
                    elog(RLOG_WARNING, "add buffer 2 wait flush error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                continue;
            }

            /* 执行过程中异常了, 退出 */
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* 系统表应用 */
        /* 先加载，在应用 */
        /* 事务类型为提交 */

        /* 将 entry 数据落盘 */
        online_end = onlinerefresh_captureserial_txn2disk(serial, entry);

        /* txn 内存释放 */
        txn_free(entry);
        rfree(entry);
        entry = NULL;

        if (false == online_end)
        {
            continue;
        }
        
        if(false == onlinerefresh_captureserial_buffer2waitflush(cserial, NULL, true))
        {
            elog(RLOG_WARNING, "add buffer 2 wait flush error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        thr_node->stat = THRNODE_STAT_EXIT;
        break;
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_captureserial_free(void *args)
{
    onlinerefresh_captureserial* serial = NULL;

    serial = (onlinerefresh_captureserial*)args;
    if (serial->serialstate)
    {
        serialstate_destroy(serial->serialstate);
    }

    if(NULL != serial->dictcache)
    {
        transcache_free(serial->dictcache);
        rfree(serial->dictcache);
        serial->dictcache = NULL;
    }
    rfree(serial->serialstate);
    rfree(serial);
}
