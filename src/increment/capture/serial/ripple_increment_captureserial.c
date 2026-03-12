#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "misc/ripple_misc_control.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "catalog/ripple_control.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "cache/ripple_fpwcache.h"
#include "catalog/ripple_catalog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "serial/ripple_serial.h"
#include "increment/capture/serial/ripple_increment_captureserial.h"

/* sysdicthis系统字典应用 */
static void ripple_increment_captureserial_sysdicthis2sysdict(ripple_increment_captureserialstate* cserial, List *his)
{
    if (NULL == his)
    {
        return;
    }

    /* 可以简单套一层, 内部基本只用了sysdict和relfilenode, 且relfilenode有空值判断 */
    ripple_cache_sysdicts_txnsysdicthis2cache(cserial->dictcache->sysdicts, his);
}

/* 设置时间戳 */
static void ripple_increment_captureserial_settimestamp(ripple_serialstate* serialstate, ripple_txn* txn)
{
    ripple_file_buffer*     fbuffer = NULL;

    if(NULL == txn || NULL == serialstate)
    {
        return;
    }

    fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);

    fbuffer->extra.timestamp = txn->endtimestamp;

    return;
}

/* 将 entry 数据落盘 */
static void ripple_increment_captureserial_txn2disk(ripple_serialstate* serialstate, ripple_txn* txn)
{
    bool first = true;
    bool txnformetadata = true;                     /* 用于标识当前事务中只含有metadata */
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

        if (RIPPLE_TXNSTMT_TYPE_SYSDICTHIS == rstmt->type)
        {
            ripple_increment_captureserial_sysdicthis2sysdict((ripple_increment_captureserialstate*)serialstate, txn->sysdictHis);
            continue;
        }

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
    }
    ripple_increment_captureserial_settimestamp(serialstate, txn);
}

/* 设置serial 解析节点信息 */
static void ripple_increment_captureserial_lsn_set(ripple_increment_captureserialstate* captureserialstate, XLogRecPtr redolsn, XLogRecPtr restartlsn, XLogRecPtr confirmlsn)
{
    captureserialstate->redolsn = redolsn;
    captureserialstate->restartlsn = restartlsn;
    captureserialstate->confirmlsn = confirmlsn;
}

/* 设置lsn和timeline */
static void ripple_increment_captureserial_fbuffer_lsnset(ripple_increment_captureserialstate* captureserialstate)
{
    ripple_file_buffer* fbuffer = NULL;

    fbuffer = ripple_file_buffer_getbybufid(captureserialstate->base.txn2filebuffer, 
                                            captureserialstate->base.ffsmgrstate->bufid);

    fbuffer->extra.chkpoint.redolsn.wal.lsn = captureserialstate->redolsn;
    fbuffer->extra.rewind.restartlsn.wal.lsn = captureserialstate->restartlsn;
    fbuffer->extra.rewind.confirmlsn.wal.lsn = captureserialstate->confirmlsn;
    fbuffer->extra.rewind.flushlsn.wal.lsn = captureserialstate->confirmlsn;
    fbuffer->extra.rewind.curtlid = captureserialstate->curtlid;
}

/* 设置timeline信息 */
static void ripple_increment_captureserial_timeline_set(ripple_increment_captureserialstate* captureserialstate, TimeLineID curtlid)
{
    captureserialstate->curtlid = curtlid;
}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static void ripple_increment_captureserial_buffer2waitflush(ripple_increment_captureserialstate* captureserialstate, ripple_txn* txn)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 wstate 中的 lsn 信息设置旧缓存的标识信息
     */
    int             oldflag = 0;
    int             bufid = 0;
    bool            flush = false;
    int timeout         = 0;

    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* foldbuffer = NULL;
    ripple_serialstate* serialstate = NULL;

    serialstate = (ripple_serialstate*)captureserialstate;

    foldbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if(0 == foldbuffer->start)
    {
        return;
    }

    oldflag = foldbuffer->flag;
    if (NULL != txn)
    {
        if (RIPPLE_TXN_TYPE_TIMELINE == txn->type)
        {
            captureserialstate->curtlid = txn->curtlid;
            foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REWIND;
        }

        if (txn->restart.wal.lsn > captureserialstate->restartlsn)
        {
            captureserialstate->restartlsn = txn->restart.wal.lsn;
            foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REWIND;
            foldbuffer->extra.rewind.restartlsn.wal.lsn = captureserialstate->restartlsn;
        }

        if (txn->confirm.wal.lsn > captureserialstate->confirmlsn)
        {
            captureserialstate->confirmlsn = txn->confirm.wal.lsn;
            foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REWIND;
            foldbuffer->extra.rewind.confirmlsn.wal.lsn = captureserialstate->confirmlsn;
            if(NULL != txn->stmts)
            {
                foldbuffer->extra.rewind.flushlsn.wal.lsn = captureserialstate->confirmlsn;
            }
        }

        if (txn->redo.wal.lsn > captureserialstate->redolsn)
        {
            captureserialstate->redolsn = txn->redo.wal.lsn;
            foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REDO;
            foldbuffer->extra.chkpoint.redolsn.wal.lsn = captureserialstate->redolsn;
            /* 两个 checkpoint 之间的系统表变更 */
            foldbuffer->extra.chkpoint.sysdicts = ripple_catalog_sysdict_filterbylsn(&captureserialstate->redosysdicts, captureserialstate->redolsn);
            flush = true;
        }

        if (captureserialstate->onlinerefreshdataset)
        {
            foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESH_DATASET;
            foldbuffer->extra.dataset.dataset = captureserialstate->onlinerefreshdataset;
            captureserialstate->onlinerefreshdataset = NULL;
            flush = true;
        }
    }
    else
    {
        /* 设置 oldbuffer 的信息 */
        foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REWIND;
        foldbuffer->extra.rewind.restartlsn.wal.lsn = captureserialstate->restartlsn;
        foldbuffer->extra.rewind.confirmlsn.wal.lsn = captureserialstate->confirmlsn;
        flush = true;
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

        /* 设置新 buffer 的其它信息 */
        rmemcpy0(finfo, 0, (ripple_ff_fileinfo*)foldbuffer->privdata, sizeof(ripple_ff_fileinfo));

        /* 设置 oldbuffer 的信息 */
        foldbuffer->extra.rewind.curtlid = captureserialstate->curtlid;

        /* 将 oldbuffer 放入到待刷新缓存中 */
        rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(ripple_file_buffer_extra));
        ripple_file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

        /* 重置 fbuffer 的内容 */
        fbuffer->flag = oldflag;
        fbuffer->extra.chkpoint.sysdicts = NULL;
        fbuffer->extra.dataset.dataset = NULL;
        serialstate->ffsmgrstate->bufid = bufid;
    }

    return;
}

/* 序列化故障恢复 */
static bool ripple_increment_captureserial_recovery(ripple_increment_captureserialstate* captureserialstate, uint64 fileoffset)
{
    bool shiftfile = false;
    int bufid = 0;
    int maxbufid = 0;
    int mbytes = 0;
    int minsize = 0;
    int timeout = 0;
    uint64 bytes = 0;
    uint64 freespc = 0;

    ripple_file_buffer* in_fbuffer = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_ff_fileinfo* nfinfo = NULL;
    ripple_serialstate* serialstate = NULL;
    ripple_ff_tail fftail = { 0 };

    if (NULL == captureserialstate)
    {
        return false;
    }

    serialstate = (ripple_serialstate*)captureserialstate;
    in_fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if(0 == fileoffset)
    {
        return false;
    }

    /* 计算 maxbufid */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

    /* 需要考虑块内空间不够和整好切文件 */
    finfo = (ripple_ff_fileinfo*)in_fbuffer->privdata;

    /* 获取 minsize */
    minsize = ripple_fftrail_data_tokenminsize(serialstate->ffsmgrstate->compatibility);
    if(maxbufid == finfo->blknum)
    {
        /* 追加后面的内容 */
        minsize += ripple_fftrail_taillen(serialstate->ffsmgrstate->compatibility);
        shiftfile = true;
    }

    /* 查看剩余空间 */
    freespc = (in_fbuffer->maxsize - in_fbuffer->start);

    elog(RLOG_INFO, "minsize:%u, freespc:%u, fileoffset:%lu", minsize, freespc, fileoffset);
    /* 比较剩余空间是否满足放入数据的最小要求 */
    if(minsize >= freespc)
    {
        if(false == shiftfile)
        {
            finfo->blknum++;
        }
        else
        {
            finfo->blknum = 1;
            finfo->fileid++;
        }
        
        in_fbuffer->start = 0;
        rmemset0(in_fbuffer->data, 0, '\0', in_fbuffer->maxsize);
    }

    fftail.nexttrailno = (finfo->fileid + 1);
    serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(RIPPLE_FFTRAIL_CXT_TYPE_RESET,
                                                    &fftail,
                                                    (void*)serialstate->ffsmgrstate);

    /* 重新获取 fbuffer */
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

    if(NULL != fbuffer->privdata)
    {
        nfinfo = (ripple_ff_fileinfo*)fbuffer->privdata;
    }
    else
    {
        nfinfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
        if(NULL == nfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(nfinfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        fbuffer->privdata = (void*)nfinfo;
    }
    /* 设置新 buffer 的其它信息 */
    rmemcpy0(nfinfo, 0, finfo, sizeof(ripple_ff_fileinfo));
    nfinfo->fileid = finfo->fileid;
    nfinfo->fileid++;
    nfinfo->blknum = 1;

    /* 初始化头部信息 */
    serialstate->ffsmgrstate->bufid = bufid;
    serialstate->ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL,
                                                  serialstate->ffsmgrstate);

    /*稳定后删除*/
    //serialstate->ffsmgrstate->fdata->extradata = (void*)captureserialstate->dictcache;

    /* 将 buffer 放入到待刷新缓存中 */
    rmemcpy1(&fbuffer->extra, 0, &in_fbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(serialstate->txn2filebuffer, in_fbuffer);

    /* 切换文件后将新生成的buffer加入缓存，用于write将新的文件点信息更新到base文件 */
    ripple_increment_captureserial_buffer2waitflush(captureserialstate, NULL);

    return true;
}

/* 设置ffsmgrstate privdata和fdata */
static void ripple_increment_captureserialstate_ffsmgr_set(ripple_increment_captureserialstate* serialstate)
{
    serialstate->base.ffsmgrstate->privdata = (void *)serialstate;
    serialstate->base.ffsmgrstate->fdata->ffdata2 = serialstate->dictcache;
}

/* 设置redo保存的系统字典 */
static void  ripple_increment_captureserial_setredosysdicts(void* serial, void* catalogdata)
{
    ripple_increment_captureserialstate* captureserialstate = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, privdata point is NULL");
    }

    if (NULL == catalogdata)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, catalogdata point is NULL");
    }

    captureserialstate = (ripple_increment_captureserialstate*)serial;
    captureserialstate->redosysdicts = lappend(captureserialstate->redosysdicts, catalogdata);

    return;
}

static void ripple_increment_captureserial_setonlinerefreshdataset(void* serial,  void* dataset)
{
    ripple_increment_captureserialstate* captureserialstate = NULL;
    List* dataset_list = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, privdata point is NULL");
    }

    if (NULL == dataset)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, catalogdata point is NULL");
    }

    dataset_list = (List*)dataset;
    captureserialstate = (ripple_increment_captureserialstate*)serial;

    /* 如果存在, 那么合并list */
    if (captureserialstate->onlinerefreshdataset)
    {
        ListCell *cell = NULL;
        foreach(cell, dataset_list)
        {
            void *node = lfirst(cell);

            captureserialstate->onlinerefreshdataset = lappend(captureserialstate->onlinerefreshdataset, node);
        }
        list_free(dataset_list);
    }
    else
    {
        captureserialstate->onlinerefreshdataset = dataset_list;
    }

    return;
}

/* 在系统字典获取dbname */
char* ripple_increment_captureserial_getdbname(void* captureserial, Oid oid)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    serialstate = (ripple_increment_captureserialstate*)captureserial;

    return ripple_transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取dboid */
Oid ripple_increment_captureserial_getdboid(void* captureserial)
{
    return ripple_misc_controldata_database_get(captureserial);
}

/* 在系统字典获取namespace */
void* ripple_increment_captureserial_getnamespace(void* captureserial, Oid oid)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    serialstate = (ripple_increment_captureserialstate*)captureserial;
    return ripple_transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取class */
void* ripple_increment_captureserial_getclass(void* captureserial, Oid oid)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    serialstate = (ripple_increment_captureserialstate*)captureserial;
    return ripple_transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* 根据 oid 获取索引信息, 返回为链表 */
static void* ripple_increment_captureserial_getindex(void* captureserial, Oid oid)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    void *index = NULL;

    serialstate = (ripple_increment_captureserialstate*)captureserial;

    index = ripple_transcache_getindex(oid, (void*)serialstate->dictcache);

    return index;
}

/* 在系统字典获取attribute */
void* ripple_increment_captureserial_getattributes(void* captureserial, Oid oid)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    serialstate = (ripple_increment_captureserialstate*)captureserial;
    return ripple_transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* 在系统字典获取type */
void* ripple_increment_captureserial_gettype(void* captureserial, Oid oid)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    serialstate = (ripple_increment_captureserialstate*)captureserial;
    return ripple_transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* 系统字典应用 */
void ripple_increment_captureserial_transcatalog2transcache(void* captureserial, void* catalog)
{
    ripple_increment_captureserialstate* serialstate = NULL;
    serialstate = (ripple_increment_captureserialstate*)captureserial;
    ripple_cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* 设置ffsmgr的callback函数 */
void ripple_increment_captureserial_ffsmgr_setcallback(ripple_increment_captureserialstate* wstate)
{
    wstate->base.ffsmgrstate->callback.getdboid = ripple_increment_captureserial_getdboid;
    wstate->base.ffsmgrstate->callback.getdbname = ripple_increment_captureserial_getdbname;
    wstate->base.ffsmgrstate->callback.getfilebuffer = ripple_serialstate_getfilebuffer;
    wstate->base.ffsmgrstate->callback.getclass = ripple_increment_captureserial_getclass;
    wstate->base.ffsmgrstate->callback.getindex = ripple_increment_captureserial_getindex;
    wstate->base.ffsmgrstate->callback.getnamespace = ripple_increment_captureserial_getnamespace;
    wstate->base.ffsmgrstate->callback.getattributes = ripple_increment_captureserial_getattributes;
    wstate->base.ffsmgrstate->callback.gettype = ripple_increment_captureserial_gettype;
    wstate->base.ffsmgrstate->callback.setredosysdicts = ripple_increment_captureserial_setredosysdicts;
    wstate->base.ffsmgrstate->callback.catalog2transcache = ripple_increment_captureserial_transcatalog2transcache;
    wstate->base.ffsmgrstate->callback.setonlinerefreshdataset = ripple_increment_captureserial_setonlinerefreshdataset;
    wstate->base.ffsmgrstate->callback.setdboid = NULL;
    wstate->base.ffsmgrstate->callback.getrecords = NULL;
    wstate->base.ffsmgrstate->callback.getparserstate = NULL;
    wstate->base.ffsmgrstate->callback.freeattributes = NULL;
}

/* 初始化capture_serialstate */
ripple_increment_captureserialstate* ripple_increment_captureserial_init(void)
{
    ripple_increment_captureserialstate* serialstate = NULL;

    serialstate = (ripple_increment_captureserialstate*)rmalloc0(sizeof(ripple_increment_captureserialstate));
    if(NULL == serialstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(serialstate, 0, '\0', sizeof(ripple_increment_captureserialstate));

    ripple_serialstate_init(&serialstate->base);

    serialstate->dictcache = (ripple_transcache*)rmalloc0(sizeof(ripple_transcache));
    if(NULL == serialstate->dictcache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(serialstate->dictcache, 0, '\0', sizeof(ripple_transcache));

    serialstate->redolsn = InvalidXLogRecPtr;
    serialstate->restartlsn = InvalidXLogRecPtr;
    serialstate->confirmlsn = InvalidXLogRecPtr;
    serialstate->redosysdicts = NULL;
    serialstate->state = RIPPLE_INCREMENT_CAPTURESERIAL_STATE_NOP;

    return serialstate;
}

static bool ripple_capture_serialstate_transcache_setfromfile(ripple_transcache* dictcache)
{
    if (NULL == dictcache)
    {
        return false;
    }

    ripple_cache_sysdictsload((void**)&dictcache->sysdicts);

    return true;

}

/*
 * 格式化主进程
*/
void* ripple_increment_captureserial_main(void *args)
{
    int iret                                    = 0;                           /* 获取缓存中的事务时，附加出参 */
    ripple_txn* entry                           = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_serialstate* serialstate             = NULL;
    ripple_increment_captureserialstate* wstate = NULL;
    ripple_capturebase dbase = { 0 };

    thrnode = (ripple_thrnode*)args;

    wstate = (ripple_increment_captureserialstate*)thrnode->data;
    serialstate = (ripple_serialstate*)wstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment capture serial stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 获取基础信息 */
    ripple_misc_stat_loaddecode(&dbase);

    /* database设置 */
    wstate->base.database = ripple_misc_controldata_database_get(NULL);

    /* 加载字典表 */
    ripple_capture_serialstate_transcache_setfromfile(wstate->dictcache);

    /* 设置lsn信息 */
    ripple_increment_captureserial_lsn_set(wstate, dbase.redolsn, dbase.restartlsn, dbase.confirmedlsn);

    ripple_increment_captureserial_timeline_set(wstate, dbase.curtlid);

    ripple_serialstate_fbuffer_set(serialstate, dbase.fileid, dbase.fileoffset, 0);

    ripple_increment_captureserial_fbuffer_lsnset(wstate);

    /* 设置ffsmgrstate回调函数 */
    ripple_increment_captureserial_ffsmgr_setcallback(wstate);

    /* 序列化内容设置 */
    ripple_serialstate_ffsmgr_set(serialstate, RIPPLE_FFSMG_IF_TYPE_TRAIL);

    /* 设置 fdata内容和privdata */
    ripple_increment_captureserialstate_ffsmgr_set(wstate);

    ripple_increment_captureserial_recovery(wstate, dbase.fileoffset);

    while(1)
    {
        entry = NULL;
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        entry = ripple_cache_txn_get(wstate->parser2serialtxns, &iret);
        if(NULL == entry)
        {
            if(RIPPLE_ERROR_TIMEOUT == iret)
            {
                /* 超时，查看是否需要将待写的 buffer 刷新到磁盘中 */
                ripple_increment_captureserial_buffer2waitflush(wstate, NULL);
                continue;
            }
            /* 需要退出，等待 worknode->status 变为 RIPPLE_WORK_STATUS_TERM 后退出*/
            if(RIPPLE_THRNODE_STAT_TERM != thrnode->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 系统表应用 */
        /* 先加载，在应用 */
        /* 事务类型为提交 */

        /* 将 entry 数据落盘 */
        ripple_increment_captureserial_txn2disk(serialstate, entry);

        /* 根据txn更新wstate->lsn信息 */
        ripple_increment_captureserial_buffer2waitflush(wstate, entry);

        /* txn 内存释放 */
        ripple_txn_free(entry);

        rfree(entry);

        entry = NULL;

    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void  ripple_increment_captureserial_destroy(ripple_increment_captureserialstate* captureserialstate)
{
    if(NULL == captureserialstate)
    {
        return;
    }

    ripple_serialstate_destroy((ripple_serialstate*)captureserialstate);

    if(NULL != captureserialstate->dictcache)
    {
        ripple_transcache_free(captureserialstate->dictcache);
        rfree(captureserialstate->dictcache);
        captureserialstate->dictcache = NULL;
    }

    ripple_cache_sysdicts_txnsysdicthisfree(captureserialstate->redosysdicts);
    list_free(captureserialstate->redosysdicts);
    captureserialstate->redosysdicts = NULL;

    captureserialstate->privdata = NULL;
    captureserialstate->parser2serialtxns = NULL;

    rfree(captureserialstate);
    captureserialstate = NULL;
}
