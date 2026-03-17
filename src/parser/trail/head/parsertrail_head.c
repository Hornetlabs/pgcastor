#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "stmts/txnstmt.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/head/parsertrail_head.h"

/* 
 * Trail 文件头应用
 *  按照当前看，此头无用可不处理
  */
bool parsertrail_trailheadapply(parsertrail* parsertrail, void* data)
{
    txn* cur_txn = NULL;
    txnstmt* stmt = NULL;
    ff_header* ffheader = NULL;
    fftrail_privdata* privdata = NULL;
    txnstmt_shiftfile* shiftfile = NULL;
    ffheader = (ff_header*)data;
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;

    if(NULL == ffheader)
    {
        return true;
    }

    if(NULL == privdata)
    {
        return true;
    }

    /* 将redolsn、restartlsn、confirmlsn保存到privdata中在切页时使用 */
    privdata->redolsn = ffheader->redolsn;
    privdata->restartlsn = ffheader->restartlsn;
    privdata->confirmlsn = ffheader->confirmlsn;

    /* 创建shiftfile事务，保存redolsn、restartlsn、confirmlsn在序列化时使用*/
    cur_txn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);

    stmt = rmalloc0(sizeof(txnstmt));
    shiftfile = rmalloc0(sizeof(txnstmt_shiftfile));
    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    rmemset0(shiftfile, 0, 0, sizeof(txnstmt_shiftfile));

    shiftfile->redolsn = ffheader->redolsn;
    shiftfile->restartlsn = ffheader->restartlsn;
    shiftfile->confirmlsn = ffheader->confirmlsn;

    stmt->type = TXNSTMT_TYPE_SHIFTFILE;
    stmt->stmt = (void *)shiftfile;
    cur_txn->stmts = lappend(cur_txn->stmts, stmt);
    cur_txn->type = TXN_TYPE_SHIFTFILE;

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, cur_txn);
    if(NULL == parsertrail->dtxns)
    {
        elog(RLOG_WARNING, "parser trail head add txn to dlist error");
        return false;
    }

    /* 内存清理 */
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    /* 日志级别为 debug */
    if(RLOG_DEBUG == g_loglevel)
    {
        /* 输出调试日志 */
        elog(RLOG_DEBUG, "----------Trail File Header Begin------------");
        elog(RLOG_DEBUG, "version:          %s", ffheader->version);
        elog(RLOG_DEBUG, "compatibility:    %u", ffheader->compatibility);
        elog(RLOG_DEBUG, "dbtype:           %u", ffheader->dbtype);
        elog(RLOG_DEBUG, "dbversion:        %s", ffheader->dbversion);
        elog(RLOG_DEBUG, "filename:         %s", ffheader->filename);
        elog(RLOG_DEBUG, "filesize:         %lu", ffheader->filesize);
        elog(RLOG_DEBUG, "startxid:         %lu", ffheader->startxid);
        elog(RLOG_DEBUG, "endxid:           %lu", ffheader->endxid);
        elog(RLOG_DEBUG, "redolsn:          %u", ffheader->redolsn);
        elog(RLOG_DEBUG, "redolsn:          %08X/%08X",
                        (uint32)(ffheader->redolsn>>32), (uint32)(ffheader->redolsn&0xFFFFFFFF));
        elog(RLOG_DEBUG, "restartlsn:       %08X/%08X",
                        (uint32)(ffheader->restartlsn>>32), (uint32)(ffheader->restartlsn&0xFFFFFFFF));
        elog(RLOG_DEBUG, "confirmlsn:       %08X/%08X",
                        (uint32)(ffheader->confirmlsn>>32), (uint32)(ffheader->confirmlsn&0xFFFFFFFF));
        elog(RLOG_DEBUG, "----------Trail File Header   End------------");
    }
    return true;
}

void parsertrail_trailheadclean(parsertrail* parsertrail, void* data)
{
    ff_header* ffheader = (ff_header*)data;

    UNUSED(parsertrail);

    if(NULL == ffheader)
    {
        return;
    }

    /*
     * 内存释放
     */
    if(NULL != ffheader->version)
    {
        rfree(ffheader->version);
    }

    if(NULL != ffheader->dbversion)
    {
        rfree(ffheader->dbversion);
    }

    if(NULL != ffheader->filename)
    {
        rfree(ffheader->filename);
    }

    rfree(ffheader);
}
