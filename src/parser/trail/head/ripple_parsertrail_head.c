#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "stmts/ripple_txnstmt.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/head/ripple_parsertrail_head.h"

/* 
 * Trail 文件头应用
 *  按照当前看，此头无用可不处理
  */
bool ripple_parsertrail_trailheadapply(ripple_parsertrail* parsertrail, void* data)
{
    ripple_txn* cur_txn = NULL;
    ripple_txnstmt* stmt = NULL;
    ripple_ff_header* ffheader = NULL;
    ripple_fftrail_privdata* privdata = NULL;
    ripple_txnstmt_shiftfile* shiftfile = NULL;
    ffheader = (ripple_ff_header*)data;
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

    /* 创建shiftfile事务，保存redolsn、restartlsn、confirmlsn在pump序列化时使用*/
    cur_txn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);

    stmt = rmalloc0(sizeof(ripple_txnstmt));
    shiftfile = rmalloc0(sizeof(ripple_txnstmt_shiftfile));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    rmemset0(shiftfile, 0, 0, sizeof(ripple_txnstmt_shiftfile));

    shiftfile->redolsn = ffheader->redolsn;
    shiftfile->restartlsn = ffheader->restartlsn;
    shiftfile->confirmlsn = ffheader->confirmlsn;

    stmt->type = RIPPLE_TXNSTMT_TYPE_SHIFTFILE;
    stmt->stmt = (void *)shiftfile;
    cur_txn->stmts = lappend(cur_txn->stmts, stmt);
    cur_txn->type = RIPPLE_TXN_TYPE_SHIFTFILE;

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, cur_txn);
    if(NULL == parsertrail->dtxns)
    {
        elog(RLOG_WARNING, "parser trail head add txn to dlist error");
        return false;
    }

    /* 内存清理 */
    ripple_fftrail_invalidprivdata(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

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

void ripple_parsertrail_trailheadclean(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_header* ffheader = (ripple_ff_header*)data;

    RIPPLE_UNUSED(parsertrail);

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
