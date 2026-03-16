#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "catalog/ripple_control.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_relmapfile.h"
#include "stmts/ripple_txnstmt.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_relmap.h"
#include "works/parserwork/wal/ripple_decode_ddl.h"

/* 解析 relmap 提交 */
void ripple_decode_relmap(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    /*
     * relmapfile 也是为元数据的一部分，所以应该放在 ripple_catalogdata 当中，那么在事务中的逻辑位置如下
     *  txn
     *      --> ripple_txnstmt
     *                  ---->ripple_catalog
     *                                  ---->ripple_relmapfile
    */
    ripple_txn* txn = NULL;
    ripple_txnstmt *stmt = NULL;
    ripple_replmapfile* relmapfile = NULL;
    ripple_catalogdata* catalogdata = NULL;
    ripple_txnstmt_metadata *metadata = NULL;
    xk_pg_parser_translog_pre_relmap* prerelmap = NULL;

    prerelmap = (xk_pg_parser_translog_pre_relmap*)pbase;

    if(ctx->decode_record->start.wal.lsn < ctx->base.restartlsn
        || ctx->database != prerelmap->m_dboid)
    {
        return;
    }

    if(0 == prerelmap->m_count)
    {
        return;
    }

    /* 获取 relmap 中的内容，并应用 */
    if(InvalidTransactionId != prerelmap->m_base.m_xid)
    {
        txn = ripple_transcache_getTXNByXidFind(ctx->transcache, prerelmap->m_base.m_xid);
        if(NULL == txn)
        {
            /* 初始化 */
            if(InvalidTransactionId == prerelmap->m_base.m_xid)
            {
                elog(RLOG_ERROR, "relmap's xid is invalid, amazing");
            }
            else
            {
                txn = ripple_transcache_getTXNByXid(ctx, prerelmap->m_base.m_xid);
            }
        }
    }

    if(NULL == txn)
    {
        elog(RLOG_ERROR, "relmap's xid is invalid, amazing");
    }

    ripple_dml2ddl(ctx, txn);
    ripple_transcache_sysdict2his(txn);
    ripple_transcache_sysdict_free(txn);
    RIPPLE_TXN_UNSET_TRANS_DDL(txn->flag);

    stmt = rmalloc0(sizeof(ripple_txnstmt));
    if(NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    stmt->type = RIPPLE_TXNSTMT_TYPE_METADATA;

    /* 指向性信息 */
    metadata = rmalloc0(sizeof(ripple_txnstmt_metadata));
    if(NULL == metadata)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(metadata, 0, 0, sizeof(ripple_txnstmt_metadata));
    stmt->stmt = (void*)metadata;

    /* 申请 ripple_catalogdata 放入到 sysdicthis 中 */
    catalogdata = rmalloc0(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    relmapfile = (ripple_replmapfile*)rmalloc0(sizeof(ripple_replmapfile));
    if(NULL == relmapfile)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(relmapfile, 0, '\0', sizeof(ripple_replmapfile));
    catalogdata->catalog = (ripple_replmapfile*)relmapfile;
    catalogdata->type = RIPPLE_CATALOG_TYPE_RELMAPFILE;
    relmapfile->num = prerelmap->m_count;
    relmapfile->mapping = prerelmap->m_mapping;
    prerelmap->m_count = 0;
    prerelmap->m_mapping = NULL;

    /* 设置 */
    txn->sysdictHis = lappend(txn->sysdictHis, catalogdata);

    metadata->begin = list_tail(txn->sysdictHis);
    metadata->end = metadata->begin;

    txn->stmts = lappend(txn->stmts, stmt);
    return;
}
