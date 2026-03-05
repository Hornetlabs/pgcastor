#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "misc/ripple_misc_stat.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/string/stringinfo.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_translog.h"
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
#include "stmts/ripple_txnstmt.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_seq.h"
#include "common/xk_pg_parser_common_utils.h"

/* 通过dboid判断是否需要捕获 */
static bool ripple_heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    if (dboid && dboid != capture_dboid)
        return false;
    return true;
}

void ripple_decode_seq(ripple_decodingcontext *ctx, xk_pg_parser_translog_pre_base *pbase)
{
    xk_pg_parser_translog_pre_seq *seq = NULL;
    ripple_txn *txn = NULL;
    Oid seq_oid = InvalidOid;
    xk_pg_sysdict_Form_pg_class class = NULL;
    xk_pg_sysdict_Form_pg_namespace nsp = NULL;
    StringInfo alter_seq_restart_stmt = NULL;
    ripple_txnstmt *stmt =  NULL;
    ripple_txnstmt_ddl *dstmt = NULL;
    char *seq_name = NULL;
    char *seq_nspname = NULL;

    seq = (xk_pg_parser_translog_pre_seq *) pbase;

    if (!ripple_heap_check_dboid(seq->m_dboid, ctx->database))
    {
        return;
    }

    //获取当前事务的信息
    txn = ripple_transcache_getTXNByXid((void*)ctx, pbase->m_xid);
    seq_oid = ripple_catalog_get_oid_by_relfilenode(ctx->transcache->sysdicts->by_relfilenode,
                                                    txn->sysdictHis,
                                                    txn->sysdict,
                                                    seq->m_dboid,
                                                    seq->m_tbspcoid,
                                                    seq->m_relfilenode,
                                                    true);

    /* 从缓存中查找系统表记录 */
    class = (xk_pg_sysdict_Form_pg_class) ripple_catalog_get_class_sysdict(ctx->transcache->sysdicts->by_class,
                                                                           txn->sysdict,
                                                                           txn->sysdictHis,
                                                                           seq_oid);
    if (!class)
    {
        elog(RLOG_ERROR, "can't find sequence by oid: %u", seq_oid);
    }

    /* 从缓存中查找系统表记录 */
    nsp = (xk_pg_parser_sysdict_pgnamespace *) ripple_catalog_get_namespace_sysdict(ctx->transcache->sysdicts->by_namespace,
                                                                           txn->sysdict,
                                                                           txn->sysdictHis,
                                                                           class->relnamespace);

    if (!nsp)
    {
        elog(RLOG_ERROR, "can't find namespace by oid: %u", class->relnamespace);
    }

    seq_name = class->relname.data;
    seq_nspname = nsp->nspname.data;

    alter_seq_restart_stmt = makeStringInfo();

    appendStringInfo(alter_seq_restart_stmt, "ALTER SEQUENCE \"%s\".\"%s\" RESTART WITH %ld;", seq_nspname,
                                                                                           seq_name,
                                                                                           seq->m_last_value);

    stmt = rmalloc0(sizeof(ripple_txnstmt));
    if (!stmt)
    {
        elog(RLOG_ERROR, "oom");
    }

    dstmt = rmalloc0(sizeof(ripple_txnstmt_ddl));
    if (!dstmt)
    {
        elog(RLOG_ERROR, "oom");
    }

    rmemset0(dstmt, 0, 0, sizeof(ripple_txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));

    stmt->type = RIPPLE_TXNSTMT_TYPE_DDL;
    stmt->len = alter_seq_restart_stmt->len;

    txn->stmtsize += stmt->len;
    ctx->transcache->totalsize += stmt->len;
    dstmt->type = XK_PG_PARSER_DDLTYPE_ALTER;
    dstmt->subtype = XK_PG_PARSER_DDLTYPE_ALTER;
    dstmt->ddlstmt = rstrdup(alter_seq_restart_stmt->data);
    stmt->stmt = dstmt;

    deleteStringInfo(alter_seq_restart_stmt);
    elog(RLOG_DEBUG, "ddl trans result: %s", stmt->stmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
}
