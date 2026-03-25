#include "app_incl.h"
#include "libpq-fe.h"
#include "misc/misc_stat.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/string/stringinfo.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "catalog/catalog.h"
#include "stmts/txnstmt.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_seq.h"
#include "common/pg_parser_common_utils.h"

/* Check if capture is needed based on dboid */
static bool heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    if (dboid && dboid != capture_dboid)
    {
        return false;
    }
    return true;
}

void decode_seq(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_pre_seq*  seq = NULL;
    txn*                         txn = NULL;
    Oid                          seq_oid = INVALIDOID;
    pg_sysdict_Form_pg_class     class = NULL;
    pg_sysdict_Form_pg_namespace nsp = NULL;
    StringInfo                   alter_seq_restart_stmt = NULL;
    txnstmt*                     stmt = NULL;
    txnstmt_ddl*                 dstmt = NULL;
    char*                        seq_name = NULL;
    char*                        seq_nspname = NULL;

    seq = (pg_parser_translog_pre_seq*)pbase;

    if (!heap_check_dboid(seq->m_dboid, ctx->database))
    {
        return;
    }

    // Get current transaction info
    txn = transcache_getTXNByXid((void*)ctx, pbase->m_xid);
    seq_oid = catalog_get_oid_by_relfilenode(ctx->trans_cache->sysdicts->by_relfilenode,
                                             txn->sysdictHis, txn->sysdict, seq->m_dboid,
                                             seq->m_tbspcoid, seq->m_relfilenode, true);

    /* Find system catalog record from cache */
    class = (pg_sysdict_Form_pg_class)catalog_get_class_sysdict(
        ctx->trans_cache->sysdicts->by_class, txn->sysdict, txn->sysdictHis, seq_oid);
    if (!class)
    {
        elog(RLOG_ERROR, "can't find sequence by oid: %u", seq_oid);
    }

    /* Find system catalog record from cache */
    nsp = (pg_parser_sysdict_pgnamespace*)catalog_get_namespace_sysdict(
        ctx->trans_cache->sysdicts->by_namespace, txn->sysdict, txn->sysdictHis,
        class->relnamespace);

    if (!nsp)
    {
        elog(RLOG_ERROR, "can't find namespace by oid: %u", class->relnamespace);
    }

    seq_name = class->relname.data;
    seq_nspname = nsp->nspname.data;

    alter_seq_restart_stmt = makeStringInfo();

    appendStringInfo(alter_seq_restart_stmt, "ALTER SEQUENCE \"%s\".\"%s\" RESTART WITH %ld;",
                     seq_nspname, seq_name, seq->m_last_value);

    stmt = rmalloc0(sizeof(txnstmt));
    if (!stmt)
    {
        elog(RLOG_ERROR, "oom");
    }

    dstmt = rmalloc0(sizeof(txnstmt_ddl));
    if (!dstmt)
    {
        elog(RLOG_ERROR, "oom");
    }

    rmemset0(dstmt, 0, 0, sizeof(txnstmt_ddl));
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_DDL;
    stmt->len = alter_seq_restart_stmt->len;

    txn->stmtsize += stmt->len;
    ctx->trans_cache->totalsize += stmt->len;
    dstmt->type = PG_PARSER_DDLTYPE_ALTER;
    dstmt->subtype = PG_PARSER_DDLTYPE_ALTER;
    dstmt->ddlstmt = rstrdup(alter_seq_restart_stmt->data);
    stmt->stmt = dstmt;

    deleteStringInfo(alter_seq_restart_stmt);
    elog(RLOG_DEBUG, "ddl trans result: %s", stmt->stmt);

    txn->stmts = lappend(txn->stmts, (void*)stmt);
}
