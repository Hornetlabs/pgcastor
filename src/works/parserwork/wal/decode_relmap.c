#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "catalog/control.h"
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
#include "catalog/relmapfile.h"
#include "stmts/txnstmt.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_ddl.h"

/* Parse relmap commit */
void decode_relmap(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    /*
     * relmapfile is also part of metadata, so it should be placed in catalogdata.
     * The logical position in the transaction is as follows:
     *  txn
     *      --> txnstmt
     *                  ---->catalog
     *                                  ---->relmapfile
     */
    txn*                           txn = NULL;
    txnstmt*                       stmt = NULL;
    replmapfile*                   relmapfile = NULL;
    catalogdata*                   catalogdata = NULL;
    txnstmt_metadata*              metadata = NULL;
    pg_parser_translog_pre_relmap* prerelmap = NULL;

    prerelmap = (pg_parser_translog_pre_relmap*)pbase;

    if (ctx->decode_record->start.wal.lsn < ctx->base.restartlsn ||
        ctx->database != prerelmap->m_dboid)
    {
        return;
    }

    if (0 == prerelmap->m_count)
    {
        return;
    }

    /* Get content from relmap and apply */
    if (InvalidTransactionId != prerelmap->m_base.m_xid)
    {
        txn = transcache_getTXNByXidFind(ctx->trans_cache, prerelmap->m_base.m_xid);
        if (NULL == txn)
        {
            /* Initialize */
            if (InvalidTransactionId == prerelmap->m_base.m_xid)
            {
                elog(RLOG_ERROR, "relmap's xid is invalid, amazing");
            }
            else
            {
                txn = transcache_getTXNByXid(ctx, prerelmap->m_base.m_xid);
            }
        }
    }

    if (NULL == txn)
    {
        elog(RLOG_ERROR, "relmap's xid is invalid, amazing");
    }

    dml2ddl(ctx, txn);
    transcache_sysdict2his(txn);
    transcache_sysdict_free(txn);
    TXN_UNSET_TRANS_DDL(txn->flag);

    stmt = rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    stmt->type = TXNSTMT_TYPE_METADATA;

    /* Pointing information */
    metadata = rmalloc0(sizeof(txnstmt_metadata));
    if (NULL == metadata)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(metadata, 0, 0, sizeof(txnstmt_metadata));
    stmt->stmt = (void*)metadata;

    /* Allocate catalogdata and put into sysdicthis */
    catalogdata = rmalloc0(sizeof(catalogdata));
    if (NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(catalogdata, 0, '\0', sizeof(catalogdata));

    relmapfile = (replmapfile*)rmalloc0(sizeof(replmapfile));
    if (NULL == relmapfile)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(relmapfile, 0, '\0', sizeof(replmapfile));
    catalogdata->catalog = (replmapfile*)relmapfile;
    catalogdata->type = CATALOG_TYPE_RELMAPFILE;
    relmapfile->num = prerelmap->m_count;
    relmapfile->mapping = prerelmap->m_mapping;
    prerelmap->m_count = 0;
    prerelmap->m_mapping = NULL;

    /* Set */
    txn->sysdictHis = lappend(txn->sysdictHis, catalogdata);

    metadata->begin = list_tail(txn->sysdictHis);
    metadata->end = metadata->begin;

    txn->stmts = lappend(txn->stmts, stmt);
    return;
}
