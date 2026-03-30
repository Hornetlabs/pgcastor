#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/reset/parsertrail_reset.h"

/*
 * Trail RESET application
 *  Clean data
 */
bool parsertrail_trailresetapply(parsertrail* parsertrail, void* data)
{
    /* Big transaction related, not needed for now */
    txnstmt*  rstmt = NULL;
    txn*      resettxn = NULL;
    ff_reset* resetdata = NULL;
    txn*      cur_txn = NULL;

    if (NULL == data)
    {
        return true;
    }

    /* Data cleanup */
    cur_txn = parsertrail->lasttxn;
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    /* Cleanup all incomplete transactions */
    if (cur_txn)
    {
        if (InvalidFullTransactionId != cur_txn->xid)
        {
            transcache_removeTXNByXid(parsertrail->transcache, cur_txn->xid);
            parsertrail->lasttxn = NULL;
        }
    }
    resetdata = (ff_reset*)data;

    /* Generate reset transaction and add to cache */
    resettxn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == resettxn)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }

    rstmt = txnstmt_init();
    if (NULL == rstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rstmt->extra0.wal.lsn = MAX_LSN;
    resettxn->confirm.wal.lsn = MAX_LSN;
    resettxn->stmts = lappend(resettxn->stmts, rstmt);
    rstmt->type = TXNSTMT_TYPE_RESET;
    resettxn->type = TXN_TYPE_RESET;
    resettxn->segno = resetdata->nexttrailno;

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, resettxn);
    return true;
}

void parsertrail_trailresetclean(parsertrail* parsertrail, void* data)
{
    UNUSED(parsertrail);

    /* Free data */
    if (data)
    {
        rfree(data);
    }
}
