#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_ddl.h"
#include "stmts/txnstmt_dml.h"
#include "stmts/txnstmt_metadata.h"
#include "stmts/txnstmt_shiftfile.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "stmts/txnstmt_onlinerefresh_dataset.h"
#include "stmts/txnstmt_prepared.h"
#include "stmts/txnstmt_updatesynctable.h"
#include "stmts/txnstmt_reset.h"
#include "stmts/txnstmt_abandon.h"
#include "stmts/txnstmt_bigtxnend.h"
#include "stmts/txnstmt_bigtxnbegin.h"
#include "stmts/txnstmt_updaterewind.h"
#include "stmts/txnstmt_commit.h"
#include "stmts/txnstmt_burst.h"

static void txnstmt_simpleclean(void* data);
typedef void (*txnstmtfuncfree)(void* data);

typedef struct TXNSTMTOPS
{
    txnstmt_type    type;
    char*           desc;
    txnstmtfuncfree free;
} txnstmtops;

static txnstmtops m_txnstmtsops[] = {
    {TXNSTMT_TYPE_NOP, "NOP", NULL},
    {TXNSTMT_TYPE_DML, "DML STMT", txnstmt_dmlfree},
    {TXNSTMT_TYPE_DDL, "DDL STMT", txnstmt_ddlfree},
    {TXNSTMT_TYPE_METADATA, "METADATA STMT", txnstmt_metadatafree},
    {TXNSTMT_TYPE_SHIFTFILE, "SHIFTFILE STMT", txnstmt_shiftfilefree},
    {TXNSTMT_TYPE_REFRESH, "REFRESH STMT", txnstmt_refreshfree},
    {TXNSTMT_TYPE_ONLINEREFRESH_BEGIN, "ONLINEREFRESH BEGIN STMT",
     txnstmt_onlinerefresh_begin_free},
    {TXNSTMT_TYPE_ONLINEREFRESH_END, "ONLINEREFRESH END STMT", txnstmt_onlinerefresh_end_free},
    {TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END, "ONLINEREFRESH INCREMENT END STMT",
     txnstmt_onlinerefresh_increment_end_free},
    {TXNSTMT_TYPE_ONLINEREFRESH_DATASET, "ONLINEREFRESH DATASET STMT",
     txnstmt_onlinerefresh_dataset_free},
    {TXNSTMT_TYPE_UPDATESYNCTABLE, "UPDATE SYNCTABLE STMT", txnstmt_updatesynctable_free},
    {TXNSTMT_TYPE_PREPARED, "PREPARED STMT", txnstmt_prepared_free},
    {TXNSTMT_TYPE_BURST, "PREPARED STMT", txnstmt_burst_free},
    {TXNSTMT_TYPE_BIGTXN_BEGIN, "BIGTXN BEGIN STMT", txnstmt_bigtxnbegin_free},
    {TXNSTMT_TYPE_BIGTXN_END, "BIGTXN END STMT", txnstmt_bigtxnend_free},
    {TXNSTMT_TYPE_ABANDON, "ABANDON STMT", txnstmt_abandon_free},
    {TXNSTMT_TYPE_RESET, "RESET STMT", txnstmt_reset_free},
    {TXNSTMT_TYPE_UPDATEREWIND, "UPDATEREWIND STMT", txnstmt_updaterewind_free},
    {TXNSTMT_TYPE_SYSDICTHIS, "UPDATEREWIND STMT", txnstmt_simpleclean},
    {TXNSTMT_TYPE_ONLINEREFRESHABANDON, "ONLINEREFRESHABANDON STMT", txnstmt_simpleclean},
    {TXNSTMT_TYPE_COMMIT, "OCOMMIT STMT", txnstmt_commit_free},
    {TXNSTMT_TYPE_MAX, "MAX STMT", NULL}};

/* Simple cleanup */
static void txnstmt_simpleclean(void* data)
{
    if (data)
    {
        rfree(data);
    }
}

/* Initialization */
txnstmt* txnstmt_init(void)
{
    txnstmt* txn_stmt = NULL;

    txn_stmt = (txnstmt*)rmalloc1(sizeof(txnstmt));
    if (NULL == txn_stmt)
    {
        elog(RLOG_WARNING, "txnstmt init oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(txn_stmt, 0, '\0', sizeof(txnstmt));
    txn_stmt->database = INVALIDOID;
    txn_stmt->stmt = NULL;
    txn_stmt->type = TXNSTMT_TYPE_NOP;
    return txn_stmt;
}

void txnstmt_free(txnstmt* txn_stmt)
{
    /*
     * Release memory based on txnstmt
     */
    if (NULL == txn_stmt)
    {
        return;
    }

    if (txn_stmt->type >= TXNSTMT_TYPE_MAX)
    {
        elog(RLOG_WARNING, "unknown type:%d", txn_stmt->type);
    }

    if (NULL == m_txnstmtsops[txn_stmt->type].free)
    {
        elog(RLOG_ERROR, "txnstmt free unsupport %s free", m_txnstmtsops[txn_stmt->type].desc);
    }
    m_txnstmtsops[txn_stmt->type].free(txn_stmt->stmt);

    rfree(txn_stmt);
}
