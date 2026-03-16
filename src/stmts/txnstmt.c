#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_ddl.h"
#include "stmts/ripple_txnstmt_dml.h"
#include "stmts/ripple_txnstmt_metadata.h"
#include "stmts/ripple_txnstmt_shiftfile.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh_dataset.h"
#include "stmts/ripple_txnstmt_prepared.h"
#include "stmts/ripple_txnstmt_updatesynctable.h"
#include "stmts/ripple_txnstmt_reset.h"
#include "stmts/ripple_txnstmt_abandon.h"
#include "stmts/ripple_txnstmt_bigtxnend.h"
#include "stmts/ripple_txnstmt_bigtxnbegin.h"
#include "stmts/ripple_txnstmt_updaterewind.h"
#include "stmts/ripple_txnstmt_commit.h"
#include "stmts/ripple_txnstmt_burst.h"


static void ripple_txnstmt_simpleclean(void* data);
typedef void (*txnstmtfuncfree)(void* data);

typedef struct RIPPLE_TXNSTMTOPS
{
    ripple_txnstmt_type             type;
    char*                           desc;
    txnstmtfuncfree                 free;
} ripple_txnstmtops;

static ripple_txnstmtops m_txnstmtsops[] =
{
    {
        RIPPLE_TXNSTMT_TYPE_NOP,
        "NOP",
        NULL
    },
    {
        RIPPLE_TXNSTMT_TYPE_DML,
        "DML STMT",
        ripple_txnstmt_dmlfree
    },
    {
        RIPPLE_TXNSTMT_TYPE_DDL,
        "DDL STMT",
        ripple_txnstmt_ddlfree
    },
    {
        RIPPLE_TXNSTMT_TYPE_METADATA,
        "METADATA STMT",
        ripple_txnstmt_metadatafree
    },
    {
        RIPPLE_TXNSTMT_TYPE_SHIFTFILE,
        "SHIFTFILE STMT",
        ripple_txnstmt_shiftfilefree
    },
    {
        RIPPLE_TXNSTMT_TYPE_REFRESH,
        "REFRESH STMT",
        ripple_txnstmt_refreshfree
    },
    {
        RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_BEGIN,
        "ONLINEREFRESH BEGIN STMT",
        ripple_txnstmt_onlinerefresh_begin_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END,
        "ONLINEREFRESH END STMT",
        ripple_txnstmt_onlinerefresh_end_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END,
        "ONLINEREFRESH INCREMENT END STMT",
        ripple_txnstmt_onlinerefresh_increment_end_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_DATASET,
        "ONLINEREFRESH DATASET STMT",
        ripple_txnstmt_onlinerefresh_dataset_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_UPDATESYNCTABLE,
        "UPDATE SYNCTABLE STMT",
        ripple_txnstmt_updatesynctable_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_PREPARED,
        "PREPARED STMT",
        ripple_txnstmt_prepared_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_BURST,
        "PREPARED STMT",
        ripple_txnstmt_burst_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_BIGTXN_BEGIN,
        "BIGTXN BEGIN STMT",
        ripple_txnstmt_bigtxnbegin_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_BIGTXN_END,
        "BIGTXN END STMT",
        ripple_txnstmt_bigtxnend_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_ABANDON,
        "ABANDON STMT",
        ripple_txnstmt_abandon_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_RESET,
        "RESET STMT",
        ripple_txnstmt_reset_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_UPDATEREWIND,
        "UPDATEREWIND STMT",
        ripple_txnstmt_updaterewind_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_SYSDICTHIS,
        "UPDATEREWIND STMT",
        ripple_txnstmt_simpleclean
    },
    {
        RIPPLE_TXNSTMT_TYPE_ONLINEREFRESHABANDON,
        "ONLINEREFRESHABANDON STMT",
        ripple_txnstmt_simpleclean
    },
    {
        RIPPLE_TXNSTMT_TYPE_COMMIT,
        "OCOMMIT STMT",
        ripple_txnstmt_commit_free
    },
    {
        RIPPLE_TXNSTMT_TYPE_MAX,
        "MAX STMT",
        NULL
    }
};

/* 简单清理 */
static void ripple_txnstmt_simpleclean(void* data)
{
    if (data)
    {
        rfree(data);
    }
}

/* 初始化 */
ripple_txnstmt* ripple_txnstmt_init(void)
{
    ripple_txnstmt* txnstmt = NULL;

    txnstmt = (ripple_txnstmt*)rmalloc1(sizeof(ripple_txnstmt));
    if(NULL == txnstmt)
    {
        elog(RLOG_WARNING,"txnstmt init oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(txnstmt, 0, '\0', sizeof(ripple_txnstmt));
    txnstmt->database = InvalidOid;
    txnstmt->stmt = NULL;
    txnstmt->type = RIPPLE_TXNSTMT_TYPE_NOP;
    return txnstmt;
}

void ripple_txnstmt_free(ripple_txnstmt* txnstmt)
{
    /*
     * 根据 txnstmt 释放内存
     */
    if(NULL == txnstmt)
    {
        return;
    }

    if(txnstmt->type >= RIPPLE_TXNSTMT_TYPE_MAX)
    {
        elog(RLOG_WARNING, "unknown type:%d", txnstmt->type);
    }

    if(NULL == m_txnstmtsops[txnstmt->type].free)
    {
        elog(RLOG_ERROR, "txnstmt free unsupport %s free", m_txnstmtsops[txnstmt->type].desc);
    }
    m_txnstmtsops[txnstmt->type].free(txnstmt->stmt);

    rfree(txnstmt);
}
