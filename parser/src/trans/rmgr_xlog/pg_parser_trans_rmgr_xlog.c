#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_xlog/pg_parser_trans_rmgr_xlog.h"

#define PG_PARSER_RMGR_XLOG_INFOCNT 4
#define RMGR_XLOG_MCXT              NULL

typedef struct FullTransactionId
{
    uint64_t value;
} FullTransactionId;

typedef struct CheckPoint
{
    pg_parser_XLogRecPtr    redo;              /* next RecPtr available when we began to
                                                * create CheckPoint (i.e. REDO start point) */
    pg_parser_TimeLineID    ThisTimeLineID;    /* current TLI */
    pg_parser_TimeLineID    PrevTimeLineID;    /* previous TLI, if this record begins a new
                                                * timeline (equals ThisTimeLineID otherwise) */
    bool                    fullPageWrites;    /* current full_page_writes */
    FullTransactionId       nextFullXid;       /* next free full transaction ID */
    uint32_t                nextOid;           /* next free OID */
    uint32_t                nextMulti;         /* next free MultiXactId */
    uint32_t                nextMultiOffset;   /* next free MultiXact offset */
    pg_parser_TransactionId oldestXid;         /* cluster-wide minimum datfrozenxid */
    uint32_t                oldestXidDB;       /* database with minimum datfrozenxid */
    uint32_t                oldestMulti;       /* cluster-wide minimum datminmxid */
    uint32_t                oldestMultiDB;     /* database with minimum datminmxid */
    int64_t                 time;              /* time stamp of checkpoint */
    pg_parser_TransactionId oldestCommitTsXid; /* oldest Xid with valid commit
                                                * timestamp */
    pg_parser_TransactionId newestCommitTsXid; /* newest Xid with valid commit
                                                * timestamp */

    /*
     * Oldest XID still running. This is only needed to initialize hot standby
     * mode from an online checkpoint, so we only bother calculating this for
     * online checkpoints and only when wal_level is replica. Otherwise it's
     * set to InvalidTransactionId.
     */
    pg_parser_TransactionId oldestActiveXid;
} CheckPoint;

typedef struct xl_end_of_recovery
{
    int64_t  end_time;
    uint32_t ThisTimeLineID; /* new TLI */
    uint32_t PrevTimeLineID; /* previous TLI we forked off from */
} xl_end_of_recovery;

typedef bool (*pg_parser_trans_transrec_rmgr_info_func_pre)(pg_parser_XLogReaderState*    state,
                                                            pg_parser_translog_pre_base** result,
                                                            int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xlog_ckps(pg_parser_XLogReaderState*    state,
                                           pg_parser_translog_pre_base** result,
                                           int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xlog_ckpo(pg_parser_XLogReaderState*    state,
                                           pg_parser_translog_pre_base** result,
                                           int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xlog_switch(pg_parser_XLogReaderState*    state,
                                             pg_parser_translog_pre_base** result,
                                             int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xlog_recovery(pg_parser_XLogReaderState*    state,
                                               pg_parser_translog_pre_base** result,
                                               int32_t*                      pg_parser_errno);

typedef struct PG_PARSER_TRANS_RMGR_XLOG
{
    pg_parser_trans_rmgr_xlog_info              m_infoid;   /* info value */
    pg_parser_trans_transrec_rmgr_info_func_pre m_infofunc; /* info-level handler for pre-parse interface */
} pg_parser_trans_rmgr_xlog;

static pg_parser_trans_rmgr_xlog m_record_rmgr_xlog_info[] = {
    {PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_SHUTDOWN, pg_parser_trans_rmgr_xlog_ckps    },
    {PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_ONLINE,   pg_parser_trans_rmgr_xlog_ckpo    },
    {PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_SWITCH,              pg_parser_trans_rmgr_xlog_switch  },
    {PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_END_OF_RECOVERY,     pg_parser_trans_rmgr_xlog_recovery}
};

bool pg_parser_trans_rmgr_xlog_pre(pg_parser_XLogReaderState*    state,
                                   pg_parser_translog_pre_base** result,
                                   int32_t*                      pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t  infocnts = PG_PARSER_RMGR_XLOG_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_xlog_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_xlog_info[index].m_infofunc(state, result, pg_parser_errno);
        break;
    }
    /* Return false when not found */
    return false;
}

static bool pg_parser_trans_rmgr_xlog_ckps(pg_parser_XLogReaderState*    state,
                                           pg_parser_translog_pre_base** result,
                                           int32_t*                      pg_parser_errno)
{
    pg_parser_translog_pre_transchkp* transchp = NULL;
    CheckPoint*                       checkp = (CheckPoint*)state->main_data;

    /* Check validity of input/output parameters */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_CKPS_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: pre record is [xlog checkpoint_shutdown], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(RMGR_XLOG_MCXT, (void**)(&transchp), sizeof(pg_parser_translog_pre_transchkp)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_09;
        return false;
    }
    transchp->m_base.m_type = PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN;
    transchp->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    transchp->m_base.m_originid = state->record_origin;
    transchp->m_nextid = checkp->nextFullXid.value;
    transchp->m_redo_lsn = checkp->redo;
    transchp->m_this_timeline = checkp->ThisTimeLineID;
    transchp->m_prev_timeline = checkp->PrevTimeLineID;
    *result = (pg_parser_translog_pre_base*)transchp;

    return true;
}

static bool pg_parser_trans_rmgr_xlog_ckpo(pg_parser_XLogReaderState*    state,
                                           pg_parser_translog_pre_base** result,
                                           int32_t*                      pg_parser_errno)
{
    pg_parser_translog_pre_transchkp* transchp = NULL;
    CheckPoint*                       checkp = (CheckPoint*)state->main_data;

    /* Check validity of input/output parameters */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_CKPO_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: pre record is [xlog checkpoint_online], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(RMGR_XLOG_MCXT, (void**)(&transchp), sizeof(pg_parser_translog_pre_transchkp)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_0A;
        return false;
    }
    transchp->m_base.m_type = PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE;
    transchp->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    transchp->m_base.m_originid = state->record_origin;
    transchp->m_nextid = checkp->nextFullXid.value;
    transchp->m_redo_lsn = checkp->redo;
    transchp->m_this_timeline = checkp->ThisTimeLineID;
    transchp->m_prev_timeline = checkp->PrevTimeLineID;
    *result = (pg_parser_translog_pre_base*)transchp;

    return true;
}

static bool pg_parser_trans_rmgr_xlog_switch(pg_parser_XLogReaderState*    state,
                                             pg_parser_translog_pre_base** result,
                                             int32_t*                      pg_parser_errno)
{
    pg_parser_translog_pre_base* xlog_switch = NULL;

    /* Check validity of input/output parameters */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_SWITCH_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: pre record is [xlog switch], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(RMGR_XLOG_MCXT, (void**)(&xlog_switch), sizeof(pg_parser_translog_pre_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_08;
        return false;
    }
    if (24 != state->decoded_record->xl_tot_len)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_SWITCH_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: pre record is [xlog switch], record length is not 24\n");
        return false;
    }
    xlog_switch->m_type = PG_PARSER_TRANSLOG_XLOG_SWITCH;
    xlog_switch->m_xid = pg_parser_XLogRecGetXid(state);
    xlog_switch->m_originid = state->record_origin;
    *result = (pg_parser_translog_pre_base*)xlog_switch;

    return true;
}

static bool pg_parser_trans_rmgr_xlog_recovery(pg_parser_XLogReaderState*    state,
                                               pg_parser_translog_pre_base** result,
                                               int32_t*                      pg_parser_errno)
{
    pg_parser_translog_pre_endrecovery* xlog_recovery = NULL;
    xl_end_of_recovery*                 recovery = (xl_end_of_recovery*)state->main_data;

    /* Check validity of input/output parameters */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_RECOVERY_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: pre record is [xlog recovery], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(RMGR_XLOG_MCXT, (void**)(&xlog_recovery), sizeof(pg_parser_translog_pre_endrecovery)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_XLOG_RECOVERY_MEMALLOC_ERROR1;
        return false;
    }

    xlog_recovery->m_base.m_type = PG_PARSER_TRANSLOG_XLOG_RECOVERY;
    xlog_recovery->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    xlog_recovery->m_base.m_originid = state->record_origin;
    xlog_recovery->m_this_timeline = recovery->ThisTimeLineID;
    xlog_recovery->m_prev_timeline = recovery->PrevTimeLineID;

    *result = (pg_parser_translog_pre_base*)xlog_recovery;

    return true;
}
