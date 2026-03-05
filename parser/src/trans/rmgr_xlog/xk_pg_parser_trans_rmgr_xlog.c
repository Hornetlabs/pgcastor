#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_xlog/xk_pg_parser_trans_rmgr_xlog.h"

#define XK_PG_PARSER_RMGR_XLOG_INFOCNT 4
#define RMGR_XLOG_MCXT NULL


typedef struct FullTransactionId
{
    uint64_t        value;
} FullTransactionId;

typedef struct CheckPoint
{
    xk_pg_parser_XLogRecPtr    redo;            /* next RecPtr available when we began to
                                 * create CheckPoint (i.e. REDO start point) */
    xk_pg_parser_TimeLineID    ThisTimeLineID; /* current TLI */
    xk_pg_parser_TimeLineID    PrevTimeLineID; /* previous TLI, if this record begins a new
                                 * timeline (equals ThisTimeLineID otherwise) */
    bool        fullPageWrites; /* current full_page_writes */
    FullTransactionId nextFullXid;    /* next free full transaction ID */
    uint32_t            nextOid;        /* next free OID */
    uint32_t nextMulti;        /* next free MultiXactId */
    uint32_t nextMultiOffset;    /* next free MultiXact offset */
    xk_pg_parser_TransactionId oldestXid;    /* cluster-wide minimum datfrozenxid */
    uint32_t            oldestXidDB;    /* database with minimum datfrozenxid */
    uint32_t oldestMulti;    /* cluster-wide minimum datminmxid */
    uint32_t            oldestMultiDB;    /* database with minimum datminmxid */
    int64_t    time;            /* time stamp of checkpoint */
    xk_pg_parser_TransactionId oldestCommitTsXid;    /* oldest Xid with valid commit
                                         * timestamp */
    xk_pg_parser_TransactionId newestCommitTsXid;    /* newest Xid with valid commit
                                         * timestamp */

    /*
     * Oldest XID still running. This is only needed to initialize hot standby
     * mode from an online checkpoint, so we only bother calculating this for
     * online checkpoints and only when wal_level is replica. Otherwise it's
     * set to InvalidTransactionId.
     */
    xk_pg_parser_TransactionId oldestActiveXid;
} CheckPoint;

typedef struct xl_end_of_recovery
{
    int64_t     end_time;
    uint32_t    ThisTimeLineID; /* new TLI */
    uint32_t    PrevTimeLineID; /* previous TLI we forked off from */
} xl_end_of_recovery;

typedef bool (*xk_pg_parser_trans_transrec_rmgr_info_func_pre)(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xlog_ckps(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xlog_ckpo(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xlog_switch(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xlog_recovery(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

typedef struct XK_PG_PARSER_TRANS_RMGR_XLOG
{
    xk_pg_parser_trans_rmgr_xlog_info                   m_infoid;       /* info值 */
    xk_pg_parser_trans_transrec_rmgr_info_func_pre          m_infofunc;     /* 预解析接口info级的处理函数 */
} xk_pg_parser_trans_rmgr_xlog;

static xk_pg_parser_trans_rmgr_xlog m_record_rmgr_xlog_info[] =
{
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_SHUTDOWN, xk_pg_parser_trans_rmgr_xlog_ckps},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_ONLINE, xk_pg_parser_trans_rmgr_xlog_ckpo},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_SWITCH, xk_pg_parser_trans_rmgr_xlog_switch},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_END_OF_RECOVERY, xk_pg_parser_trans_rmgr_xlog_recovery}
};

bool xk_pg_parser_trans_rmgr_xlog_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = XK_PG_PARSER_RMGR_XLOG_INFOCNT;
    int32_t index = 0;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_xlog_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_xlog_info[index].m_infofunc(state, result, xk_pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;

}

static bool xk_pg_parser_trans_rmgr_xlog_ckps(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_transchkp * transchp = NULL;
    CheckPoint* checkp = (CheckPoint*)state->main_data;

    /* 检查出入参的合法性 */
    if (!result || !state || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_CKPS_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: pre record is [xlog checkpoint_shutdown], invalid param\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(RMGR_XLOG_MCXT,
                                 (void **) (&transchp),
                                  sizeof(xk_pg_parser_translog_pre_transchkp)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_09;
        return false;
    }
    transchp->m_base.m_type = XK_PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN;
    transchp->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
    transchp->m_base.m_originid = state->record_origin;
    transchp->m_nextid = checkp->nextFullXid.value;
    transchp->m_redo_lsn = checkp->redo;
    transchp->m_this_timeline = checkp->ThisTimeLineID;
    transchp->m_prev_timeline =checkp->PrevTimeLineID;
    *result = (xk_pg_parser_translog_pre_base *) transchp;

    return true;
}

static bool xk_pg_parser_trans_rmgr_xlog_ckpo(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_transchkp * transchp = NULL;
    CheckPoint* checkp = (CheckPoint*)state->main_data;

    /* 检查出入参的合法性 */
    if (!result || !state || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_CKPO_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: pre record is [xlog checkpoint_online], invalid param\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(RMGR_XLOG_MCXT,
                                 (void **) (&transchp),
                                  sizeof(xk_pg_parser_translog_pre_transchkp)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_0A;
        return false;
    }
    transchp->m_base.m_type = XK_PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE;
    transchp->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
    transchp->m_base.m_originid = state->record_origin;
    transchp->m_nextid = checkp->nextFullXid.value;
    transchp->m_redo_lsn = checkp->redo;
    transchp->m_this_timeline = checkp->ThisTimeLineID;
    transchp->m_prev_timeline =checkp->PrevTimeLineID;
    *result = (xk_pg_parser_translog_pre_base *) transchp;

    return true;
}

static bool xk_pg_parser_trans_rmgr_xlog_switch(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_base * xlog_switch = NULL;

    /* 检查出入参的合法性 */
    if (!result || !state || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_SWITCH_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: pre record is [xlog switch], invalid param\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(RMGR_XLOG_MCXT,
                                 (void **) (&xlog_switch),
                                  sizeof(xk_pg_parser_translog_pre_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_08;
        return false;
    }
    if (24 != state->decoded_record->xl_tot_len)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_SWITCH_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: pre record is [xlog switch], record length is not 24\n");
        return false;
    }
    xlog_switch->m_type = XK_PG_PARSER_TRANSLOG_XLOG_SWITCH;
    xlog_switch->m_xid = xk_pg_parser_XLogRecGetXid(state);
    xlog_switch->m_originid = state->record_origin;
    *result = (xk_pg_parser_translog_pre_base *) xlog_switch;

    return true;
}

static bool xk_pg_parser_trans_rmgr_xlog_recovery(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_endrecovery *xlog_recovery = NULL;
    xl_end_of_recovery *recovery = (xl_end_of_recovery *)state->main_data;

    /* 检查出入参的合法性 */
    if (!result || !state || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XLOG_RECOVERY_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: pre record is [xlog recovery], invalid param\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(RMGR_XLOG_MCXT,
                                 (void **) (&xlog_recovery),
                                  sizeof(xk_pg_parser_translog_pre_endrecovery)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_XLOG_RECOVERY_MEMALLOC_ERROR1;
        return false;
    }

    xlog_recovery->m_base.m_type = XK_PG_PARSER_TRANSLOG_XLOG_RECOVERY;
    xlog_recovery->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
    xlog_recovery->m_base.m_originid = state->record_origin;
    xlog_recovery->m_this_timeline = recovery->ThisTimeLineID;
    xlog_recovery->m_prev_timeline = recovery->PrevTimeLineID;

    *result = (xk_pg_parser_translog_pre_base *) xlog_recovery;

    return true;
}
