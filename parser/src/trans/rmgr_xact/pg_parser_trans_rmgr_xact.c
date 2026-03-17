#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_xact/xk_pg_parser_trans_rmgr_xact.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_fmgr.h"

#define XK_PG_PARSER_RMGR_XACT_INFOCNT 6
#define RMGR_XACT_MCXT NULL

#define XK_PG_PARSER_RMGR_XACT_STATUS_ABORT (uint8_t) 0x01
#define XK_PG_PARSER_RMGR_XACT_STATUS_COMMIT (uint8_t) 0x02

#define MinSizeOfXactSubxacts offsetof(xk_pg_parser_xl_xact_subxacts, subxacts)

typedef bool (*xk_pg_parser_trans_transrec_rmgr_info_func_pre)(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);
//todo 两阶段提交
static bool xk_pg_parser_trans_rmgr_xact_commit(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xact_abort(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xact_commit_prepare(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xact_abort_prepare(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xact_assignment(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_xact_prepare(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

typedef struct XK_PG_PARSER_TRANS_RMGR_XLOG
{
    xk_pg_parser_trans_rmgr_xact_info                   m_infoid;       /* info值 */
    xk_pg_parser_trans_transrec_rmgr_info_func_pre     m_infofunc;     /* 预解析接口info级的处理函数 */
} xk_pg_parser_trans_rmgr_xact;

static xk_pg_parser_trans_rmgr_xact m_record_rmgr_xlog_info[] =
{
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT, xk_pg_parser_trans_rmgr_xact_commit},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT, xk_pg_parser_trans_rmgr_xact_abort},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT_PREPARED, xk_pg_parser_trans_rmgr_xact_commit_prepare},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT_PREPARED, xk_pg_parser_trans_rmgr_xact_abort_prepare},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ASSIGNMENT, xk_pg_parser_trans_rmgr_xact_assignment},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_PREPARE, xk_pg_parser_trans_rmgr_xact_prepare},

};

bool xk_pg_parser_trans_rmgr_xact_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = XK_PG_PARSER_RMGR_XACT_INFOCNT;
    int32_t index = 0;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_OPMASK;
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

static size_t strlcpy(char *dst, const char *src, size_t siz)
{
    char       *d = dst;
    const char *s = src;
    size_t      n = siz;

    /* Copy as many bytes as will fit */
    if (n != 0)
    {
        while (--n != 0)
        {
            if ((*d++ = *s++) == '\0')
                break;
        }
    }

    /* Not enough room in dst, add NUL and traverse rest of src */
    if (n == 0)
    {
        if (siz != 0)
            *d = '\0';            /* NUL-terminate dst */
        while (*s++)
            ;
    }

    return (s - src - 1);        /* count does not include NUL */
}

static void ParseCommitRecord(uint8_t info, xk_pg_parser_xl_xact_commit *xlrec, xk_pg_parser_xl_xact_parsed_commit *parsed)
{
    char       *data = ((char *) xlrec) + xk_pg_parser_MinSizeOfXactCommit;

    rmemset0(parsed, 0, 0, sizeof(*parsed));

    parsed->xinfo = 0;            /* default, if no XLOG_XACT_HAS_INFO is
                                 * present */

    parsed->xact_time = xlrec->xact_time;

    if (info & XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_HAS_INFO)
    {
        xk_pg_parser_xl_xact_xinfo *xl_xinfo = (xk_pg_parser_xl_xact_xinfo *) data;

        parsed->xinfo = xl_xinfo->xinfo;

        data += sizeof(xk_pg_parser_xl_xact_xinfo);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_DBINFO)
    {
        xk_pg_parser_xl_xact_dbinfo *xl_dbinfo = (xk_pg_parser_xl_xact_dbinfo *) data;

        parsed->dbId = xl_dbinfo->dbId;
        parsed->tsId = xl_dbinfo->tsId;

        data += sizeof(xk_pg_parser_xl_xact_dbinfo);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_SUBXACTS)
    {
        xk_pg_parser_xl_xact_subxacts *xl_subxacts = (xk_pg_parser_xl_xact_subxacts *) data;

        parsed->nsubxacts = xl_subxacts->nsubxacts;

        xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **) &parsed->subxacts, parsed->nsubxacts * sizeof(uint32_t));
        rmemcpy0(parsed->subxacts, 0, xl_subxacts->subxacts, parsed->nsubxacts * sizeof(uint32_t));

        data += MinSizeOfXactSubxacts;
        data += parsed->nsubxacts * sizeof(uint32_t);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_RELFILENODES)
    {
        xk_pg_parser_xl_xact_relfilenodes *xl_relfilenodes = (xk_pg_parser_xl_xact_relfilenodes *) data;

        parsed->nrels = xl_relfilenodes->nrels;
        xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **) &parsed->xnodes, xl_relfilenodes->nrels * sizeof(xk_pg_parser_RelFileNode));
        rmemcpy0(parsed->xnodes, 0, xl_relfilenodes->xnodes, xl_relfilenodes->nrels * sizeof(xk_pg_parser_RelFileNode));

        data += xk_pg_parser_MinSizeOfXactRelfilenodes;
        data += xl_relfilenodes->nrels * sizeof(xk_pg_parser_RelFileNode);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_INVALS)
    {
        xk_pg_parser_xl_xact_invals *xl_invals = (xk_pg_parser_xl_xact_invals *) data;

        parsed->nmsgs = xl_invals->nmsgs;
        parsed->msgs = xl_invals->msgs;

        xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **) &parsed->msgs, xl_invals->nmsgs * sizeof(xk_pg_parser_SharedInvalidationMessage));
        rmemcpy0(parsed->msgs, 0, xl_invals->msgs, xl_invals->nmsgs * sizeof(xk_pg_parser_SharedInvalidationMessage));

        data += xk_pg_parser_MinSizeOfXactInvals;
        data += xl_invals->nmsgs * sizeof(xk_pg_parser_SharedInvalidationMessage);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_TWOPHASE)
    {
        xk_pg_parser_xl_xact_twophase *xl_twophase = (xk_pg_parser_xl_xact_twophase *) data;

        parsed->twophase_xid = xl_twophase->xid;

        data += sizeof(xk_pg_parser_xl_xact_twophase);

        if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_GID)
        {
            strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
            data += strlen(data) + 1;
        }
    }

    /* Note: no alignment is guaranteed after this point */

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_ORIGIN)
    {
        xk_pg_parser_xl_xact_origin xl_origin;

        /* no alignment is guaranteed, so copy onto stack */
        rmemcpy1(&xl_origin, 0, data, sizeof(xl_origin));

        parsed->origin_lsn = xl_origin.origin_lsn;
        parsed->origin_timestamp = xl_origin.origin_timestamp;

        data += sizeof(xk_pg_parser_xl_xact_origin);
    }
}

static void ParseAbortRecord(uint8_t info, xk_pg_parser_xl_xact_abort *xlrec, xk_pg_parser_xl_xact_parsed_abort *parsed)
{
    char       *data = ((char *) xlrec) + xk_pg_parser_MinSizeOfXactCommit;

    rmemset0(parsed, 0, 0, sizeof(*parsed));

    parsed->xinfo = 0;            /* default, if no XLOG_XACT_HAS_INFO is
                                 * present */

    parsed->xact_time = xlrec->xact_time;

    if (info & XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_HAS_INFO)
    {
        xk_pg_parser_xl_xact_xinfo *xl_xinfo = (xk_pg_parser_xl_xact_xinfo *) data;

        parsed->xinfo = xl_xinfo->xinfo;

        data += sizeof(xk_pg_parser_xl_xact_xinfo);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_DBINFO)
    {
        xk_pg_parser_xl_xact_dbinfo *xl_dbinfo = (xk_pg_parser_xl_xact_dbinfo *) data;

        parsed->dbId = xl_dbinfo->dbId;
        parsed->tsId = xl_dbinfo->tsId;

        data += sizeof(xk_pg_parser_xl_xact_dbinfo);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_SUBXACTS)
    {
        xk_pg_parser_xl_xact_subxacts *xl_subxacts = (xk_pg_parser_xl_xact_subxacts *) data;

        parsed->nsubxacts = xl_subxacts->nsubxacts;

        xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **) &parsed->subxacts, parsed->nsubxacts * sizeof(uint32_t));
        rmemcpy0(parsed->subxacts, 0, xl_subxacts->subxacts, parsed->nsubxacts * sizeof(uint32_t));

        data += MinSizeOfXactSubxacts;
        data += parsed->nsubxacts * sizeof(uint32_t);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_RELFILENODES)
    {
        xk_pg_parser_xl_xact_relfilenodes *xl_relfilenodes = (xk_pg_parser_xl_xact_relfilenodes *) data;

        parsed->nrels = xl_relfilenodes->nrels;
        xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **) &parsed->xnodes, xl_relfilenodes->nrels * sizeof(xk_pg_parser_RelFileNode));
        rmemcpy0(parsed->xnodes, 0, xl_relfilenodes->xnodes, xl_relfilenodes->nrels * sizeof(xk_pg_parser_RelFileNode));

        data += xk_pg_parser_MinSizeOfXactRelfilenodes;
        data += xl_relfilenodes->nrels * sizeof(xk_pg_parser_RelFileNode);
    }

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_TWOPHASE)
    {
        xk_pg_parser_xl_xact_twophase *xl_twophase = (xk_pg_parser_xl_xact_twophase *) data;

        parsed->twophase_xid = xl_twophase->xid;

        data += sizeof(xk_pg_parser_xl_xact_twophase);

        if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_GID)
        {
            strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
            data += strlen(data) + 1;
        }
    }

    /* Note: no alignment is guaranteed after this point */

    if (parsed->xinfo & XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_ORIGIN)
    {
        xk_pg_parser_xl_xact_origin xl_origin;

        /* no alignment is guaranteed, so copy onto stack */
        rmemcpy1(&xl_origin, 0, data, sizeof(xl_origin));

        parsed->origin_lsn = xl_origin.origin_lsn;
        parsed->origin_timestamp = xl_origin.origin_timestamp;

        data += sizeof(xk_pg_parser_xl_xact_origin);
    }
}

static bool xk_pg_parser_trans_rmgr_xact_commit(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_xl_xact_commit     *xlrec = (xk_pg_parser_xl_xact_commit *) state->main_data;
    uint8_t             info = state->decoded_record->xl_info;
    xk_pg_parser_translog_pre_trans * trans = NULL;

    /* 检查出入参的合法性 */
    if (!result || !state || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XACT_COMMIT_CHECK;
        xk_pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [xact commit], invalid param\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                                 (void **) (&trans),
                                  sizeof(xk_pg_parser_translog_pre_trans)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    /* 先设置基础信息 */
    trans->m_base.m_type = XK_PG_PARSER_TRANSLOG_XACT_COMMIT;
    trans->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;
    trans->m_status = XK_PG_PARSER_RMGR_XACT_STATUS_COMMIT;

    trans->m_time = xlrec->xact_time;

    if (!xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **)&trans->m_transdata, sizeof(xk_pg_parser_xl_xact_parsed_commit)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    ParseCommitRecord(info, xlrec, (xk_pg_parser_xl_xact_parsed_commit *)trans->m_transdata);

    *result = (xk_pg_parser_translog_pre_base *) trans;

    return true;
}

static bool xk_pg_parser_trans_rmgr_xact_abort(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_xl_xact_abort     *xlrec = (xk_pg_parser_xl_xact_abort *) state->main_data;
    uint8_t             info = state->decoded_record->xl_info;
    xk_pg_parser_translog_pre_trans * trans = NULL;

    /* 检查出入参的合法性 */
    if (!result || !state || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_XACT_ABORT_CHECK;
        xk_pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [xact abort], invalid param\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                                 (void **) (&trans),
                                  sizeof(xk_pg_parser_translog_pre_trans)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    /* 先设置基础信息 */
    trans->m_base.m_type = XK_PG_PARSER_TRANSLOG_XACT_ABORT;
    trans->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;
    trans->m_status = XK_PG_PARSER_RMGR_XACT_STATUS_ABORT;
    trans->m_time = xlrec->xact_time;

    if (!xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT, (void **)&trans->m_transdata, sizeof(xk_pg_parser_xl_xact_parsed_abort)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    ParseAbortRecord(info, xlrec, (xk_pg_parser_xl_xact_parsed_abort *)trans->m_transdata);

    *result = (xk_pg_parser_translog_pre_base *) trans;
    return true;
}

static bool xk_pg_parser_trans_rmgr_xact_commit_prepare(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    bool pre_result = false;
    pre_result = xk_pg_parser_trans_rmgr_xact_commit(state, result, xk_pg_parser_errno);
    (*result)->m_type = XK_PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE;
    return pre_result;
}

static bool xk_pg_parser_trans_rmgr_xact_abort_prepare(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    bool pre_result = false;
    pre_result = xk_pg_parser_trans_rmgr_xact_abort(state, result, xk_pg_parser_errno);
    (*result)->m_type = XK_PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE;
    return pre_result;
}

static bool xk_pg_parser_trans_rmgr_xact_assignment(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_xl_xact_assignment *assignment =
            (xk_pg_parser_xl_xact_assignment *)state->main_data;
    xk_pg_parser_translog_pre_assignment *trans = NULL;

    if (!xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                                 (void **) (&trans),
                                  sizeof(xk_pg_parser_translog_pre_assignment)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_base.m_type = XK_PG_PARSER_TRANSLOG_XACT_ASSIGNMENT;
    trans->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;
    xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                            (void **) (&trans->m_assignment),
                             sizeof(xk_pg_parser_xl_xact_assignment)
                             + sizeof(uint32_t) * assignment->nsubxacts);
    rmemcpy0(trans->m_assignment,
           0,
           assignment,
           sizeof(xk_pg_parser_xl_xact_assignment)
                + sizeof(uint32_t) * assignment->nsubxacts);
    *result = (xk_pg_parser_translog_pre_base *)trans;
    return true;
}

static bool xk_pg_parser_trans_rmgr_xact_prepare(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_base *trans = NULL;

    if (!xk_pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                                 (void **) (&trans),
                                  sizeof(xk_pg_parser_translog_pre_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_type = XK_PG_PARSER_TRANSLOG_XACT_PREPARE;
    trans->m_xid = xk_pg_parser_XLogRecGetXid(state);
    trans->m_originid = state->record_origin;
    *result = trans;
    return true;
}
