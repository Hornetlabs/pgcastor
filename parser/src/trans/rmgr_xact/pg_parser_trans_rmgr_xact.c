#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_xact/pg_parser_trans_rmgr_xact.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define PG_PARSER_RMGR_XACT_INFOCNT       6
#define RMGR_XACT_MCXT                    NULL

#define PG_PARSER_RMGR_XACT_STATUS_ABORT  (uint8_t)0x01
#define PG_PARSER_RMGR_XACT_STATUS_COMMIT (uint8_t)0x02

#define MinSizeOfXactSubxacts             offsetof(pg_parser_xl_xact_subxacts, subxacts)

typedef bool (*pg_parser_trans_transrec_rmgr_info_func_pre)(pg_parser_XLogReaderState*    state,
                                                            pg_parser_translog_pre_base** result,
                                                            int32_t* pg_parser_errno);
static bool pg_parser_trans_rmgr_xact_commit(pg_parser_XLogReaderState*    state,
                                             pg_parser_translog_pre_base** result,
                                             int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xact_abort(pg_parser_XLogReaderState*    state,
                                            pg_parser_translog_pre_base** result,
                                            int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xact_commit_prepare(pg_parser_XLogReaderState*    state,
                                                     pg_parser_translog_pre_base** result,
                                                     int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xact_abort_prepare(pg_parser_XLogReaderState*    state,
                                                    pg_parser_translog_pre_base** result,
                                                    int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xact_assignment(pg_parser_XLogReaderState*    state,
                                                 pg_parser_translog_pre_base** result,
                                                 int32_t*                      pg_parser_errno);

static bool pg_parser_trans_rmgr_xact_prepare(pg_parser_XLogReaderState*    state,
                                              pg_parser_translog_pre_base** result,
                                              int32_t*                      pg_parser_errno);

typedef struct PG_PARSER_TRANS_RMGR_XLOG
{
    pg_parser_trans_rmgr_xact_info m_infoid; /* info value */
    pg_parser_trans_transrec_rmgr_info_func_pre
        m_infofunc; /* info-level handler for pre-parse interface */
} pg_parser_trans_rmgr_xact;

static pg_parser_trans_rmgr_xact m_record_rmgr_xlog_info[] = {
    {PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT, pg_parser_trans_rmgr_xact_commit},
    {PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT, pg_parser_trans_rmgr_xact_abort},
    {PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT_PREPARED, pg_parser_trans_rmgr_xact_commit_prepare},
    {PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT_PREPARED, pg_parser_trans_rmgr_xact_abort_prepare},
    {PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ASSIGNMENT, pg_parser_trans_rmgr_xact_assignment},
    {PG_PARSER_TRANS_TRANSREC_RMGR_XACT_PREPARE, pg_parser_trans_rmgr_xact_prepare},

};

bool pg_parser_trans_rmgr_xact_pre(pg_parser_XLogReaderState*    state,
                                   pg_parser_translog_pre_base** result,
                                   int32_t*                      pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t  infocnts = PG_PARSER_RMGR_XACT_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_XACT_OPMASK;
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

static size_t pg_parser_strlcpy(char* dst, const char* src, size_t siz)
{
    char*       d = dst;
    const char* s = src;
    size_t      n = siz;

    /* Copy as many bytes as will fit */
    if (n != 0)
    {
        while (--n != 0)
        {
            if ((*d++ = *s++) == '\0')
            {
                break;
            }
        }
    }

    /* Not enough room in dst, add NUL and traverse rest of src */
    if (n == 0)
    {
        if (siz != 0)
        {
            *d = '\0'; /* NUL-terminate dst */
        }
        while (*s++)
            ;
    }

    return (s - src - 1); /* count does not include NUL */
}

static void ParseCommitRecord(uint8_t                          info,
                              pg_parser_xl_xact_commit*        xlrec,
                              pg_parser_xl_xact_parsed_commit* parsed)
{
    char* data = ((char*)xlrec) + pg_parser_MinSizeOfXactCommit;

    rmemset0(parsed, 0, 0, sizeof(*parsed));

    parsed->xinfo = 0; /* default, if no XLOG_XACT_HAS_INFO is
                        * present */

    parsed->xact_time = xlrec->xact_time;

    if (info & PG_PARSER_TRANS_TRANSREC_RMGR_XACT_HAS_INFO)
    {
        pg_parser_xl_xact_xinfo* xl_xinfo = (pg_parser_xl_xact_xinfo*)data;

        parsed->xinfo = xl_xinfo->xinfo;

        data += sizeof(pg_parser_xl_xact_xinfo);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_DBINFO)
    {
        pg_parser_xl_xact_dbinfo* xl_dbinfo = (pg_parser_xl_xact_dbinfo*)data;

        parsed->dbId = xl_dbinfo->dbId;
        parsed->tsId = xl_dbinfo->tsId;

        data += sizeof(pg_parser_xl_xact_dbinfo);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_SUBXACTS)
    {
        pg_parser_xl_xact_subxacts* xl_subxacts = (pg_parser_xl_xact_subxacts*)data;

        parsed->nsubxacts = xl_subxacts->nsubxacts;

        pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)&parsed->subxacts, parsed->nsubxacts * sizeof(uint32_t));
        rmemcpy0(parsed->subxacts, 0, xl_subxacts->subxacts, parsed->nsubxacts * sizeof(uint32_t));

        data += MinSizeOfXactSubxacts;
        data += parsed->nsubxacts * sizeof(uint32_t);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_RELFILENODES)
    {
        pg_parser_xl_xact_relfilenodes* xl_relfilenodes = (pg_parser_xl_xact_relfilenodes*)data;

        parsed->nrels = xl_relfilenodes->nrels;
        pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                              (void**)&parsed->xnodes,
                              xl_relfilenodes->nrels * sizeof(pg_parser_RelFileNode));
        rmemcpy0(parsed->xnodes,
                 0,
                 xl_relfilenodes->xnodes,
                 xl_relfilenodes->nrels * sizeof(pg_parser_RelFileNode));

        data += pg_parser_MinSizeOfXactRelfilenodes;
        data += xl_relfilenodes->nrels * sizeof(pg_parser_RelFileNode);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_INVALS)
    {
        pg_parser_xl_xact_invals* xl_invals = (pg_parser_xl_xact_invals*)data;

        parsed->nmsgs = xl_invals->nmsgs;
        parsed->msgs = xl_invals->msgs;

        pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                              (void**)&parsed->msgs,
                              xl_invals->nmsgs * sizeof(pg_parser_SharedInvalidationMessage));
        rmemcpy0(parsed->msgs,
                 0,
                 xl_invals->msgs,
                 xl_invals->nmsgs * sizeof(pg_parser_SharedInvalidationMessage));

        data += pg_parser_MinSizeOfXactInvals;
        data += xl_invals->nmsgs * sizeof(pg_parser_SharedInvalidationMessage);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_TWOPHASE)
    {
        pg_parser_xl_xact_twophase* xl_twophase = (pg_parser_xl_xact_twophase*)data;

        parsed->twophase_xid = xl_twophase->xid;

        data += sizeof(pg_parser_xl_xact_twophase);

        if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_GID)
        {
            pg_parser_strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
            data += strlen(data) + 1;
        }
    }

    /* Note: no alignment is guaranteed after this point */

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_ORIGIN)
    {
        pg_parser_xl_xact_origin xl_origin;

        /* no alignment is guaranteed, so copy onto stack */
        rmemcpy1(&xl_origin, 0, data, sizeof(xl_origin));

        parsed->origin_lsn = xl_origin.origin_lsn;
        parsed->origin_timestamp = xl_origin.origin_timestamp;

        data += sizeof(pg_parser_xl_xact_origin);
    }
}

static void ParseAbortRecord(uint8_t                         info,
                             pg_parser_xl_xact_abort*        xlrec,
                             pg_parser_xl_xact_parsed_abort* parsed)
{
    char* data = ((char*)xlrec) + pg_parser_MinSizeOfXactCommit;

    rmemset0(parsed, 0, 0, sizeof(*parsed));

    parsed->xinfo = 0; /* default, if no XLOG_XACT_HAS_INFO is
                        * present */

    parsed->xact_time = xlrec->xact_time;

    if (info & PG_PARSER_TRANS_TRANSREC_RMGR_XACT_HAS_INFO)
    {
        pg_parser_xl_xact_xinfo* xl_xinfo = (pg_parser_xl_xact_xinfo*)data;

        parsed->xinfo = xl_xinfo->xinfo;

        data += sizeof(pg_parser_xl_xact_xinfo);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_DBINFO)
    {
        pg_parser_xl_xact_dbinfo* xl_dbinfo = (pg_parser_xl_xact_dbinfo*)data;

        parsed->dbId = xl_dbinfo->dbId;
        parsed->tsId = xl_dbinfo->tsId;

        data += sizeof(pg_parser_xl_xact_dbinfo);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_SUBXACTS)
    {
        pg_parser_xl_xact_subxacts* xl_subxacts = (pg_parser_xl_xact_subxacts*)data;

        parsed->nsubxacts = xl_subxacts->nsubxacts;

        pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)&parsed->subxacts, parsed->nsubxacts * sizeof(uint32_t));
        rmemcpy0(parsed->subxacts, 0, xl_subxacts->subxacts, parsed->nsubxacts * sizeof(uint32_t));

        data += MinSizeOfXactSubxacts;
        data += parsed->nsubxacts * sizeof(uint32_t);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_RELFILENODES)
    {
        pg_parser_xl_xact_relfilenodes* xl_relfilenodes = (pg_parser_xl_xact_relfilenodes*)data;

        parsed->nrels = xl_relfilenodes->nrels;
        pg_parser_mcxt_malloc(RMGR_XACT_MCXT,
                              (void**)&parsed->xnodes,
                              xl_relfilenodes->nrels * sizeof(pg_parser_RelFileNode));
        rmemcpy0(parsed->xnodes,
                 0,
                 xl_relfilenodes->xnodes,
                 xl_relfilenodes->nrels * sizeof(pg_parser_RelFileNode));

        data += pg_parser_MinSizeOfXactRelfilenodes;
        data += xl_relfilenodes->nrels * sizeof(pg_parser_RelFileNode);
    }

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_TWOPHASE)
    {
        pg_parser_xl_xact_twophase* xl_twophase = (pg_parser_xl_xact_twophase*)data;

        parsed->twophase_xid = xl_twophase->xid;

        data += sizeof(pg_parser_xl_xact_twophase);

        if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_GID)
        {
            pg_parser_strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
            data += strlen(data) + 1;
        }
    }

    /* Note: no alignment is guaranteed after this point */

    if (parsed->xinfo & PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_ORIGIN)
    {
        pg_parser_xl_xact_origin xl_origin;

        /* no alignment is guaranteed, so copy onto stack */
        rmemcpy1(&xl_origin, 0, data, sizeof(xl_origin));

        parsed->origin_lsn = xl_origin.origin_lsn;
        parsed->origin_timestamp = xl_origin.origin_timestamp;

        data += sizeof(pg_parser_xl_xact_origin);
    }
}

static bool pg_parser_trans_rmgr_xact_commit(pg_parser_XLogReaderState*    state,
                                             pg_parser_translog_pre_base** result,
                                             int32_t*                      pg_parser_errno)
{
    pg_parser_xl_xact_commit*     xlrec = (pg_parser_xl_xact_commit*)state->main_data;
    uint8_t                       info = state->decoded_record->xl_info;
    pg_parser_translog_pre_trans* trans = NULL;

    /* Check validity of input/output parameters */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XACT_COMMIT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                             "ERROR: pre record is [xact commit], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)(&trans), sizeof(pg_parser_translog_pre_trans)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    /* Set basic information first */
    trans->m_base.m_type = PG_PARSER_TRANSLOG_XACT_COMMIT;
    trans->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;
    trans->m_status = PG_PARSER_RMGR_XACT_STATUS_COMMIT;

    trans->m_time = xlrec->xact_time;

    if (!pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)&trans->m_transdata, sizeof(pg_parser_xl_xact_parsed_commit)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    ParseCommitRecord(info, xlrec, (pg_parser_xl_xact_parsed_commit*)trans->m_transdata);

    *result = (pg_parser_translog_pre_base*)trans;

    return true;
}

static bool pg_parser_trans_rmgr_xact_abort(pg_parser_XLogReaderState*    state,
                                            pg_parser_translog_pre_base** result,
                                            int32_t*                      pg_parser_errno)
{
    pg_parser_xl_xact_abort*      xlrec = (pg_parser_xl_xact_abort*)state->main_data;
    uint8_t                       info = state->decoded_record->xl_info;
    pg_parser_translog_pre_trans* trans = NULL;

    /* Check validity of input/output parameters */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_XACT_ABORT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                             "ERROR: pre record is [xact abort], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)(&trans), sizeof(pg_parser_translog_pre_trans)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    /* Set basic information first */
    trans->m_base.m_type = PG_PARSER_TRANSLOG_XACT_ABORT;
    trans->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;
    trans->m_status = PG_PARSER_RMGR_XACT_STATUS_ABORT;
    trans->m_time = xlrec->xact_time;

    if (!pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)&trans->m_transdata, sizeof(pg_parser_xl_xact_parsed_abort)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    ParseAbortRecord(info, xlrec, (pg_parser_xl_xact_parsed_abort*)trans->m_transdata);

    *result = (pg_parser_translog_pre_base*)trans;
    return true;
}

static bool pg_parser_trans_rmgr_xact_commit_prepare(pg_parser_XLogReaderState*    state,
                                                     pg_parser_translog_pre_base** result,
                                                     int32_t*                      pg_parser_errno)
{
    bool pre_result = false;
    pre_result = pg_parser_trans_rmgr_xact_commit(state, result, pg_parser_errno);
    (*result)->m_type = PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE;
    return pre_result;
}

static bool pg_parser_trans_rmgr_xact_abort_prepare(pg_parser_XLogReaderState*    state,
                                                    pg_parser_translog_pre_base** result,
                                                    int32_t*                      pg_parser_errno)
{
    bool pre_result = false;
    pre_result = pg_parser_trans_rmgr_xact_abort(state, result, pg_parser_errno);
    (*result)->m_type = PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE;
    return pre_result;
}

static bool pg_parser_trans_rmgr_xact_assignment(pg_parser_XLogReaderState*    state,
                                                 pg_parser_translog_pre_base** result,
                                                 int32_t*                      pg_parser_errno)
{
    pg_parser_xl_xact_assignment*      assignment = (pg_parser_xl_xact_assignment*)state->main_data;
    pg_parser_translog_pre_assignment* trans = NULL;

    if (!pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)(&trans), sizeof(pg_parser_translog_pre_assignment)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_base.m_type = PG_PARSER_TRANSLOG_XACT_ASSIGNMENT;
    trans->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;
    pg_parser_mcxt_malloc(
        RMGR_XACT_MCXT,
        (void**)(&trans->m_assignment),
        sizeof(pg_parser_xl_xact_assignment) + sizeof(uint32_t) * assignment->nsubxacts);
    rmemcpy0(trans->m_assignment,
             0,
             assignment,
             sizeof(pg_parser_xl_xact_assignment) + sizeof(uint32_t) * assignment->nsubxacts);
    *result = (pg_parser_translog_pre_base*)trans;
    return true;
}

static bool pg_parser_trans_rmgr_xact_prepare(pg_parser_XLogReaderState*    state,
                                              pg_parser_translog_pre_base** result,
                                              int32_t*                      pg_parser_errno)
{
    pg_parser_translog_pre_base* trans = NULL;

    if (!pg_parser_mcxt_malloc(
            RMGR_XACT_MCXT, (void**)(&trans), sizeof(pg_parser_translog_pre_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_type = PG_PARSER_TRANSLOG_XACT_PREPARE;
    trans->m_xid = pg_parser_XLogRecGetXid(state);
    trans->m_originid = state->record_origin;
    *result = trans;
    return true;
}
