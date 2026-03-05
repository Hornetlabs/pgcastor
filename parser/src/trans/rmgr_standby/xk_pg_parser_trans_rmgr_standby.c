#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_standby/xk_pg_parser_trans_rmgr_standby.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_fmgr.h"

#define RMGR_STANDBY_MCXT NULL

bool xk_pg_parser_trans_rmgr_standby_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                        xk_pg_parser_translog_pre_base **result,
                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_running_xact *standby_result = NULL;
    uint8_t info = state->decoded_record->xl_info;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    if (info == XK_PG_PARSER_XLOG_RUNNING_XACTS)
    {
        xk_pg_parser_xl_running_xacts *running =
                (xk_pg_parser_xl_running_xacts *) xk_pg_parser_XLogRecGetData(state);
        if (!xk_pg_parser_mcxt_malloc(RMGR_STANDBY_MCXT, (void **)&standby_result, sizeof(xk_pg_parser_translog_pre_running_xact)))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_STANDBY_MEMALLOC_ERROR1;
            return false;
        }
        if (!xk_pg_parser_mcxt_malloc(RMGR_STANDBY_MCXT,
                                     (void **)&standby_result->m_standby,
                                      sizeof(xk_pg_parser_xl_running_xacts) + sizeof(uint32_t) * running->xcnt))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_STANDBY_MEMALLOC_ERROR2;
            return false;
        }
        standby_result->m_base.m_type = XK_PG_PARSER_TRANSLOG_RUNNING_XACTS;
        standby_result->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
        standby_result->m_base.m_originid = state->record_origin;
        rmemcpy0((void *)standby_result->m_standby,
                0,
               (void *)running,
                sizeof(xk_pg_parser_xl_running_xacts) + sizeof(uint32_t) * running->xcnt);
        *result = (xk_pg_parser_translog_pre_base *)standby_result;
        return true;
    }
    return false;
}
