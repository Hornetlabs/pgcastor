#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_standby/pg_parser_trans_rmgr_standby.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define RMGR_STANDBY_MCXT NULL

bool pg_parser_trans_rmgr_standby_pre(pg_parser_trans_transrec_decode_XLogReaderState* state,
                                      pg_parser_translog_pre_base**                    result,
                                      int32_t* pg_parser_errno)
{
    pg_parser_translog_pre_running_xact* standby_result = NULL;
    uint8_t                              info = state->decoded_record->xl_info;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    if (info == PG_PARSER_XLOG_RUNNING_XACTS)
    {
        pg_parser_xl_running_xacts* running =
            (pg_parser_xl_running_xacts*)pg_parser_XLogRecGetData(state);
        if (!pg_parser_mcxt_malloc(RMGR_STANDBY_MCXT, (void**)&standby_result,
                                   sizeof(pg_parser_translog_pre_running_xact)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_PRE_STANDBY_MEMALLOC_ERROR1;
            return false;
        }
        if (!pg_parser_mcxt_malloc(
                RMGR_STANDBY_MCXT, (void**)&standby_result->m_standby,
                sizeof(pg_parser_xl_running_xacts) + sizeof(uint32_t) * running->xcnt))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_PRE_STANDBY_MEMALLOC_ERROR2;
            return false;
        }
        standby_result->m_base.m_type = PG_PARSER_TRANSLOG_RUNNING_XACTS;
        standby_result->m_base.m_xid = pg_parser_XLogRecGetXid(state);
        standby_result->m_base.m_originid = state->record_origin;
        rmemcpy0((void*)standby_result->m_standby, 0, (void*)running,
                 sizeof(pg_parser_xl_running_xacts) + sizeof(uint32_t) * running->xcnt);
        *result = (pg_parser_translog_pre_base*)standby_result;
        return true;
    }
    return false;
}
