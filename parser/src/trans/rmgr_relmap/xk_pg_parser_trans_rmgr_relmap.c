#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_relmap/xk_pg_parser_trans_rmgr_relmap.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_fmgr.h"

#define RMGR_RELMAP_MCXT NULL

bool xk_pg_parser_trans_rmgr_relmap_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                        xk_pg_parser_translog_pre_base **result,
                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_pre_relmap *relmap_result = NULL;
    uint8_t info = state->decoded_record->xl_info;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    if (info == XK_PG_PARSER_XLOG_RELMAP_UPDATE)
    {
        xk_pg_parser_xl_relmap_update *xlrec = (xk_pg_parser_xl_relmap_update *) state->main_data;
        xk_pg_parser_RelMapFile       *newmap = (xk_pg_parser_RelMapFile *) xlrec->data;

        if (xlrec->nbytes != sizeof(xk_pg_parser_RelMapFile))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_RELMAP_SIZE_CHECK;
            return false;
        }

        xk_pg_parser_mcxt_malloc(RMGR_RELMAP_MCXT, (void **) &relmap_result, sizeof(xk_pg_parser_translog_pre_relmap));
        if (newmap->num_mappings > 0)
            xk_pg_parser_mcxt_malloc(RMGR_RELMAP_MCXT, (void **) &(relmap_result->m_mapping), sizeof(xk_pg_parser_RelMapping) * newmap->num_mappings);

        relmap_result->m_base.m_type = XK_PG_PARSER_TRANSLOG_RELMAP;
        relmap_result->m_base.m_xid = xk_pg_parser_XLogRecGetXid(state);
        relmap_result->m_base.m_originid = state->record_origin;
        relmap_result->m_count = newmap->num_mappings;
        relmap_result->m_dboid = xlrec->dbid;
        if (newmap->num_mappings > 0)
        {
            rmemcpy0(relmap_result->m_mapping, 0, newmap->mappings, sizeof(xk_pg_parser_RelMapping) * relmap_result->m_count);
        }
        *result = (xk_pg_parser_translog_pre_base *)relmap_result;
        return true;
    }
    return false;
}
