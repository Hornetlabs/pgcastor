#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_relmap/pg_parser_trans_rmgr_relmap.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define RMGR_RELMAP_MCXT NULL

bool pg_parser_trans_rmgr_relmap_pre(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                        pg_parser_translog_pre_base **result,
                                        int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_relmap *relmap_result = NULL;
    uint8_t info = state->decoded_record->xl_info;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    if (info == PG_PARSER_XLOG_RELMAP_UPDATE)
    {
        pg_parser_xl_relmap_update *xlrec = (pg_parser_xl_relmap_update *) state->main_data;
        pg_parser_RelMapFile       *newmap = (pg_parser_RelMapFile *) xlrec->data;

        if (xlrec->nbytes != sizeof(pg_parser_RelMapFile))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_PRE_RELMAP_SIZE_CHECK;
            return false;
        }

        pg_parser_mcxt_malloc(RMGR_RELMAP_MCXT, (void **) &relmap_result, sizeof(pg_parser_translog_pre_relmap));
        if (newmap->num_mappings > 0)
            pg_parser_mcxt_malloc(RMGR_RELMAP_MCXT, (void **) &(relmap_result->m_mapping), sizeof(pg_parser_RelMapping) * newmap->num_mappings);

        relmap_result->m_base.m_type = PG_PARSER_TRANSLOG_RELMAP;
        relmap_result->m_base.m_xid = pg_parser_XLogRecGetXid(state);
        relmap_result->m_base.m_originid = state->record_origin;
        relmap_result->m_count = newmap->num_mappings;
        relmap_result->m_dboid = xlrec->dbid;
        if (newmap->num_mappings > 0)
        {
            rmemcpy0(relmap_result->m_mapping, 0, newmap->mappings, sizeof(pg_parser_RelMapping) * relmap_result->m_count);
        }
        *result = (pg_parser_translog_pre_base *)relmap_result;
        return true;
    }
    return false;
}
