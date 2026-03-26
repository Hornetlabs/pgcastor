#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_seq/pg_parser_trans_rmgr_seq.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"

#define RMGR_SEQ_MCXT NULL

typedef struct pg_parser_xl_seq_rec
{
    pg_parser_RelFileNode node;
    /* SEQUENCE TUPLE DATA FOLLOWS AT THE END */
} pg_parser_xl_seq_rec;

typedef struct pg_parser_FormData_pg_sequence_data
{
    int64_t last_value;
    int64_t log_cnt;
    bool    is_called;
} pg_parser_FormData_pg_sequence_data;

typedef pg_parser_FormData_pg_sequence_data* pg_parser_Form_pg_sequence_data;

#define SEQ_REC_SIZE sizeof(pg_parser_xl_seq_rec)

bool pg_parser_trans_rmgr_seq_pre(pg_parser_XLogReaderState*    state,
                                  pg_parser_translog_pre_base** result,
                                  int32_t*                      pg_parser_errno)
{
    char*                     main_data = state->main_data;

    pg_parser_xl_seq_rec*     seq_rec = (pg_parser_xl_seq_rec*)main_data;
    pg_parser_HeapTupleHeader seq_tuple = (pg_parser_HeapTupleHeader)(main_data + SEQ_REC_SIZE);
    pg_parser_Form_pg_sequence_data seq = NULL;
    pg_parser_translog_pre_seq*     pre_seq = NULL;

    seq = (pg_parser_Form_pg_sequence_data)(((char*)seq_tuple) + seq_tuple->t_hoff);

    if (!pg_parser_mcxt_malloc(
            RMGR_SEQ_MCXT, (void**)(&pre_seq), sizeof(pg_parser_translog_pre_seq)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_STANDBY_MEMALLOC_ERROR1;
        return false;
    }
    pre_seq->m_base.m_type = PG_PARSER_TRANSLOG_SEQ;
    pre_seq->m_last_value = seq->last_value;
    pre_seq->m_relfilenode = seq_rec->node.relNode;
    pre_seq->m_dboid = seq_rec->node.dbNode;
    pre_seq->m_tbspcoid = seq_rec->node.spcNode;
    pre_seq->m_base.m_xid = state->decoded_record->xl_xid;
    pre_seq->m_base.m_originid = state->record_origin;
    *result = (pg_parser_translog_pre_base*)pre_seq;
    return true;
}
