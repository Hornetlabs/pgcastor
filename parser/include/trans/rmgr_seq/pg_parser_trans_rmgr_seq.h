#ifndef PG_PARSER_TRANS_RMGR_SEQ_H
#define PG_PARSER_TRANS_RMGR_SEQ_H

extern bool pg_parser_trans_rmgr_seq_pre(pg_parser_trans_transrec_decode_XLogReaderState* state,
                                         pg_parser_translog_pre_base**                    result,
                                         int32_t* pg_parser_errno);

#endif
