#ifndef XK_PG_PARSER_TRANS_RMGR_SEQ_H
#define XK_PG_PARSER_TRANS_RMGR_SEQ_H

extern bool xk_pg_parser_trans_rmgr_seq_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                            xk_pg_parser_translog_pre_base **result, 
                                            int32_t *xk_pg_parser_errno);

#endif
