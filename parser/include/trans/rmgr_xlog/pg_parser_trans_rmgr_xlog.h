#ifndef PG_PARSER_TRANS_RMGR_XLOG_H
#define PG_PARSER_TRANS_RMGR_XLOG_H

typedef enum PG_PARSER_TRANS_RMGR_XLOG_INFO
{
    PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_SHUTDOWN = 0x00,
    PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_ONLINE = 0x10,
    PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_SWITCH = 0x40,
    PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_END_OF_RECOVERY = 0x90
} pg_parser_trans_rmgr_xlog_info;

extern bool pg_parser_trans_rmgr_xlog_pre(pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result, 
                            int32_t *pg_parser_errno);

#endif
