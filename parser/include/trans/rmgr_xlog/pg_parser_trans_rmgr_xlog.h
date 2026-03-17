#ifndef XK_PG_PARSER_TRANS_RMGR_XLOG_H
#define XK_PG_PARSER_TRANS_RMGR_XLOG_H

typedef enum XK_PG_PARSER_TRANS_RMGR_XLOG_INFO
{
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_SHUTDOWN = 0x00,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_CHECKPOINT_ONLINE = 0x10,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_SWITCH = 0x40,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XLOG_END_OF_RECOVERY = 0x90
} xk_pg_parser_trans_rmgr_xlog_info;

extern bool xk_pg_parser_trans_rmgr_xlog_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result, 
                            int32_t *xk_pg_parser_errno);

#endif
