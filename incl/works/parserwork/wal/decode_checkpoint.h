#ifndef _RIPPLE_DECODE_CHECKPOINT_H
#define _RIPPLE_DECODE_CHECKPOINT_H

void ripple_decode_chkpt_init(ripple_decodingcontext* ctx, XLogRecPtr redolsn);

void ripple_decode_chkpt(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void ripple_decode_recovery(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

#endif
