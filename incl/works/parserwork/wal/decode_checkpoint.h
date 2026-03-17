#ifndef _DECODE_CHECKPOINT_H
#define _DECODE_CHECKPOINT_H

void decode_chkpt_init(decodingcontext* ctx, XLogRecPtr redolsn);

void decode_chkpt(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void decode_recovery(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

#endif
