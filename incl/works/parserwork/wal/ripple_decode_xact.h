#ifndef _RIPPLE_DECODE_XACT_H
#define _RIPPLE_DECODE_XACT_H

void ripple_decode_xact_commit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void ripple_decode_xact_abort(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void ripple_decode_xact_commit_emit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void ripple_decode_xact_abort_emit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

#endif
