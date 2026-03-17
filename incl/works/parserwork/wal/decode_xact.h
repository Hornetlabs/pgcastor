#ifndef _DECODE_XACT_H
#define _DECODE_XACT_H

void decode_xact_commit(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void decode_xact_abort(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void decode_xact_commit_emit(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

void decode_xact_abort_emit(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase);

#endif
