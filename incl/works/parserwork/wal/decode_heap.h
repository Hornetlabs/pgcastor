#ifndef DECODE_HEAP_H
#define DECODE_HEAP_H

extern void decode_heap(decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);
extern void heap_fpw_tuples(decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);
extern void heap_truncate(decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);

extern void decode_heap_emit(decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);
#endif
