#ifndef RIPPLE_DECODE_HEAP_H
#define RIPPLE_DECODE_HEAP_H

extern void ripple_decode_heap(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);
extern void ripple_heap_fpw_tuples(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);
extern void ripple_heap_truncate(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);

extern void ripple_decode_heap_emit(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);
#endif
