#ifndef ONLINEREFRESH_CAPTURE_DECODE_XACT_H
#define ONLINEREFRESH_CAPTURE_DECODE_XACT_H

extern void onlinerefresh_decode_xact_commit(decodingcontext* ctx, pg_parser_translog_pre_base* pbase);
extern void onlinerefresh_decode_xact_abort(decodingcontext* ctx, pg_parser_translog_pre_base* pbase);

#endif
