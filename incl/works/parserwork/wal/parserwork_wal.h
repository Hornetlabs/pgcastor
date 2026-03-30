#ifndef _PARSERWORK_WAL_H
#define _PARSERWORK_WAL_H

void parserwork_stat_setrewind(decodingcontext* decodingctx);

void parserwork_stat_setpause(decodingcontext* decodingctx);

void parserwork_stat_setresume(decodingcontext* decodingctx);

HTAB* decodingcontext_stat_getsyncdataset(decodingcontext* decodingctx);

extern void parserwork_decodingctx_removeonlinerefresh(decodingcontext* ctx, onlinerefresh* onlinerefresh);

extern void parserwork_decodingctx_addonlinerefresh(decodingcontext* ctx, onlinerefresh* onlinerefresh, txn* txn);

void parserwork_stat_setrunning(decodingcontext* decodingctx);

void* parserwork_wal_main(void* args);

void parserwork_wal_destroy(decodingcontext* decodingctx);

decodingcontext* parserwork_walinitphase1(void);

void parserwork_walinitphase2(decodingcontext* decodingctx);

void parserwork_wal_getpos(decodingcontext* decodingctx, uint64* fileid, uint64* fileoffset);

void parserwork_wal_getparserinfo(decodingcontext* decodingctx, XLogRecPtr* prestartlsn, XLogRecPtr* pconfirmlsn);

bool parserwork_wal_initfromdb(decodingcontext* decodingctx);

bool parserwork_buildrefreshtransaction(decodingcontext* decodingctx, refresh_tables* tables);

txn* parserwork_build_onlinerefresh_begin_txn(txnstmt_onlinerefresh* olstmt, XLogRecPtr parserlsn);
txn* parserwork_build_onlinerefresh_end_txn(unsigned char* uuid, XLogRecPtr parserlsn);
txn* parserwork_build_onlinerefresh_increment_end_txn(unsigned char* uuid);

#endif
