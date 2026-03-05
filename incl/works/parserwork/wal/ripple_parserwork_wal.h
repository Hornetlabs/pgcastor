#ifndef _RIPPLE_PARSERWORK_WAL_H
#define _RIPPLE_PARSERWORK_WAL_H

void ripple_parserwork_stat_setrewind(ripple_decodingcontext* decodingctx);

void ripple_parserwork_stat_setpause(ripple_decodingcontext* decodingctx);

void ripple_parserwork_stat_setresume(ripple_decodingcontext* decodingctx);

HTAB *decodingcontext_stat_getsyncdataset(ripple_decodingcontext* decodingctx);

extern void ripple_parserwork_decodingctx_removeonlinerefresh(ripple_decodingcontext* ctx,
                                                              ripple_onlinerefresh* onlinerefresh);

extern void ripple_parserwork_decodingctx_addonlinerefresh(ripple_decodingcontext* ctx,
                                                           ripple_onlinerefresh* onlinerefresh,
                                                           ripple_txn* txn);

void ripple_parserwork_stat_setrunning(ripple_decodingcontext* decodingctx);

void* ripple_parserwork_wal_main(void *args);

void ripple_parserwork_wal_destroy(ripple_decodingcontext* decodingctx);

ripple_decodingcontext* ripple_parserwork_walinitphase1(void);

void ripple_parserwork_walinitphase2(ripple_decodingcontext* decodingctx);

void ripple_parserwork_wal_getpos(ripple_decodingcontext* decodingctx, uint64* fileid, uint64* fileoffset);

void ripple_parserwork_wal_getparserinfo(ripple_decodingcontext* decodingctx, XLogRecPtr* prestartlsn, XLogRecPtr* pconfirmlsn);

bool ripple_parserwork_wal_initfromdb(ripple_decodingcontext* decodingctx);

bool ripple_parserwork_buildrefreshtransaction(ripple_decodingcontext* decodingctx, ripple_refresh_tables* tables);

ripple_txn *ripple_parserwork_build_onlinerefresh_begin_txn(ripple_txnstmt_onlinerefresh *olstmt, XLogRecPtr parserlsn);
ripple_txn *ripple_parserwork_build_onlinerefresh_end_txn(unsigned char *uuid, XLogRecPtr parserlsn);
ripple_txn *ripple_parserwork_build_onlinerefresh_increment_end_txn(unsigned char *uuid);

#endif
