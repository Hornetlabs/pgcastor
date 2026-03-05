#ifndef _RIPPLE_DATABASERECV_H
#define _RIPPLE_DATABASERECV_H

typedef struct RIPPLE_CHECKPOINT
{
    XLogRecPtr              redolsn;
    TimeLineID              tlid;
    TransactionId           nextfullxid;
}ripple_checkpoint;

ripple_checkpoint* ripple_databaserecv_checkpoint_get(PGconn* conn);

XLogRecPtr ripple_databaserecv_currentlsn_get(PGconn* conn);

char* ripple_databaserecv_monetary_get(PGconn* conn);

char* ripple_databaserecv_numeric_get(PGconn* conn);

char* ripple_databaserecv_timezone_get(PGconn* conn);

char* ripple_databaserecv_orgencoding_get(PGconn* conn);

Oid ripple_databaserecv_database_get(PGconn* conn);

TransactionId ripple_databaserecv_transactionid_get(PGconn* conn);

TimestampTz ripple_databaserecv_timestamp_get(PGconn* conn);

void ripple_databaserecv_checkpoint(PGconn* conn);

bool ripple_databaserecv_trigger_set(PGconn* conn);

bool ripple_databaserecv_synctable_set(PGconn* conn);

bool ripple_databaserecv_integrate_dbinit(void);

/* 执行 IDENTIFY_SYSTEM 并获取返回值 */
bool ripple_databaserecv_identifysystem(PGconn* conn, TimeLineID* tli, XLogRecPtr* dblsn);

/* 执行 SHOW wal_segment_size 并获取返回值 */
bool ripple_databaserecv_showwalsegmentsize(PGconn* conn, uint32* segsize);

/* 获取 xlogblocksize */
bool ripple_databaserecv_showwalblocksize(PGconn* conn, int* blksize);

/* 获取 server version */
bool ripple_databaserecv_showserverversion(PGconn* conn, char** strversion);

/* 编译时是否开启 FDE */
bool ripple_databaserecv_getconfigurefde(PGconn* conn, bool *fde);

/* 获取时间线文件数据 */
bool ripple_databaserecv_timelinehistory(PGconn* conn, TimeLineID tli, char** pfilename, char** pcontent);

/* 执行 start replication */
bool ripple_databaserecv_startreplication(PGconn* conn, TimeLineID tli, XLogRecPtr startpos, char* slotname);

#endif
