#ifndef _DATABASERECV_H
#define _DATABASERECV_H

typedef struct CHECKPOINT
{
    XLogRecPtr    redolsn;
    TimeLineID    tlid;
    TransactionId nextfullxid;
} checkpoint;

checkpoint* databaserecv_checkpoint_get(PGconn* conn);

XLogRecPtr databaserecv_currentlsn_get(PGconn* conn);

char* databaserecv_monetary_get(PGconn* conn);

char* databaserecv_numeric_get(PGconn* conn);

char* databaserecv_timezone_get(PGconn* conn);

char* databaserecv_orgencoding_get(PGconn* conn);

Oid databaserecv_database_get(PGconn* conn);

TransactionId databaserecv_transactionid_get(PGconn* conn);

TimestampTz databaserecv_timestamp_get(PGconn* conn);

void databaserecv_checkpoint(PGconn* conn);

bool databaserecv_trigger_set(PGconn* conn);

bool databaserecv_synctable_set(PGconn* conn);

bool databaserecv_integrate_dbinit(void);

/* Execute IDENTIFY_SYSTEM and get return value */
bool databaserecv_identifysystem(PGconn* conn, TimeLineID* tli, XLogRecPtr* dblsn);

/* Execute SHOW wal_segment_size and get return value */
bool databaserecv_showwalsegmentsize(PGconn* conn, uint32* segsize);

/* Get xlogblocksize */
bool databaserecv_showwalblocksize(PGconn* conn, int* blksize);

/* Get server version */
bool databaserecv_showserverversion(PGconn* conn, char** strversion);

/* Whether FDE is enabled at compile time */
bool databaserecv_getconfigurefde(PGconn* conn, bool* fde);

/* Get timeline file data */
bool databaserecv_timelinehistory(PGconn* conn, TimeLineID tli, char** pfilename, char** pcontent);

/* Execute start replication */
bool databaserecv_startreplication(PGconn* conn, TimeLineID tli, XLogRecPtr startpos, char* slotname);

#endif
