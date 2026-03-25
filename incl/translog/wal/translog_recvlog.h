#ifndef _TRANSLOG_RECVLOG_H_
#define _TRANSLOG_RECVLOG_H_

#define XLOG_BLKSIZE 8192
#define PGWALSEGMENTSPERXLOGID(segsize) (UINT64CONST(0x100000000) / (segsize))
#define PGWALBYTETOSEG(lsn, segsize) (lsn / segsize)
#define PGWALSEGMENTOFFSET(lsn, segsize) ((lsn) & ((segsize) - 1))

typedef struct TRANSLOG_RECVLOG
{
    /* FDE encryption */
    bool enablefde;

    /* flag for whether DONE was actively sent, 0 = not sent, 1 = sent */
    bool senddone;

    /* database type */
    translog_recvlog_dbtype dbtype;

    /* database version */
    translog_recvlog_dbversion dbversion;

    /* log file descriptor */
    int fd;

    /* transaction log size */
    uint32 segsize;

    /* sync timeline */
    TimeLineID tli;

    /* database timeline */
    TimeLineID dbtli;

    /* transaction log sequence number */
    uint64 segno;

    /* sync LSN */
    XLogRecPtr startpos;

    /* database connection string */
    /* working directory */
    char* data;

    /* database instance identifier */
    char* sysidentifier;

    /* slotname */
    char* slotname;

    /* restorecommand */
    char* restorecmd;
} translog_recvlog;

/* initialize structure */
translog_recvlog* translog_recvlog_init(void);

/* timeline */
void translog_recvlog_settli(translog_recvlog* recvwal, TimeLineID tli);

/* set startpos */
void translog_recvlog_setstartpos(translog_recvlog* recvwal, XLogRecPtr lsn);

/* database timeline */
void translog_recvlog_setdbtli(translog_recvlog* recvwal, TimeLineID tli);

/* set segsize */
void translog_recvlog_setsegsize(translog_recvlog* recvwal, uint32 segsize);

/* set dbtype */
void translog_recvlog_setdbtype(translog_recvlog* recvwal, translog_recvlog_dbtype dbtype);

/* set data directory */
bool translog_recvlog_setdata(translog_recvlog* recvwal, char* data);

/* set sysidentifier */
bool translog_recvlog_setsysidentifier(translog_recvlog* recvwal, char* sysidentifier);

/* set slotname */
bool translog_recvlog_setslotname(translog_recvlog* recvwal, char* slotname);

/* set restore command */
bool translog_recvlog_setrestorecmd(translog_recvlog* recvwal, char* restorecmd);

/* streaming replication log receiver */
bool translog_recvlog_main(translog_recvlog* recvwal);

/* free resources */
void translog_recvlog_free(translog_recvlog* recvwal);

#endif
