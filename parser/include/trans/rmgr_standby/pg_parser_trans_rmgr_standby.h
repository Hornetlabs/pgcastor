#ifndef PG_PARSER_TRANS_RMGR_STANDBY_H
#define PG_PARSER_TRANS_RMGR_STANDBY_H

#define PG_PARSER_XLOG_RUNNING_XACTS 0x10

typedef struct pg_parser_xl_running_xacts
{
    int             xcnt;               /* # of xact ids in xids[] */
    int             subxcnt;            /* # of subxact ids in xids[] */
    bool            subxid_overflow;    /* snapshot overflowed, subxids missing */
    uint32_t        nextXid;            /* xid from ShmemVariableCache->nextFullXid */
    uint32_t        oldestRunningXid;   /* *not* oldestXmin */
    uint32_t        latestCompletedXid; /* so we can set xmax */

    uint32_t        xids[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_running_xacts;

extern bool pg_parser_trans_rmgr_standby_pre(pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result, 
                            int32_t *pg_parser_errno);


#endif
