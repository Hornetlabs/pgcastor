#ifndef _REWIND_H
#define _REWIND_H

typedef struct DECODINGCONTEXT decodingcontext;

/* Status value in rewind structure */
typedef enum REWIND_STAT
{
    REWIND_INIT = 0x00,             /* Initialization status */
    REWIND_SEARCHCHECKPOINT = 0x01, /* Search checkpoint status */
    REWIND_REWINDING = 0x02,        /* Transition status */
    REWIND_EMITING = 0x03,          /* Search emit point after finding checkpoint */
    REWIND_EMITED = 0x04            /* Set after finding emit point */
} rewind_stat;

typedef struct REWIND_STRATEGY
{
    TransactionId xmin;
    TransactionId xmax;
    HTAB*         xips;
} rewind_strategy;

/* rewind structure */
typedef struct REWIND
{
    int             stat;       /* Status */
    TransactionId   currentxid; /*xmax*/
    XLogRecPtr      currentlsn; /* lsn of latest point */
    XLogRecPtr      redolsn;    /* redolsn set after finding checkpoint */
    PGconn*         conn;       /* Save connection information */
    rewind_strategy strategy;   /* Backup of snapshot */
} rewind_info;

extern bool rewind_fastrewind(decodingcontext* decodingctx);
extern bool rewind_fastrewind_emit(decodingcontext* decodingctx);
extern void rewind_strategy_setfastrewind(snapshot* snapshot, decodingcontext* decoingctx);

extern void rewind_stat_setsearchcheckpoint(rewind_info* rewind);
extern void rewind_stat_setrewinding(rewind_info* rewind);
extern void rewind_stat_setemiting(rewind_info* rewind);
extern void rewind_stat_setemited(rewind_info* rewind);

extern bool rewind_check_stat_allow_get_entry(rewind_info* rewind);

#endif
