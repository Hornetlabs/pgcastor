#ifndef XK_PG_PARSER_TRANS_RMGR_XACT_H
#define XK_PG_PARSER_TRANS_RMGR_XACT_H

/*
 * The following flags, stored in xinfo, determine which information is
 * contained in commit/abort records.
 */
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_DBINFO              (1U << 0)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_SUBXACTS            (1U << 1)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_RELFILENODES        (1U << 2)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_INVALS              (1U << 3)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_TWOPHASE            (1U << 4)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_ORIGIN              (1U << 5)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_AE_LOCKS            (1U << 6)
#define XK_PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_GID                 (1U << 7)
/* mask for filtering opcodes out of xl_info */
#define XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_OPMASK            0x70

/* does this record have a 'xinfo' field or not */
#define XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_HAS_INFO          0x80

typedef struct
{
    int8_t              id;               /* cache ID --- must be first */
    uint32_t            dbId;             /* database ID, or 0 if a shared relation */
    uint32_t            hashValue;        /* hash value of key for this catcache */
} xk_pg_parser_SharedInvalCatcacheMsg;

typedef struct
{
    int8_t              id;               /* type field --- must be first */
    uint32_t            dbId;             /* database ID, or 0 if a shared catalog */
    uint32_t            catId;            /* ID of catalog whose contents are invalid */
} xk_pg_parser_SharedInvalCatalogMsg;

typedef struct
{
    int8_t              id;               /* type field --- must be first */
    uint32_t            dbId;             /* database ID, or 0 if a shared relation */
    uint32_t            relId;            /* relation ID, or 0 if whole relcache */
} xk_pg_parser_SharedInvalRelcacheMsg;

typedef struct
{
    /* note: field layout chosen to pack into 16 bytes */
    int8_t              id;               /* type field --- must be first */
    int8_t              backend_hi;       /* high bits of backend ID, if temprel */
    uint16_t            backend_lo;       /* low bits of backend ID, if temprel */
    xk_pg_parser_RelFileNode rnode;       /* spcNode, dbNode, relNode */
} xk_pg_parser_SharedInvalSmgrMsg;

typedef struct
{
    int8_t              id;               /* type field --- must be first */
    uint32_t            dbId;             /* database ID, or 0 for shared catalogs */
} xk_pg_parser_SharedInvalRelmapMsg;

typedef struct
{
    int8_t              id;               /* type field --- must be first */
    uint32_t            dbId;             /* database ID, or 0 if a shared relation */
    uint32_t            relId;            /* relation ID */
} xk_pg_parser_SharedInvalSnapshotMsg;

typedef union
{
    int8_t        id;                /* type field --- must be first */
    xk_pg_parser_SharedInvalCatcacheMsg cc;
    xk_pg_parser_SharedInvalCatalogMsg cat;
    xk_pg_parser_SharedInvalRelcacheMsg rc;
    xk_pg_parser_SharedInvalSmgrMsg sm;
    xk_pg_parser_SharedInvalRelmapMsg rm;
    xk_pg_parser_SharedInvalSnapshotMsg sn;
} xk_pg_parser_SharedInvalidationMessage;

typedef enum XK_PG_PARSER_TRANS_RMGR_XACT_INFO
{
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT = 0x00,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_PREPARE = 0x10,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT = 0x20,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT_PREPARED = 0x30,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT_PREPARED = 0x40,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ASSIGNMENT = 0x50
} xk_pg_parser_trans_rmgr_xact_info;

typedef struct xk_pg_parser_xl_xact_commit
{
    int64_t xact_time;        /* time of commit */
} xk_pg_parser_xl_xact_commit;

typedef struct xk_pg_parser_xl_xact_xinfo
{
    /*
     * Even though we right now only require 1 byte of space in xinfo we use
     * four so following records don't have to care about alignment. Commit
     * records can be large, so copying large portions isn't attractive.
     */
    uint32_t    xinfo;
} xk_pg_parser_xl_xact_xinfo;

typedef struct xk_pg_parser_xl_xact_dbinfo
{
    uint32_t     dbId;   /* MyDatabaseId */
    uint32_t     tsId;   /* MyDatabaseTableSpace */
} xk_pg_parser_xl_xact_dbinfo;

typedef struct xk_pg_parser_xl_xact_invals
{
    int32_t         nmsgs;            /* number of shared inval msgs */
    xk_pg_parser_SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xk_pg_parser_xl_xact_invals;

typedef struct xk_pg_parser_xl_xact_twophase
{
    uint32_t xid;
} xk_pg_parser_xl_xact_twophase;

typedef struct xk_pg_parser_xl_xact_origin
{
    uint64_t    origin_lsn;
    int64_t     origin_timestamp;
} xk_pg_parser_xl_xact_origin;

#define XK_PG_PARSER_GIDSIZE 200

typedef struct xk_pg_parser_xl_xact_parsed_commit
{
    int64_t         xact_time;
    uint32_t        xinfo;

    uint32_t        dbId;            /* MyDatabaseId */
    uint32_t        tsId;            /* MyDatabaseTableSpace */

    int32_t         nsubxacts;
    uint32_t       *subxacts;

    int32_t         nrels;
    xk_pg_parser_RelFileNode *xnodes;

    int32_t         nmsgs;
    xk_pg_parser_SharedInvalidationMessage *msgs;

    uint32_t        twophase_xid; /* only for 2PC */
    char            twophase_gid[XK_PG_PARSER_GIDSIZE];    /* only for 2PC */
    int32_t         nabortrels;        /* only for 2PC */
    xk_pg_parser_RelFileNode    *abortnodes;    /* only for 2PC */

    uint64_t        origin_lsn;
    int64_t         origin_timestamp;
} xk_pg_parser_xl_xact_parsed_commit;

typedef struct xk_pg_parser_xl_xact_subxacts
{
    int32_t           nsubxacts;        /* number of subtransaction XIDs */
    xk_pg_parser_TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER];
} xk_pg_parser_xl_xact_subxacts;

typedef struct xk_pg_parser_xl_xact_relfilenodes
{
    int32_t            nrels;            /* number of subtransaction XIDs */
    xk_pg_parser_RelFileNode xnodes[FLEXIBLE_ARRAY_MEMBER];
} xk_pg_parser_xl_xact_relfilenodes;

typedef struct xk_pg_parser_xl_xact_abort
{
    int64_t xact_time;        /* time of abort */
} xk_pg_parser_xl_xact_abort;

typedef struct xk_pg_parser_xl_xact_parsed_abort
{
    int64_t         xact_time;
    uint32_t        xinfo;

    uint32_t        dbId;            /* MyDatabaseId */
    uint32_t        tsId;            /* MyDatabaseTableSpace */

    int32_t         nsubxacts;
    uint32_t       *subxacts;

    int32_t         nrels;
    xk_pg_parser_RelFileNode *xnodes;

    uint32_t        twophase_xid; /* only for 2PC */
    char            twophase_gid[XK_PG_PARSER_GIDSIZE];    /* only for 2PC */

    uint64_t        origin_lsn;
    int64_t         origin_timestamp;
} xk_pg_parser_xl_xact_parsed_abort;

typedef struct xk_pg_parser_xl_xact_assignment
{
    uint32_t xtop;         /* assigned XID's top-level XID */
    int32_t  nsubxacts;    /* number of subtransaction XIDs */
    uint32_t xsub[FLEXIBLE_ARRAY_MEMBER];	/* assigned subxids */
} xk_pg_parser_xl_xact_assignment;

#define MinSizeOfXactAbort sizeof(xl_xact_abort)

#define xk_pg_parser_MinSizeOfXactCommit (offsetof(xk_pg_parser_xl_xact_commit, xact_time) + sizeof(int64_t))
#define xk_pg_parser_MinSizeOfXactRelfilenodes offsetof(xk_pg_parser_xl_xact_relfilenodes, xnodes)
#define xk_pg_parser_MinSizeOfXactInvals offsetof(xk_pg_parser_xl_xact_invals, msgs)

extern bool xk_pg_parser_trans_rmgr_xact_pre(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result, 
                            int32_t *xk_pg_parser_errno);


#endif
