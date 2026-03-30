#ifndef PG_PARSER_TRANS_RMGR_XACT_H
#define PG_PARSER_TRANS_RMGR_XACT_H

/*
 * The following flags, stored in xinfo, determine which information is
 * contained in commit/abort records.
 */
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_DBINFO       (1U << 0)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_SUBXACTS     (1U << 1)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_RELFILENODES (1U << 2)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_INVALS       (1U << 3)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_TWOPHASE     (1U << 4)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_ORIGIN       (1U << 5)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_AE_LOCKS     (1U << 6)
#define PG_PARSER_TRANS_RMGR_XACT_XINFO_HAS_GID          (1U << 7)
/* mask for filtering opcodes out of xl_info */
#define PG_PARSER_TRANS_TRANSREC_RMGR_XACT_OPMASK 0x70

/* does this record have a 'xinfo' field or not */
#define PG_PARSER_TRANS_TRANSREC_RMGR_XACT_HAS_INFO 0x80

typedef struct
{
    int8_t   id;        /* cache ID --- must be first */
    uint32_t dbId;      /* database ID, or 0 if a shared relation */
    uint32_t hashValue; /* hash value of key for this catcache */
} pg_parser_SharedInvalCatcacheMsg;

typedef struct
{
    int8_t   id;    /* type field --- must be first */
    uint32_t dbId;  /* database ID, or 0 if a shared catalog */
    uint32_t catId; /* ID of catalog whose contents are invalid */
} pg_parser_SharedInvalCatalogMsg;

typedef struct
{
    int8_t   id;    /* type field --- must be first */
    uint32_t dbId;  /* database ID, or 0 if a shared relation */
    uint32_t relId; /* relation ID, or 0 if whole relcache */
} pg_parser_SharedInvalRelcacheMsg;

typedef struct
{
    /* note: field layout chosen to pack into 16 bytes */
    int8_t                id;         /* type field --- must be first */
    int8_t                backend_hi; /* high bits of backend ID, if temprel */
    uint16_t              backend_lo; /* low bits of backend ID, if temprel */
    pg_parser_RelFileNode rnode;      /* spcNode, dbNode, relNode */
} pg_parser_SharedInvalSmgrMsg;

typedef struct
{
    int8_t   id;   /* type field --- must be first */
    uint32_t dbId; /* database ID, or 0 for shared catalogs */
} pg_parser_SharedInvalRelmapMsg;

typedef struct
{
    int8_t   id;    /* type field --- must be first */
    uint32_t dbId;  /* database ID, or 0 if a shared relation */
    uint32_t relId; /* relation ID */
} pg_parser_SharedInvalSnapshotMsg;

typedef union
{
    int8_t                           id; /* type field --- must be first */
    pg_parser_SharedInvalCatcacheMsg cc;
    pg_parser_SharedInvalCatalogMsg  cat;
    pg_parser_SharedInvalRelcacheMsg rc;
    pg_parser_SharedInvalSmgrMsg     sm;
    pg_parser_SharedInvalRelmapMsg   rm;
    pg_parser_SharedInvalSnapshotMsg sn;
} pg_parser_SharedInvalidationMessage;

typedef enum PG_PARSER_TRANS_RMGR_XACT_INFO
{
    PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT = 0x00,
    PG_PARSER_TRANS_TRANSREC_RMGR_XACT_PREPARE = 0x10,
    PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT = 0x20,
    PG_PARSER_TRANS_TRANSREC_RMGR_XACT_COMMIT_PREPARED = 0x30,
    PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ABORT_PREPARED = 0x40,
    PG_PARSER_TRANS_TRANSREC_RMGR_XACT_ASSIGNMENT = 0x50
} pg_parser_trans_rmgr_xact_info;

typedef struct pg_parser_xl_xact_commit
{
    int64_t xact_time; /* time of commit */
} pg_parser_xl_xact_commit;

typedef struct pg_parser_xl_xact_xinfo
{
    /*
     * Even though we right now only require 1 byte of space in xinfo we use
     * four so following records don't have to care about alignment. Commit
     * records can be large, so copying large portions isn't attractive.
     */
    uint32_t xinfo;
} pg_parser_xl_xact_xinfo;

typedef struct pg_parser_xl_xact_dbinfo
{
    uint32_t dbId; /* MyDatabaseId */
    uint32_t tsId; /* MyDatabaseTableSpace */
} pg_parser_xl_xact_dbinfo;

typedef struct pg_parser_xl_xact_invals
{
    int32_t                             nmsgs; /* number of shared inval msgs */
    pg_parser_SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_xact_invals;

typedef struct pg_parser_xl_xact_twophase
{
    uint32_t xid;
} pg_parser_xl_xact_twophase;

typedef struct pg_parser_xl_xact_origin
{
    uint64_t origin_lsn;
    int64_t  origin_timestamp;
} pg_parser_xl_xact_origin;

#define PG_PARSER_GIDSIZE 200

typedef struct pg_parser_xl_xact_parsed_commit
{
    int64_t                              xact_time;
    uint32_t                             xinfo;

    uint32_t                             dbId; /* MyDatabaseId */
    uint32_t                             tsId; /* MyDatabaseTableSpace */

    int32_t                              nsubxacts;
    uint32_t*                            subxacts;

    int32_t                              nrels;
    pg_parser_RelFileNode*               xnodes;

    int32_t                              nmsgs;
    pg_parser_SharedInvalidationMessage* msgs;

    uint32_t                             twophase_xid;                    /* only for 2PC */
    char                                 twophase_gid[PG_PARSER_GIDSIZE]; /* only for 2PC */
    int32_t                              nabortrels;                      /* only for 2PC */
    pg_parser_RelFileNode*               abortnodes;                      /* only for 2PC */

    uint64_t                             origin_lsn;
    int64_t                              origin_timestamp;
} pg_parser_xl_xact_parsed_commit;

typedef struct pg_parser_xl_xact_subxacts
{
    int32_t                 nsubxacts; /* number of subtransaction XIDs */
    pg_parser_TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_xact_subxacts;

typedef struct pg_parser_xl_xact_relfilenodes
{
    int32_t               nrels; /* number of subtransaction XIDs */
    pg_parser_RelFileNode xnodes[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_xact_relfilenodes;

typedef struct pg_parser_xl_xact_abort
{
    int64_t xact_time; /* time of abort */
} pg_parser_xl_xact_abort;

typedef struct pg_parser_xl_xact_parsed_abort
{
    int64_t                xact_time;
    uint32_t               xinfo;

    uint32_t               dbId; /* MyDatabaseId */
    uint32_t               tsId; /* MyDatabaseTableSpace */

    int32_t                nsubxacts;
    uint32_t*              subxacts;

    int32_t                nrels;
    pg_parser_RelFileNode* xnodes;

    uint32_t               twophase_xid;                    /* only for 2PC */
    char                   twophase_gid[PG_PARSER_GIDSIZE]; /* only for 2PC */

    uint64_t               origin_lsn;
    int64_t                origin_timestamp;
} pg_parser_xl_xact_parsed_abort;

typedef struct pg_parser_xl_xact_assignment
{
    uint32_t xtop;                        /* assigned XID's top-level XID */
    int32_t  nsubxacts;                   /* number of subtransaction XIDs */
    uint32_t xsub[FLEXIBLE_ARRAY_MEMBER]; /* assigned subxids */
} pg_parser_xl_xact_assignment;

#define MinSizeOfXactAbort                  sizeof(xl_xact_abort)

#define pg_parser_MinSizeOfXactCommit       (offsetof(pg_parser_xl_xact_commit, xact_time) + sizeof(int64_t))
#define pg_parser_MinSizeOfXactRelfilenodes offsetof(pg_parser_xl_xact_relfilenodes, xnodes)
#define pg_parser_MinSizeOfXactInvals       offsetof(pg_parser_xl_xact_invals, msgs)

extern bool pg_parser_trans_rmgr_xact_pre(pg_parser_XLogReaderState*    state,
                                          pg_parser_translog_pre_base** result,
                                          int32_t*                      pg_parser_errno);

#endif
