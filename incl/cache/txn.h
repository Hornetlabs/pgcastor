#ifndef _TXN_H_
#define _TXN_H_

typedef enum TXN_FLAG
{
    TXN_FLAG_NORMAL = 0x00,
    TXN_FLAG_TOAST = 0x01,
    TXN_FLAG_DDL = 0x02,
    TXN_FLAG_INHASH = 0x04,       /* ensure flag uniqueness       */
    TXN_FLAG_BIGTXN = 0x08,       /* big transaction flag         */
    TXN_FLAG_ONLINEREFRESH = 0x10 /* online refresh flag          */
} txn_flag;

typedef enum TXN_TYPE
{
    TXN_TYPE_NORMAL = 0x00,
    TXN_TYPE_METADATA,              /* metadata */
    TXN_TYPE_REFRESH,               /* refresh (full load) transaction */
    TXN_TYPE_BIGTXN_BEGIN,          /* big transaction begin */
    TXN_TYPE_BIGTXN_END_COMMIT,     /* big transaction commit */
    TXN_TYPE_BIGTXN_END_ABORT,      /* big transaction abort */
    TXN_TYPE_ONLINEREFRESH_BEGIN,   /* online refresh begin */
    TXN_TYPE_ONLINEREFRESH_END,     /* online refresh end */
    TXN_TYPE_ONLINEREFRESH_INC_END, /* online refresh incremental end */
    TXN_TYPE_ONLINEREFRESH_ABANDON, /* abandoned online refresh */
    TXN_TYPE_ONLINEREFRESH_DATASET, /* online refresh filter dataset */
    TXN_TYPE_RESET,                 /* reset transaction */
    TXN_TYPE_ABANDON,               /* abandoned transaction */
    TXN_TYPE_TIMELINE,              /* timeline switch transaction */
    TXN_TYPE_SHIFTFILE              /* file shift transaction */
} txn_type;

/*----------------online refresh flag operations begin-----------------*/
#define TXN_SET_ONLINEREFRESHTXN(flag)   (flag |= TXN_FLAG_ONLINEREFRESH)
#define TXN_UNSET_ONLINEREFRESHTXN(flag) (flag &= ~(TXN_FLAG_ONLINEREFRESH))
#define TXN_ISONLINEREFRESHTXN(flag)     (flag & TXN_FLAG_ONLINEREFRESH)

/*----------------online refresh flag operations   end-----------------*/

/*----------------big transaction flag operations begin------------------------*/

#define TXN_SET_BIGTXN(flag)   (flag |= TXN_FLAG_BIGTXN)
#define TXN_UNSET_BIGTXN(flag) (flag &= ~(TXN_FLAG_BIGTXN))
#define TXN_ISBIGTXN(flag)     (flag & TXN_FLAG_BIGTXN)

/*----------------big transaction flag operations   end------------------------*/

#define TXN_SET_TRANS_TOAST(flag)    (flag |= TXN_FLAG_TOAST)
#define TXN_UNSET_TRANS_TOAST(flag)  (flag &= ~(TXN_FLAG_TOAST))
#define TXN_CHECK_TRANS_TOAST(flag)  (flag & TXN_FLAG_TOAST)

#define TXN_SET_TRANS_DDL(flag)      (flag |= TXN_FLAG_DDL)
#define TXN_UNSET_TRANS_DDL(flag)    (flag &= ~(TXN_FLAG_DDL))
#define TXN_CHECK_TRANS_DDL(flag)    (flag & TXN_FLAG_DDL)

#define TXN_SET_TRANS_INHASH(flag)   (flag |= TXN_FLAG_INHASH)
#define TXN_CHECK_TRANS_INHASH(flag) (flag & TXN_FLAG_INHASH)

#define TXN_CHECK_COULD_SAVE(flag)   ((TXN_CHECK_TRANS_TOAST(flag)) || (TXN_CHECK_TRANS_DDL(flag)))

typedef struct TXN
{
    FullTransactionId xid;          /* transaction ID                   */
                                    /*
                                     * if no xid, the transaction has no content
                                     * currently used for:
                                     *  checkpoint parsed in parser thread that has no xid
                                     *
                                     */
    bool              filter;       /*
                                     * flag to indicate whether this transaction should be filtered
                                     * true = filter, false = do not filter
                                     */
    bool              commit;       /* transaction end indicator in big transactions */
    uint32            curtlid;      /* current timeline id */
    uint16            flag;         /* flags for transaction special handling (toast/ddl/bigtxn/onlinerefresh) */
    txn_type          type;         /* transaction type             */
    int64             endtimestamp; /* transaction end timestamp */
    uint64            segno;        /* file number for read/write */
    uint64            stmtsize;
    uint64            debugno;
    recpos            start;
    recpos            end;
    recpos            redo;
    recpos            restart;
    recpos            confirm;
    HTAB*             toast_hash;   /* toast data cache                                   */
    List*             sysdict;      /* system dictionary (struct: txn_sysdict)                     */
    List*             sysdictHis;   /* historical system dictionary (struct: catalogdata)  */
    List*             stmts;        /* statements written to relay thread cache on commit (struct: txnstmt) */
    HTAB*             hsyncdataset; /*
                                     * handles the following scenario:
                                     *  1. begin;                           ---start a transaction
                                     *  2. create table sample(...)         ---matches addtablepattern
                                     *  3. insert into sample values(...)   ---table does not exist in global
                                     * hsyncdataset    so we need txn->hsyncdataset to handle this scenario
                                     *  4. commit
                                     */
    HTAB*             oidmap;
    struct TXN*       prev;
    struct TXN*       next;
    struct TXN*       cachenext;
} txn;

typedef struct TXN_DLIST
{
    txn* head;
    txn* tail;
} txn_dlist;

void txn_initset(txn* tx_entry, FullTransactionId xid, XLogRecPtr startlsn);

/* create a transaction without xid */
txn* txn_init(FullTransactionId xid, XLogRecPtr startlsn, XLogRecPtr endlsn);

/* copy a transaction */
txn* txn_copy(txn* txn);

txn* txn_initbigtxn(FullTransactionId xid);

bool txn_addcommit(txn* txn);

txn* txn_initabandon(txn* txninhash);

/* free transaction cache */
void txn_free(txn* txn);

void txn_freevoid(void* args);

#endif