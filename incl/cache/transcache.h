#ifndef _TRANSCACHE_H
#define _TRANSCACHE_H

/* relfilnode hash 表的 key 和 entry */
typedef struct RELFILENODE2OID
{
    RelFileNode             relfilenode;
    Oid                     oid;
} relfilenode2oid;

typedef struct TXN_SYSDICT
{
    pg_parser_translog_tbcol_values* colvalues;
    void *convert_colvalues;
} txn_sysdict;

typedef struct CHECKPOINTNODE
{
    TransactionId           xid;
    XLogRecPtr              redolsn;
    struct CHECKPOINTNODE* next;
    struct CHECKPOINTNODE* prev;
} checkpointnode;

typedef struct CHECKPOINTS
{
    checkpointnode* head;
    checkpointnode* tail;
} checkpoints;


typedef struct TRANSCACHE
{
    /* stmt缓存使用的大小 */
    uint64                      totalsize;

    /* capture_buffer      事务缓存占用的空间 */
    uint64                      capture_buffer;

    /* 
     * 维护逻辑
     *  chkpts 为链表结构，其中 head 指向首个 checkpoint， tail 指向最后一个 checkpoint
     *      当 parser 线程解析到 checkpoint 时，增加一个节点到 chkpts 中。
     *      在 序列化 线程中当 restartlsn 更新后，那么计算是否应该删除 chkpts 中的某些节点，
     *          当需要删除节点时，那么同时清理 fpwcache 中的数据
     */
    checkpoints*         chkpts;
    txn_dlist*           transdlist;
    HTAB*                       by_txns;            /* 待提交事务缓存, key: transactionid, entry: txn              */

    /* fpw 缓存 */
    HTAB*                       by_fpwtuples;       /* 全页写tuple保存的哈希表 */
    List*                       fpwtupleslist;      /* 含有哈希key和lsn的链表 */

    cache_sysdicts*      sysdicts;           /* 系统字典             */

    /*
     * 同步数据集
     */
    /* 同步表规则 */
    List*                       tableincludes;

    /* 同步排除表规则 */
    List*                       tableexcludes;

    /* 新增表规则 */
    List*                       addtablepattern;

    /*
     * 同步数据集,                           relid
     *  根据 tableincludes/tableexcludes 和 运行时 tableaddpattern 生成的同步数据集合
     *  程序刚启动时由 tableincludes 和 tableexcludes 生成初始同步数据集
     *  在运行过程中, 当 新创建 表时, 新创建的表满足 tableaddpattern 规则时 则将新创建的表加入到 同步数据集中
     */
    HTAB*                       hsyncdataset;

    /*
    * 导致事务设置为 filter 的数据集合
    *    Key:Oid
    *    Value: filter_oid2datasetnode
    */
    HTAB*                       htxnfilterdataset;
} transcache;

typedef transcache       txnscontext;

extern void transcache_dlist_remove(void* in_ctx,
                                    txn* txn,
                                    bool* brestart,
                                    XLogRecPtr* restartlsn,
                                    bool* bconfirm,
                                    XLogRecPtr* confirmlsn,
                                    bool bset);

extern txn *transcache_getTXNByXid(void* in_ctx, uint64_t xid);

extern txn *transcache_getTXNByXidFind(transcache* transcache, uint64_t xid);

extern void transcache_removeTXNByXid(transcache * in_transcache, uint64_t xid);

bool transcache_refreshlsn(void* in_ctx, txn* txn);

bool transcache_deletetxn(void* in_ctx, txn* txn);

extern void transcache_sysdict2his(txn* txn);

extern void transcache_sysdict_free(txn* txn);

extern void transcache_free(transcache* transcache);

/* 获取数据库的标识 */
extern Oid transcache_getdboid(void* in_transcache);

/* 获取数据库的名称 */
extern char* transcache_getdbname(Oid dbid, void* in_transcache);

/* 获取namespace数据 */
extern void* transcache_getnamespace(Oid oid, void* in_transcache);

/* 获取class数据 */
extern void* transcache_getclass(Oid oid, void* in_transcache);

/* 获取attribute数据 */
extern void* transcache_getattributes(Oid oid, void* in_transcache);

extern void* transcache_getindex(Oid oid, void* in_transcache);

/* 获取type数据 */
extern void* transcache_gettype(Oid oid, void* in_transcache);

#endif
