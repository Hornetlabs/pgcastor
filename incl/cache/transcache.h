#ifndef _RIPPLE_TRANSCACHE_H
#define _RIPPLE_TRANSCACHE_H

/* relfilnode hash 表的 key 和 entry */
typedef struct RIPPLE_RELFILENODE2OID
{
    RelFileNode             relfilenode;
    Oid                     oid;
} ripple_relfilenode2oid;

typedef struct RIPPLE_TXN_SYSDICT
{
    xk_pg_parser_translog_tbcol_values* colvalues;
    void *convert_colvalues;
} ripple_txn_sysdict;

typedef struct RIPPLE_CHECKPOINTNODE
{
    TransactionId           xid;
    XLogRecPtr              redolsn;
    struct RIPPLE_CHECKPOINTNODE* next;
    struct RIPPLE_CHECKPOINTNODE* prev;
} ripple_checkpointnode;

typedef struct RIPPLE_CHECKPOINTS
{
    ripple_checkpointnode* head;
    ripple_checkpointnode* tail;
} ripple_checkpoints;


typedef struct RIPPLE_TRANSCACHE
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
    ripple_checkpoints*         chkpts;
    ripple_txn_dlist*           transdlist;
    HTAB*                       by_txns;            /* 待提交事务缓存, key: transactionid, entry: ripple_txn              */

    /* fpw 缓存 */
    HTAB*                       by_fpwtuples;       /* 全页写tuple保存的哈希表 */
    List*                       fpwtupleslist;      /* 含有哈希key和lsn的链表 */

    ripple_cache_sysdicts*      sysdicts;           /* 系统字典             */

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
    *    Value: ripple_filter_oid2datasetnode
    */
    HTAB*                       htxnfilterdataset;
} ripple_transcache;

typedef ripple_transcache       ripple_txnscontext;

extern void ripple_transcache_dlist_remove(void* in_ctx,
                                    ripple_txn* txn,
                                    bool* brestart,
                                    XLogRecPtr* restartlsn,
                                    bool* bconfirm,
                                    XLogRecPtr* confirmlsn,
                                    bool bset);

extern ripple_txn *ripple_transcache_getTXNByXid(void* in_ctx, uint64_t xid);

extern ripple_txn *ripple_transcache_getTXNByXidFind(ripple_transcache* transcache, uint64_t xid);

extern void ripple_transcache_removeTXNByXid(ripple_transcache * in_transcache, uint64_t xid);

bool ripple_transcache_refreshlsn(void* in_ctx, ripple_txn* txn);

bool ripple_transcache_deletetxn(void* in_ctx, ripple_txn* txn);

extern void ripple_transcache_sysdict2his(ripple_txn* txn);

extern void ripple_transcache_sysdict_free(ripple_txn* txn);

extern void ripple_transcache_free(ripple_transcache* transcache);

/* 获取数据库的标识 */
extern Oid ripple_transcache_getdboid(void* in_transcache);

/* 获取数据库的名称 */
extern char* ripple_transcache_getdbname(Oid dbid, void* in_transcache);

/* 获取namespace数据 */
extern void* ripple_transcache_getnamespace(Oid oid, void* in_transcache);

/* 获取class数据 */
extern void* ripple_transcache_getclass(Oid oid, void* in_transcache);

/* 获取attribute数据 */
extern void* ripple_transcache_getattributes(Oid oid, void* in_transcache);

extern void* ripple_transcache_getindex(Oid oid, void* in_transcache);

/* 获取type数据 */
extern void* ripple_transcache_gettype(Oid oid, void* in_transcache);

#endif
