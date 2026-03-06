#ifndef _RIPPLE_TXN_H_
#define _RIPPLE_TXN_H_

typedef enum RIPPLE_TXN_FLAG 
{
    RIPPLE_TXN_FLAG_NORMAL                      = 0x00,
    RIPPLE_TXN_FLAG_TOAST                       = 0x01,
    RIPPLE_TXN_FLAG_DDL                         = 0x02,
    RIPPLE_TXN_FLAG_INHASH                      = 0x04,         /* 保证标记的唯一性       */
    RIPPLE_TXN_FLAG_BIGTXN                      = 0x08,         /* 大事务标识             */
    RIPPLE_TXN_FLAG_ONLINEREFRESH               = 0x10          /* onlinerefesh 标识      */
}ripple_txn_flag;


typedef enum RIPPLE_TXN_TYPE
{
    RIPPLE_TXN_TYPE_NORMAL                      = 0x00,
    RIPPLE_TXN_TYPE_METADATA                    ,               /* metadata */
    RIPPLE_TXN_TYPE_REFRESH                     ,               /* 存量事务 */
    RIPPLE_TXN_TYPE_BIGTXN_BEGIN                ,               /* 大事务开始 */
    RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT           ,               /* 大事务提交 */
    RIPPLE_TXN_TYPE_BIGTXN_END_ABORT            ,               /* 大事务回滚 */
    RIPPLE_TXN_TYPE_ONLINEREFRESH_BEGIN         ,               /* onlinerefresh开始 */
    RIPPLE_TXN_TYPE_ONLINEREFRESH_END           ,               /* onlinerefresh结束 */
    RIPPLE_TXN_TYPE_ONLINEREFRESH_INC_END       ,               /* onlinerefresh增量结束 */
    RIPPLE_TXN_TYPE_ONLINEREFRESH_ABANDON       ,               /* 要放弃的onlinerefresh */
    RIPPLE_TXN_TYPE_ONLINEREFRESH_DATASET       ,               /* onlinerefresh过滤数据集 */
    RIPPLE_TXN_TYPE_RESET                       ,               /* reset事务 */
    RIPPLE_TXN_TYPE_ABANDON                     ,               /* 要放弃的onlinerefresh */
    RIPPLE_TXN_TYPE_TIMELINE                    ,               /* timeline切换事务 */
    RIPPLE_TXN_TYPE_SHIFTFILE                                   /* 文件切换事务 */
}ripple_txn_type;


/*----------------onlinerefresh标识操作相关 begin-----------------*/
#define RIPPLE_TXN_SET_ONLINEREFRESHTXN(flag)                   (flag |= RIPPLE_TXN_FLAG_ONLINEREFRESH)
#define RIPPLE_TXN_UNSET_ONLINEREFRESHTXN(flag)                 (flag &= ~(RIPPLE_TXN_FLAG_ONLINEREFRESH))
#define RIPPLE_TXN_ISONLINEREFRESHTXN(flag)                     (flag & RIPPLE_TXN_FLAG_ONLINEREFRESH)

/*----------------onlinerefresh标识操作相关   end-----------------*/

/*----------------大事务标识操作相关 begin------------------------*/

#define RIPPLE_TXN_SET_BIGTXN(flag)                             (flag |= RIPPLE_TXN_FLAG_BIGTXN)
#define RIPPLE_TXN_UNSET_BIGTXN(flag)                           (flag &= ~(RIPPLE_TXN_FLAG_BIGTXN))
#define RIPPLE_TXN_ISBIGTXN(flag)                               (flag & RIPPLE_TXN_FLAG_BIGTXN)

/*----------------大事务标识操作相关   end------------------------*/

#define RIPPLE_TXN_SET_TRANS_TOAST(flag)                        (flag |= RIPPLE_TXN_FLAG_TOAST)
#define RIPPLE_TXN_UNSET_TRANS_TOAST(flag)                      (flag &= ~(RIPPLE_TXN_FLAG_TOAST))
#define RIPPLE_TXN_CHECK_TRANS_TOAST(flag)                      (flag & RIPPLE_TXN_FLAG_TOAST)

#define RIPPLE_TXN_SET_TRANS_DDL(flag)                          (flag |= RIPPLE_TXN_FLAG_DDL)
#define RIPPLE_TXN_UNSET_TRANS_DDL(flag)                        (flag &= ~(RIPPLE_TXN_FLAG_DDL))
#define RIPPLE_TXN_CHECK_TRANS_DDL(flag)                        (flag & RIPPLE_TXN_FLAG_DDL)

#define RIPPLE_TXN_SET_TRANS_INHASH(flag)                       (flag |= RIPPLE_TXN_FLAG_INHASH)
#define RIPPLE_TXN_CHECK_TRANS_INHASH(flag)                     (flag & RIPPLE_TXN_FLAG_INHASH)


#define RIPPLE_TXN_CHECK_COULD_SAVE(flag) ((RIPPLE_TXN_CHECK_TRANS_TOAST(flag)) || (RIPPLE_TXN_CHECK_TRANS_DDL(flag)))

typedef struct RIPPLE_TXN
{
    FullTransactionId       xid;                /* 事务号                   */
                                                /*
                                                 * 没有事务号那么说明事务内没有内容
                                                 * 当前用到的为:
                                                 *  在 parser 线程中解析出来不含事务号的 checkpoint
                                                 * 
                                                 */
    bool                    filter;             /* 
                                                 * 用于标识该事务是否需要过滤, true 过滤， false 不过滤
                                                 */
    bool                    commit;             /* 大事务中事务结束表示 */
    uint32                  curtlid;            /* 当前时间线id */
    uint16                  flag;               /* 用于标记该事务的特殊性, 是否处于行外存储或系统表处理流程中 */
    ripple_txn_type         type;               /* 事务类型             */
    int64                   endtimestamp;       /* 事务结束时间戳 */
    uint64                  segno;              /* 文件号, 读取或写入的 */
    uint64                  stmtsize;
    uint64                  debugno;
    ripple_recpos           start;
    ripple_recpos           end;
    ripple_recpos           redo;
    ripple_recpos           restart;
    ripple_recpos           confirm;
    HTAB*                   toast_hash;         /* toast 缓存                                   */
    List*                   sysdict;            /* 结构(ripple_txn_sysdict)                     */
    List*                   sysdictHis;         /* 结构(ripple_catalogdata)  */
    List*                   stmts;              /* 提交时将此语句写入到中转线程缓存, 结构内容为 ripple_txnstmt */
    HTAB*                   hsyncdataset;       /* 
                                                 * 为了应对如下场景:
                                                 *  1、begin;                           ---开启一个事务
                                                 *  2、create table sample(...)         ---满足 addtablepattern 条件
                                                 *  3、insert into sample values(...)   ---此时表在全局的 hsyncdataset 中不存在
                                                 *                                         所以需要 txn->hsyncdataset 解决此场景
                                                 *  4、commit
                                                 */
    HTAB*                   oidmap;
    struct RIPPLE_TXN*      prev;
    struct RIPPLE_TXN*      next;
    struct RIPPLE_TXN*      cachenext;
} ripple_txn;

typedef struct RIPPLE_TXN_DLIST
{
    ripple_txn* head;
    ripple_txn* tail;
} ripple_txn_dlist;

void ripple_txn_initset(ripple_txn *tx_entry, FullTransactionId xid, XLogRecPtr startlsn);

/* 生成一个没有 xid 的事务 */
ripple_txn* ripple_txn_init(FullTransactionId xid, XLogRecPtr startlsn, XLogRecPtr endlsn);

/* 事务复制 */
ripple_txn* ripple_txn_copy(ripple_txn* txn);

ripple_txn *ripple_txn_initbigtxn(FullTransactionId xid);

bool ripple_txn_addcommit(ripple_txn* txn);

ripple_txn *ripple_txn_initabandon(ripple_txn *txninhash);

/* 删除事务缓存 */
void ripple_txn_free(ripple_txn* txn);

void ripple_txn_freevoid(void* args);

#endif