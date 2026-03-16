#ifndef _RIPPLE_BIGTXN_PERSIST_H
#define _RIPPLE_BIGTXN_PERSIST_H

typedef enum RIPPLE_BIGTXN_PERSISTNODE_STAT
{
    RIPPLE_BIGTXN_PERSISTNODE_STAT_NOP                 = 0x00,
    RIPPLE_BIGTXN_PERSISTNODE_STAT_INIT                ,
    RIPPLE_BIGTXN_PERSISTNODE_STAT_INPROCESS           ,
    RIPPLE_BIGTXN_PERSISTNODE_STAT_DONE                ,
    RIPPLE_BIGTXN_PERSISTNODE_STAT_ABANDON
} ripple_bigtxn_persistnode_stat;

typedef struct RIPPLE_BIGTXN_PERSISTNODE
{
    ripple_recpos           begin;                         /* 大事物开始点, fileid/fileoffset */
    ripple_recpos           end;                           /* 大事物结束点, fileid/fileoffset */
    FullTransactionId       xid;                           /* 大事物的事务号 */
    int                     stat;                          /* 大事物状态 */
} ripple_bigtxn_persistnode;

typedef struct RIPPLE_BIGTXN_PERSIST
{
    ripple_recpos       rewind;                             /* 最早未完成的大事物开始点 */
    uint32              count;                              /* 保存的大事物状态数量 */
    dlist*              dpersistnodes;                      /* 大事物状态节点ripple_bigtxn_persistnode */
} ripple_bigtxn_persist;

ripple_bigtxn_persist* ripple_bigtxn_persist_init(void);

ripple_bigtxn_persistnode* ripple_bigtxn_persist_node_init(void);

void ripple_bigtxn_persist_electionrewind(ripple_bigtxn_persist* persist, ripple_recpos* pos);

/* 筛选新的 rewind 节点 */
void ripple_bigtxn_persist_electionrewindbyxid(ripple_bigtxn_persist* persist, FullTransactionId xid);

void ripple_bigtxn_persist_removebyxid(ripple_bigtxn_persist* persist, FullTransactionId xid);

int ripple_bigtxn_integratepersist_delectbyxidcmp(void* vala, void* valb);

void ripple_bigtxn_integratepersist_cleannotdone(ripple_bigtxn_persist* persist);

void ripple_bigtxn_persist_free(ripple_bigtxn_persist* persist);

void ripple_bigtxn_persistnode_free(void* persistnode);

bool ripple_bigtxn_write_persist(ripple_bigtxn_persist *persist);

ripple_bigtxn_persist *ripple_bigtxn_read_persist(void);

void ripple_bigtxn_persist_set_state_by_xid(ripple_bigtxn_persist* persist, FullTransactionId xid, int state);

extern void ripple_bigtxn_persistnode_set_begin(ripple_bigtxn_persistnode *node, ripple_recpos *pos);
extern void ripple_bigtxn_persistnode_set_end(ripple_bigtxn_persistnode *node, ripple_recpos *pos);
extern void ripple_bigtxn_persistnode_set_xid(ripple_bigtxn_persistnode *node, FullTransactionId xid);
extern void ripple_bigtxn_persistnode_set_stat_init(ripple_bigtxn_persistnode *node);

#endif
