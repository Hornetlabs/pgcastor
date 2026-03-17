#ifndef _BIGTXN_PERSIST_H
#define _BIGTXN_PERSIST_H

typedef enum BIGTXN_PERSISTNODE_STAT
{
    BIGTXN_PERSISTNODE_STAT_NOP                 = 0x00,
    BIGTXN_PERSISTNODE_STAT_INIT                ,
    BIGTXN_PERSISTNODE_STAT_INPROCESS           ,
    BIGTXN_PERSISTNODE_STAT_DONE                ,
    BIGTXN_PERSISTNODE_STAT_ABANDON
} bigtxn_persistnode_stat;

typedef struct BIGTXN_PERSISTNODE
{
    recpos           begin;                         /* 大事物开始点, fileid/fileoffset */
    recpos           end;                           /* 大事物结束点, fileid/fileoffset */
    FullTransactionId       xid;                           /* 大事物的事务号 */
    int                     stat;                          /* 大事物状态 */
} bigtxn_persistnode;

typedef struct BIGTXN_PERSIST
{
    recpos       rewind;                             /* 最早未完成的大事物开始点 */
    uint32              count;                              /* 保存的大事物状态数量 */
    dlist*              dpersistnodes;                      /* 大事物状态节点bigtxn_persistnode */
} bigtxn_persist;

bigtxn_persist* bigtxn_persist_init(void);

bigtxn_persistnode* bigtxn_persist_node_init(void);

void bigtxn_persist_electionrewind(bigtxn_persist* persist, recpos* pos);

/* 筛选新的 rewind 节点 */
void bigtxn_persist_electionrewindbyxid(bigtxn_persist* persist, FullTransactionId xid);

void bigtxn_persist_removebyxid(bigtxn_persist* persist, FullTransactionId xid);

int bigtxn_integratepersist_delectbyxidcmp(void* vala, void* valb);

void bigtxn_integratepersist_cleannotdone(bigtxn_persist* persist);

void bigtxn_persist_free(bigtxn_persist* persist);

void bigtxn_persistnode_free(void* persistnode);

bool bigtxn_write_persist(bigtxn_persist *persist);

bigtxn_persist *bigtxn_read_persist(void);

void bigtxn_persist_set_state_by_xid(bigtxn_persist* persist, FullTransactionId xid, int state);

extern void bigtxn_persistnode_set_begin(bigtxn_persistnode *node, recpos *pos);
extern void bigtxn_persistnode_set_end(bigtxn_persistnode *node, recpos *pos);
extern void bigtxn_persistnode_set_xid(bigtxn_persistnode *node, FullTransactionId xid);
extern void bigtxn_persistnode_set_stat_init(bigtxn_persistnode *node);

#endif
