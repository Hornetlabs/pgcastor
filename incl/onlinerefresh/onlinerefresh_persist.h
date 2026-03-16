#ifndef _RIPPLE_ONLINEREFRESH_PERSIST_H_
#define _RIPPLE_ONLINEREFRESH_PERSIST_H_

typedef enum ripple_onlinerefresh_persistnode_stat
{
    RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_NOP           = 0x00,
    RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_INIT          ,
    RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_INPROCESS     ,
    RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_DONE          ,
    RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_ABANDON       
}ripple_onlinerefresh_persistnode_stat;

typedef struct RIPPLE_ONLINEREFRESH_PERSISTNODE
{
    bool                                        increment;
    ripple_onlinerefresh_persistnode_stat       stat;
    FullTransactionId                           txid;           /* 生成过滤集 */
    ripple_recpos                               begin;
    ripple_recpos                               end;
    ripple_uuid_t                               uuid;
    ripple_refresh_table_syncstats*             refreshtbs;
}ripple_onlinerefresh_persistnode;

typedef struct RIPPLE_ONLINEREFRESH_PERSIST
{
    ripple_recpos               rewind;
    dlist*                      dpersistnodes;
}ripple_onlinerefresh_persist;

/* 初始化onlinerefresh persistnode */
extern ripple_onlinerefresh_persistnode* ripple_onlinerefresh_persistnode_init(void);

/* 设置 persistnode 状态 */
extern void ripple_onlinerefresh_persistnode_statset(ripple_onlinerefresh_persistnode* persistnode, int stat);

/* 设置 persistnode txid */
extern void ripple_onlinerefresh_persistnode_txidset(ripple_onlinerefresh_persistnode* persistnode, FullTransactionId txid);

/* 设置persistnode uuid  */
extern void ripple_onlinerefresh_persistnode_uuidset(ripple_onlinerefresh_persistnode* persistnode, ripple_uuid_t* uuid);

/* 设置 persistnode 开始位置  */
extern void ripple_onlinerefresh_persistnode_beginset(ripple_onlinerefresh_persistnode* persistnode, ripple_recpos pos);

/* 设置 persistnode 结束位置  */
extern void ripple_onlinerefresh_persistnode_endset(ripple_onlinerefresh_persistnode* persistnode, ripple_recpos pos);

/* 设置 persistnode 增量标识 */
extern void ripple_onlinerefresh_persistnode_incrementset(ripple_onlinerefresh_persistnode* persistnode, bool incrment);

/* 初始化onlinerefresh persist */
extern ripple_onlinerefresh_persist* ripple_onlinerefresh_persist_init(void);

/* 删除persistnode时uuid比较 */
extern int ripple_onlinerefresh_persist_delectbyuuidcmp(void* vala, void* valb);

/* 根据uuid设置node状态 */
extern void ripple_onlinerefresh_persist_statesetbyuuid(ripple_onlinerefresh_persist* persist, ripple_uuid_t* uuid, int state);

/* 根据uuid删除node中的存量表 */
extern void ripple_onlinerefresh_persist_removerefreshtbsbyuuid(ripple_onlinerefresh_persist* persist, ripple_uuid_t* uuid);

/* persist落盘 */
extern bool ripple_onlinerefresh_persist_write(ripple_onlinerefresh_persist* persist);

/* persist加载 */
extern ripple_onlinerefresh_persist *ripple_onlinerefresh_persist_read(void);

/* 根据uuid删除node */
extern void ripple_onlinerefresh_persist_removebyuuid(ripple_onlinerefresh_persist* persist, ripple_uuid_t* uuid);

/* 计算persist的rewind点 */
extern void ripple_onlinerefresh_persist_electionrewindbyuuid(ripple_onlinerefresh_persist* persist, ripple_uuid_t* uuid);

/* 清理persistnode内存 */
extern void ripple_onlinerefresh_persistnode_free(void* privdata);

/* 清理persist内存 */
extern void ripple_onlinerefresh_persist_free(ripple_onlinerefresh_persist* persist);

#endif
