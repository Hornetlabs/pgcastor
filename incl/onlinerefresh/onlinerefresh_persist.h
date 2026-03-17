#ifndef _ONLINEREFRESH_PERSIST_H_
#define _ONLINEREFRESH_PERSIST_H_

typedef enum onlinerefresh_persistnode_stat
{
    ONLINEREFRESH_PERSISTNODE_STAT_NOP           = 0x00,
    ONLINEREFRESH_PERSISTNODE_STAT_INIT          ,
    ONLINEREFRESH_PERSISTNODE_STAT_INPROCESS     ,
    ONLINEREFRESH_PERSISTNODE_STAT_DONE          ,
    ONLINEREFRESH_PERSISTNODE_STAT_ABANDON       
}onlinerefresh_persistnode_stat;

typedef struct ONLINEREFRESH_PERSISTNODE
{
    bool                                        increment;
    onlinerefresh_persistnode_stat       stat;
    FullTransactionId                           txid;           /* 生成过滤集 */
    recpos                               begin;
    recpos                               end;
    uuid_t                               uuid;
    refresh_table_syncstats*             refreshtbs;
}onlinerefresh_persistnode;

typedef struct ONLINEREFRESH_PERSIST
{
    recpos               rewind;
    dlist*                      dpersistnodes;
}onlinerefresh_persist;

/* 初始化onlinerefresh persistnode */
extern onlinerefresh_persistnode* onlinerefresh_persistnode_init(void);

/* 设置 persistnode 状态 */
extern void onlinerefresh_persistnode_statset(onlinerefresh_persistnode* persistnode, int stat);

/* 设置 persistnode txid */
extern void onlinerefresh_persistnode_txidset(onlinerefresh_persistnode* persistnode, FullTransactionId txid);

/* 设置persistnode uuid  */
extern void onlinerefresh_persistnode_uuidset(onlinerefresh_persistnode* persistnode, uuid_t* uuid);

/* 设置 persistnode 开始位置  */
extern void onlinerefresh_persistnode_beginset(onlinerefresh_persistnode* persistnode, recpos pos);

/* 设置 persistnode 结束位置  */
extern void onlinerefresh_persistnode_endset(onlinerefresh_persistnode* persistnode, recpos pos);

/* 设置 persistnode 增量标识 */
extern void onlinerefresh_persistnode_incrementset(onlinerefresh_persistnode* persistnode, bool incrment);

/* 初始化onlinerefresh persist */
extern onlinerefresh_persist* onlinerefresh_persist_init(void);

/* 删除persistnode时uuid比较 */
extern int onlinerefresh_persist_delectbyuuidcmp(void* vala, void* valb);

/* 根据uuid设置node状态 */
extern void onlinerefresh_persist_statesetbyuuid(onlinerefresh_persist* persist, uuid_t* uuid, int state);

/* 根据uuid删除node中的存量表 */
extern void onlinerefresh_persist_removerefreshtbsbyuuid(onlinerefresh_persist* persist, uuid_t* uuid);

/* persist落盘 */
extern bool onlinerefresh_persist_write(onlinerefresh_persist* persist);

/* persist加载 */
extern onlinerefresh_persist *onlinerefresh_persist_read(void);

/* 根据uuid删除node */
extern void onlinerefresh_persist_removebyuuid(onlinerefresh_persist* persist, uuid_t* uuid);

/* 计算persist的rewind点 */
extern void onlinerefresh_persist_electionrewindbyuuid(onlinerefresh_persist* persist, uuid_t* uuid);

/* 清理persistnode内存 */
extern void onlinerefresh_persistnode_free(void* privdata);

/* 清理persist内存 */
extern void onlinerefresh_persist_free(onlinerefresh_persist* persist);

#endif
