#ifndef _ONLINEREFRESH_PERSIST_H_
#define _ONLINEREFRESH_PERSIST_H_

typedef enum onlinerefresh_persistnode_stat
{
    ONLINEREFRESH_PERSISTNODE_STAT_NOP = 0x00,
    ONLINEREFRESH_PERSISTNODE_STAT_INIT,
    ONLINEREFRESH_PERSISTNODE_STAT_INPROCESS,
    ONLINEREFRESH_PERSISTNODE_STAT_DONE,
    ONLINEREFRESH_PERSISTNODE_STAT_ABANDON
} onlinerefresh_persistnode_stat;

typedef struct ONLINEREFRESH_PERSISTNODE
{
    bool                           increment;
    onlinerefresh_persistnode_stat stat;
    FullTransactionId              txid; /* Generate filter set */
    recpos                         begin;
    recpos                         end;
    uuid_t                         uuid;
    refresh_table_syncstats*       refreshtbs;
} onlinerefresh_persistnode;

typedef struct ONLINEREFRESH_PERSIST
{
    recpos rewind;
    dlist* dpersistnodes;
} onlinerefresh_persist;

/* Initialize onlinerefresh persistnode */
extern onlinerefresh_persistnode* onlinerefresh_persistnode_init(void);

/* Set persistnode status */
extern void onlinerefresh_persistnode_statset(onlinerefresh_persistnode* persistnode, int stat);

/* Set persistnode txid */
extern void onlinerefresh_persistnode_txidset(onlinerefresh_persistnode* persistnode,
                                              FullTransactionId          txid);

/* Set persistnode uuid  */
extern void onlinerefresh_persistnode_uuidset(onlinerefresh_persistnode* persistnode, uuid_t* uuid);

/* Set persistnode start position  */
extern void onlinerefresh_persistnode_beginset(onlinerefresh_persistnode* persistnode, recpos pos);

/* Set persistnode end position  */
extern void onlinerefresh_persistnode_endset(onlinerefresh_persistnode* persistnode, recpos pos);

/* Set persistnode incremental flag */
extern void onlinerefresh_persistnode_incrementset(onlinerefresh_persistnode* persistnode,
                                                   bool                       incrment);

/* Initialize onlinerefresh persist */
extern onlinerefresh_persist* onlinerefresh_persist_init(void);

/* UUID comparison when deleting persistnode */
extern int onlinerefresh_persist_delectbyuuidcmp(void* vala, void* valb);

/* Set node status by uuid */
extern void onlinerefresh_persist_statesetbyuuid(onlinerefresh_persist* persist,
                                                 uuid_t*                uuid,
                                                 int                    state);

/* Delete stock table in node by uuid */
extern void onlinerefresh_persist_removerefreshtbsbyuuid(onlinerefresh_persist* persist,
                                                         uuid_t*                uuid);

/* Write persist to disk */
extern bool onlinerefresh_persist_write(onlinerefresh_persist* persist);

/* Load persist */
extern onlinerefresh_persist* onlinerefresh_persist_read(void);

/* Delete node by uuid */
extern void onlinerefresh_persist_removebyuuid(onlinerefresh_persist* persist, uuid_t* uuid);

/* Calculate persist rewind point */
extern void onlinerefresh_persist_electionrewindbyuuid(onlinerefresh_persist* persist,
                                                       uuid_t*                uuid);

/* Clean up persistnode memory */
extern void onlinerefresh_persistnode_free(void* privdata);

/* Clean up persist memory */
extern void onlinerefresh_persist_free(onlinerefresh_persist* persist);

#endif
