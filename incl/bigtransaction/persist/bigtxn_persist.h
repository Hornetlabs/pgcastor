#ifndef _BIGTXN_PERSIST_H
#define _BIGTXN_PERSIST_H

typedef enum BIGTXN_PERSISTNODE_STAT
{
    BIGTXN_PERSISTNODE_STAT_NOP = 0x00,
    BIGTXN_PERSISTNODE_STAT_INIT,
    BIGTXN_PERSISTNODE_STAT_INPROCESS,
    BIGTXN_PERSISTNODE_STAT_DONE,
    BIGTXN_PERSISTNODE_STAT_ABANDON
} bigtxn_persistnode_stat;

typedef struct BIGTXN_PERSISTNODE
{
    recpos            begin; /* Big transaction start point, fileid/fileoffset */
    recpos            end;   /* Big transaction end point, fileid/fileoffset */
    FullTransactionId xid;   /* Big transaction ID */
    int               stat;  /* Big transaction status */
} bigtxn_persistnode;

typedef struct BIGTXN_PERSIST
{
    recpos rewind;        /* Earliest unfinished big transaction start point */
    uint32 count;         /* Number of saved big transaction status */
    dlist* dpersistnodes; /* Big transaction status node bigtxn_persistnode */
} bigtxn_persist;

bigtxn_persist* bigtxn_persist_init(void);

bigtxn_persistnode* bigtxn_persist_node_init(void);

void bigtxn_persist_electionrewind(bigtxn_persist* persist, recpos* pos);

/* Select new rewind node */
void bigtxn_persist_electionrewindbyxid(bigtxn_persist* persist, FullTransactionId xid);

void bigtxn_persist_removebyxid(bigtxn_persist* persist, FullTransactionId xid);

int bigtxn_integratepersist_delectbyxidcmp(void* vala, void* valb);

void bigtxn_integratepersist_cleannotdone(bigtxn_persist* persist);

void bigtxn_persist_free(bigtxn_persist* persist);

void bigtxn_persistnode_free(void* persistnode);

bool bigtxn_write_persist(bigtxn_persist* persist);

bigtxn_persist* bigtxn_read_persist(void);

void bigtxn_persist_set_state_by_xid(bigtxn_persist* persist, FullTransactionId xid, int state);

extern void bigtxn_persistnode_set_begin(bigtxn_persistnode* node, recpos* pos);
extern void bigtxn_persistnode_set_end(bigtxn_persistnode* node, recpos* pos);
extern void bigtxn_persistnode_set_xid(bigtxn_persistnode* node, FullTransactionId xid);
extern void bigtxn_persistnode_set_stat_init(bigtxn_persistnode* node);

#endif
