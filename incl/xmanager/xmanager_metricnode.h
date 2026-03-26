#ifndef _XMANAGER_METRICNODE_H_
#define _XMANAGER_METRICNODE_H_

#define XMANAGER_METRICNODEBLKSIZE 8192

typedef enum XMANAGER_METRICNODESTAT
{
    XMANAGER_METRICNODESTAT_NOP = 0x00,
    XMANAGER_METRICNODESTAT_INIT,
    XMANAGER_METRICNODESTAT_ONLINE,
    XMANAGER_METRICNODESTAT_OFFLINE,

    /* Add before this */
    XMANAGER_METRICNODESTAT_MAX
} xmanager_metricnodestat;

typedef struct XMANAGER_METRICNODE
{
    xmanager_metricnodetype type;

    /* Reserved, not used yet, for future automatic networking */
    bool                    remote;

    /* Status */
    xmanager_metricnodestat stat;

    /* Name */
    char*                   name;

    /* Working directory, can be empty */
    char*                   data;

    /* Config directory, can be empty */
    char*                   conf;

    /* Trail file directory, can be empty */
    char*                   traildir;
} xmanager_metricnode;

typedef struct XMANAGER_METRICFD2NODE
{
    int                  fd;
    xmanager_metricnode* metricnode;
} xmanager_metricfd2node;

typedef struct XMANAGER_METRICREGNODE
{
    /* Node type */
    xmanager_metricnodetype nodetype;

    /* Message type */
    xmanager_msg            msgtype;

    /* Used to indicate if operation succeeded, only contains msg on error, 0 success 1 failure */
    int                     result;

    /* Error code */
    int                     errcode;

    /* Error message */
    char*                   msg;

    /* Node */
    xmanager_metricfd2node* metricfd2node;
} xmanager_metricregnode;

extern void xmanager_metricnode_reset(xmanager_metricnode* metricnode);

/* Calculate memory occupied by metricnode */
extern int xmanager_metricnode_serialsize(xmanager_metricnode* metricnode);

/* Serialize metricnode */
extern void xmanager_metricnode_serial(xmanager_metricnode* metricnode, uint8* blk, int* blkstart);

/* Deserialize */
extern bool xmanager_metricnode_deserial(xmanager_metricnode* metricnode,
                                         uint8*               blk,
                                         int*                 blkstart);

extern xmanager_metricnode* xmanager_metricnode_init(xmanager_metricnodetype nodetype);

extern char* xmanager_metricnode_getname(xmanager_metricnodetype nodetype);

extern void xmanager_metricnode_destroy(xmanager_metricnode* metricnode);

extern int xmanager_metricnode_cmp(void* s1, void* s2);

/* Write metricnode to disk */
extern void xmanager_metricnode_flush(dlist* dlmetricnodes);

/* Load metircnode.dat file */
extern bool xmanager_metricnode_load(dlist** pdlmetricnodes);

extern void xmanager_metricnode_destroyvoid(void* args);

/*-----------------Descriptor and structure mapping operation begin-------------------*/
extern xmanager_metricfd2node* xmanager_metricfd2node_init(void);

extern int xmanager_metricfd2node_cmp(void* s1, void* s2);

/* Compare using metricnode */
extern int xmanager_metricfd2node_cmp2(void* s1, void* s2);

extern void xmanager_metricfd2node_destroy(xmanager_metricfd2node* metricfd2node);

extern void xmanager_metricfd2node_destroyvoid(void* args);

/*-----------------Descriptor and structure mapping operation   end-------------------*/

/* Initialize */
extern xmanager_metricregnode* xmanager_metricregnode_init(void);

/* Release */
extern void xmanager_metricregnode_destroy(xmanager_metricregnode* mregnode);

#endif
