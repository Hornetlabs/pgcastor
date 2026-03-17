#ifndef _XMANAGER_METRICNODE_H_
#define _XMANAGER_METRICNODE_H_


#define XMANAGER_METRICNODEBLKSIZE               8192

typedef enum XMANAGER_METRICNODESTAT
{
    XMANAGER_METRICNODESTAT_NOP                  = 0x00,
    XMANAGER_METRICNODESTAT_INIT                 ,
    XMANAGER_METRICNODESTAT_ONLINE               ,
    XMANAGER_METRICNODESTAT_OFFLINE              ,

    /* 在此之前添加 */
    XMANAGER_METRICNODESTAT_MAX
} xmanager_metricnodestat;

typedef struct XMANAGER_METRICNODE
{
    xmanager_metricnodetype          type;

    /* 预留, 暂不使用, 用于后期的自动组网 */
    bool                                    remote;

    /* 状态 */
    xmanager_metricnodestat          stat;

    /* 名称 */
    char*                                   name;

    /* 工作目录,可为空 */
    char*                                   data;

    /* 配置目录,可为空 */
    char*                                   conf;

    /* trail文件目录,可为空 */
    char*                                   traildir;
} xmanager_metricnode;

typedef struct XMANAGER_METRICFD2NODE
{
    int                                     fd;
    xmanager_metricnode*             metricnode;
} xmanager_metricfd2node;


typedef struct XMANAGER_METRICREGNODE
{
    /* 节点类型 */
    xmanager_metricnodetype          nodetype;

    /* 消息类型 */
    xmanager_msg                     msgtype;

    /* 用于标识操作是否成功, 错误时只含有 msg, 0 成功 1 失败 */
    int                                     result;

    /* 错误码 */
    int                                     errcode;

    /* 错误信息 */
    char*                                   msg;

    /* 节点 */
    xmanager_metricfd2node*          metricfd2node;
} xmanager_metricregnode;

extern void xmanager_metricnode_reset(xmanager_metricnode* metricnode);

/* 计算 metricnode 所占用的内存 */
extern int xmanager_metricnode_serialsize(xmanager_metricnode* metricnode);

/* 序列化 metricnode */
extern void xmanager_metricnode_serial(xmanager_metricnode* metricnode,
                                              uint8* blk,
                                              int* blkstart);

/* 反序列化 */
extern bool xmanager_metricnode_deserial(xmanager_metricnode* metricnode,
                                                uint8* blk,
                                                int* blkstart);


extern xmanager_metricnode* xmanager_metricnode_init(xmanager_metricnodetype nodetype);

extern char* xmanager_metricnode_getname(xmanager_metricnodetype nodetype);

extern void xmanager_metricnode_destroy(xmanager_metricnode* metricnode);

extern int xmanager_metricnode_cmp(void* s1, void* s2);

/* 将 metricnode 落盘 */
extern void xmanager_metricnode_flush(dlist* dlmetricnodes);


/* 加载 metircnode.dat 文件 */
extern bool xmanager_metricnode_load(dlist** pdlmetricnodes);

extern void xmanager_metricnode_destroyvoid(void* args);

/*-----------------描述符与结构映射操作 begin-------------------*/
extern xmanager_metricfd2node* xmanager_metricfd2node_init(void);

extern int xmanager_metricfd2node_cmp(void* s1, void* s2);

/* 用 metricnode 进行比较 */
extern int xmanager_metricfd2node_cmp2(void* s1, void* s2);

extern void xmanager_metricfd2node_destroy(xmanager_metricfd2node* metricfd2node);

extern void xmanager_metricfd2node_destroyvoid(void* args);

/*-----------------描述符与结构映射操作   end-------------------*/

/* 初始化 */
extern xmanager_metricregnode* xmanager_metricregnode_init(void);

/* 释放 */
extern void xmanager_metricregnode_destroy(xmanager_metricregnode* mregnode);

#endif
