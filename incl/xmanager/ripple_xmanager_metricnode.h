#ifndef _RIPPLE_XMANAGER_METRICNODE_H_
#define _RIPPLE_XMANAGER_METRICNODE_H_


#define RIPPLE_XMANAGER_METRICNODEBLKSIZE               8192

typedef enum RIPPLE_XMANAGER_METRICNODESTAT
{
    RIPPLE_XMANAGER_METRICNODESTAT_NOP                  = 0x00,
    RIPPLE_XMANAGER_METRICNODESTAT_INIT                 ,
    RIPPLE_XMANAGER_METRICNODESTAT_ONLINE               ,
    RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE              ,

    /* 在此之前添加 */
    RIPPLE_XMANAGER_METRICNODESTAT_MAX
} ripple_xmanager_metricnodestat;

typedef struct RIPPLE_XMANAGER_METRICNODE
{
    ripple_xmanager_metricnodetype          type;

    /* 预留, 暂不使用, 用于后期的自动组网 */
    bool                                    remote;

    /* 状态 */
    ripple_xmanager_metricnodestat          stat;

    /* 名称 */
    char*                                   name;

    /* 工作目录,可为空 */
    char*                                   data;

    /* 配置目录,可为空 */
    char*                                   conf;

    /* trail文件目录,可为空 */
    char*                                   traildir;
} ripple_xmanager_metricnode;

typedef struct RIPPLE_XMANAGER_METRICFD2NODE
{
    int                                     fd;
    ripple_xmanager_metricnode*             metricnode;
} ripple_xmanager_metricfd2node;


typedef struct RIPPLE_XMANAGER_METRICREGNODE
{
    /* 节点类型 */
    ripple_xmanager_metricnodetype          nodetype;

    /* 消息类型 */
    ripple_xmanager_msg                     msgtype;

    /* 用于标识操作是否成功, 错误时只含有 msg, 0 成功 1 失败 */
    int                                     result;

    /* 错误码 */
    int                                     errcode;

    /* 错误信息 */
    char*                                   msg;

    /* 节点 */
    ripple_xmanager_metricfd2node*          metricfd2node;
} ripple_xmanager_metricregnode;

extern void ripple_xmanager_metricnode_reset(ripple_xmanager_metricnode* metricnode);

/* 计算 metricnode 所占用的内存 */
extern int ripple_xmanager_metricnode_serialsize(ripple_xmanager_metricnode* metricnode);

/* 序列化 metricnode */
extern void ripple_xmanager_metricnode_serial(ripple_xmanager_metricnode* metricnode,
                                              uint8* blk,
                                              int* blkstart);

/* 反序列化 */
extern bool ripple_xmanager_metricnode_deserial(ripple_xmanager_metricnode* metricnode,
                                                uint8* blk,
                                                int* blkstart);


extern ripple_xmanager_metricnode* ripple_xmanager_metricnode_init(ripple_xmanager_metricnodetype nodetype);

extern char* ripple_xmanager_metricnode_getname(ripple_xmanager_metricnodetype nodetype);

extern void ripple_xmanager_metricnode_destroy(ripple_xmanager_metricnode* metricnode);

extern int ripple_xmanager_metricnode_cmp(void* s1, void* s2);

/* 将 metricnode 落盘 */
extern void ripple_xmanager_metricnode_flush(dlist* dlmetricnodes);


/* 加载 metircnode.dat 文件 */
extern bool ripple_xmanager_metricnode_load(dlist** pdlmetricnodes);

extern void ripple_xmanager_metricnode_destroyvoid(void* args);

/*-----------------描述符与结构映射操作 begin-------------------*/
extern ripple_xmanager_metricfd2node* ripple_xmanager_metricfd2node_init(void);

extern int ripple_xmanager_metricfd2node_cmp(void* s1, void* s2);

/* 用 metricnode 进行比较 */
extern int ripple_xmanager_metricfd2node_cmp2(void* s1, void* s2);

extern void ripple_xmanager_metricfd2node_destroy(ripple_xmanager_metricfd2node* metricfd2node);

extern void ripple_xmanager_metricfd2node_destroyvoid(void* args);

/*-----------------描述符与结构映射操作   end-------------------*/

/* 初始化 */
extern ripple_xmanager_metricregnode* ripple_xmanager_metricregnode_init(void);

/* 释放 */
extern void ripple_xmanager_metricregnode_destroy(ripple_xmanager_metricregnode* mregnode);

#endif
