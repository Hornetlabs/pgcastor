#ifndef _RIPPLE_FILETRANSFER_H
#define _RIPPLE_FILETRANSFER_H

#define RIPPLE_GAP_TIMEOUT                  20

typedef enum RIPPLE_FILETRANSFERNODE_TYPE
{
    RIPPLE_FILETRANSFERNODE_TYPE_NOP                            = 0x00,
    RIPPLE_FILETRANSFERNODE_TYPE_INCREAMENT                     ,           /* 增量数据类型 */
    RIPPLE_FILETRANSFERNODE_TYPE_BIGTXN_INC                     ,           /* 大事务 */
    RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_INC              ,           /* onlinerefresh增量数据类型 */
    RIPPLE_FILETRANSFERNODE_TYPE_REFRESH                        ,           /* 存量数据类型 */
    RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_SHARDING         ,           /* onlinerefresh存量数据类型 */
    RIPPLE_FILETRANSFERNODE_TYPE_REFRESHSHARDS                  ,           /* 存量数据分片信息文件 */
    RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS            ,           /* olinerefresh存量数据分片信息文件 */
    RIPPLE_FILETRANSFERNODE_TYPE_DELETEFILE                     ,
    RIPPLE_FILETRANSFERNODE_TYPE_DELETEDIR
}ripple_filetransfernode_type;

typedef struct RIPPLE_FILETRANSFERNODE
{
    int                 type;
    bool                partial;                        /* 文件是否不完整 用于上传时清理文件*/
    uint64              trail;
    char                jobname[128];
    char                localpath[RIPPLE_MAXPATH];      /* 本地文件 */
    char                localdir[RIPPLE_MAXPATH];       /* 本地文件夹，下载时创建本地文件夹 */
}ripple_filetransfernode;

/* collector保存未上传文件信息 */
typedef struct RIPPLE_FILETRANSFER_METADATA
{
    int                 type;
    uint32              shards;
    uint32              shardnum;
    uint64              trail;
    FullTransactionId   xid;
    char                schema[NAMEDATALEN];
    char                table[NAMEDATALEN];
    char                uuid[37];
}ripple_filetransfer_metadata;

typedef struct RIPPLE_FILETRANSFER_INCREMENT
{
    ripple_filetransfernode                 base;
    bool                                    partial;                            /* 文件是否完整 */
    char                                    prefixpath[RIPPLE_MAXPATH];         /* data/前缀路径 */
}ripple_filetransfer_increment;

typedef struct RIPPLE_FILETRANSFER_ONLINEREFRESHINC
{
    ripple_filetransfernode                 base;
    bool                                    partial;                            /* 文件是否完整 */
    char                                    uuid[37];                           /* onlinerefresh使用 */
    char                                    prefixpath[RIPPLE_MAXPATH];         /* data/前缀路径 */
}ripple_filetransfer_onlinerefreshinc;

typedef struct RIPPLE_FILETRANSFER_BIGTXNINC
{
    ripple_filetransfernode                 base;
    bool                                    partial;                            /* 文件是否完整 */
    FullTransactionId                       xid;                                /* 事务号 */
    char                                    prefixpath[RIPPLE_MAXPATH];         /* data/前缀路径 */
}ripple_filetransfer_bigtxninc;

typedef struct RIPPLE_FILETRANSFER_REFRESH
{
    ripple_filetransfernode                 base;
    char                                    uuid[37];                           /* onlinerefresh使用 */
    char                                    prefixpath[RIPPLE_MAXPATH];         /* data/前缀路径 */
    char                                    schema[NAMEDATALEN];
    char                                    table[NAMEDATALEN];
    uint32                                  shards;
    uint32                                  shardnum;
}ripple_filetransfer_refresh;

/* 上传refresh分片数，下载时生成分片任务 */
typedef struct RIPPLE_FILETRANSFER_REFRESHSHARDS
{
    ripple_filetransfernode                 base;
    char                                    prefixpath[RIPPLE_MAXPATH];         /* data/前缀路径 */
    char                                    schema[NAMEDATALEN];
    char                                    table[NAMEDATALEN];
    char                                    uuid[37];
}ripple_filetransfer_refreshshards;

typedef struct RIPPLE_FILETRANSFER_INCDELETEFILE
{
    ripple_filetransfernode                 base;
    char                                    prefixpath[RIPPLE_MAXPATH];         /* data/前缀路径 */
}ripple_filetransfer_cleanpath;

typedef struct RIPPLE_FILETRANSFER_REFRESHINFO
{
    char                                    schema[NAMEDATALEN];
    char                                    table[NAMEDATALEN];
    uint32                                  shards;
}ripple_filetransfer_refreshinfo;

/* 服务器连接信息 */
typedef struct RIPPLE_FILETRANSFER
{
    char                prefixurl[RIPPLE_MAXPATH];
    char                user[64];
    char                password[128];
    char*               ftpdata;
    int64               port;
}ripple_filetransfer;

void ripple_filetransfer_reset(ripple_filetransfer* filetransfer);

ripple_filetransfer_increment* ripple_filetransfer_increment_init(void);

ripple_filetransfer_cleanpath* ripple_filetransfer_cleanpath_init(void);

ripple_filetransfer_onlinerefreshinc* ripple_filetransfer_onlinerefreshinc_init(void);

ripple_filetransfer_bigtxninc* ripple_filetransfer_bigtxninc_init(void);

ripple_filetransfer_refreshshards* ripple_filetransfer_refreshshards_init(void);

ripple_filetransfer_refresh* ripple_filetransfer_refresh_init(void);

bool ripple_filetransfer_node_cmp(void* node1, void* node2);

void ripple_filetransfer_node_add(void* queue_in, void* filetransfernode);

void ripple_filetransfer_increment_trail_set(ripple_filetransfernode* node, uint64 trail);

void ripple_filetransfer_refresh_set(ripple_filetransfer_refresh* ftransfer_refresh, char* schema, char* table, uint32 shards, uint32 shardnum);

void ripple_filetransfer_upload_path_set(ripple_filetransfer_increment* filetransfer_inc, char* jobname);

void ripple_filetransfer_upload_bigtxnincpath_set(ripple_filetransfer_bigtxninc* filetransfer_inc, FullTransactionId xid, char* jobname);

void ripple_filetransfer_upload_olincpath_set(ripple_filetransfer_onlinerefreshinc* filetransfer_inc, char* uuid, char* jobname);

void ripple_filetransfer_upload_refreshpath_set(ripple_filetransfer_refresh* ftransfer_refresh, char* jobname);

void ripple_filetransfer_upload_olrefreshshardspath_set(ripple_filetransfer_refresh* ftransfer_refresh, char* uuid, char* jobname);

void ripple_filetransfer_download_path_set(ripple_filetransfer_increment* filetransfer_inc, char* traildir, char* jobname);

void ripple_filetransfer_download_bigtxnincpath_set(ripple_filetransfer_bigtxninc* filetransfer_inc, char* traildir, FullTransactionId xid, char* jobname);

void ripple_filetransfer_download_olincpath_set(ripple_filetransfer_onlinerefreshinc* filetransfer_inc, char* traildir, char* uuid, char* jobname);

void ripple_filetransfer_download_refreshshards_set(ripple_filetransfer_refreshshards* ftransfer_check, char* schema, char* table);

void ripple_filetransfer_download_olrefreshshards_set(ripple_filetransfer_refreshshards* ftransfer_check, char* uuid, char* schema, char* table);

bool ripple_filetransfer_metadatafile_set(void* filetransfernode);

bool ripple_filetransfer_metadatafile_remove(void* filetransfernode);

void* ripple_filetransfer_makenode_fromfile(char* jobname, char* filename);

void ripple_filetransfer_queuefree(void* filetransfernode);

#endif
