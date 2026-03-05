#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "queue/ripple_queue.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"

void ripple_filetransfer_reset(ripple_filetransfer* filetransfer)
{
    char* url = NULL;
    /* 重置url */
    rmemset1(filetransfer->prefixurl, 0, '\0', RIPPLE_MAXPATH);

    /* 重置user */
    rmemset1(filetransfer->user, 0, '\0', 64);

    /* 重置密码 */
    rmemset1(filetransfer->password, 0, '\0', 128);

    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);

    if (url == NULL || url[0] == '\0')
    {
        return;
    }

    snprintf(filetransfer->prefixurl, RIPPLE_MAXPATH, url);

    filetransfer->ftpdata = guc_getConfigOption(RIPPLE_CFG_KEY_FTPDATA);

    return;
}

void ripple_filetransfer_increment_trail_set(ripple_filetransfernode* node, uint64 trail)
{
    node->trail = trail;
}

void ripple_filetransfer_refresh_set(ripple_filetransfer_refresh* ftransfer_refresh, char* schema, char* table, uint32 shards, uint32 shardnum)
{
    rmemset1(ftransfer_refresh->schema, 0, '\0', NAMEDATALEN);
    rmemset1(ftransfer_refresh->table, 0, '\0', NAMEDATALEN);
    rmemcpy1(ftransfer_refresh->schema, 0, schema, strlen(schema));
    rmemcpy1(ftransfer_refresh->table, 0, table, strlen(table));
    ftransfer_refresh->shardnum = shardnum;
    ftransfer_refresh->shards = shards;
}

void ripple_filetransfer_upload_path_set(ripple_filetransfer_increment* filetransfer_inc, char* jobname)
{
    snprintf(filetransfer_inc->base.jobname, 128, "%s", jobname);
    snprintf(filetransfer_inc->base.localpath, RIPPLE_MAXPATH, "%s/%s/%016lX", jobname, RIPPLE_STORAGE_TRAIL_DIR, filetransfer_inc->base.trail);
    snprintf(filetransfer_inc->base.localdir, RIPPLE_MAXPATH, "%s/%s", jobname, RIPPLE_STORAGE_TRAIL_DIR);
    snprintf(filetransfer_inc->prefixpath, RIPPLE_MAXPATH, "%s/%s", jobname, RIPPLE_STORAGE_TRAIL_DIR);
}

void ripple_filetransfer_upload_olincpath_set(ripple_filetransfer_onlinerefreshinc* filetransfer_inc, char* uuid, char* jobname)
{
    snprintf(filetransfer_inc->base.jobname, 128, "%s", jobname);
    snprintf(filetransfer_inc->uuid, 37, "%s", uuid);
    snprintf(filetransfer_inc->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%016lX", jobname,
                                                                                     RIPPLE_REFRESH_ONLINEREFRESH,
                                                                                     uuid, RIPPLE_STORAGE_TRAIL_DIR,
                                                                                     filetransfer_inc->base.trail);
    snprintf(filetransfer_inc->base.localdir, RIPPLE_MAXPATH, "%s/%s/%s/%s", jobname,
                                                                             RIPPLE_REFRESH_ONLINEREFRESH,
                                                                             uuid,
                                                                             RIPPLE_STORAGE_TRAIL_DIR);
    snprintf(filetransfer_inc->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s/%s", jobname,
                                                                          RIPPLE_REFRESH_ONLINEREFRESH,
                                                                          uuid,
                                                                          RIPPLE_STORAGE_TRAIL_DIR);
}

void ripple_filetransfer_upload_bigtxnincpath_set(ripple_filetransfer_bigtxninc* filetransfer_inc, FullTransactionId xid, char* jobname)
{
    snprintf(filetransfer_inc->base.jobname, 128, "%s", jobname);
    filetransfer_inc->xid = xid;
    snprintf(filetransfer_inc->base.localpath, RIPPLE_MAXPATH, "%s/%s/%lu/%016lX", jobname,
                                                                                   RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                                                                                   xid,
                                                                                   filetransfer_inc->base.trail);
    snprintf(filetransfer_inc->base.localdir, RIPPLE_MAXPATH, "%s/%s/%lu", jobname,
                                                                           RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                                                                           xid);
    snprintf(filetransfer_inc->prefixpath, RIPPLE_MAXPATH, "%s/%s/%lu", jobname,
                                                                        RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                                                                        xid);
}

void ripple_filetransfer_upload_refreshpath_set(ripple_filetransfer_refresh* ftransfer_refresh, char* jobname)
{
    snprintf(ftransfer_refresh->base.jobname, 128, "%s", jobname);
    snprintf(ftransfer_refresh->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s_%s/%s/%s_%s_%u_%u",
                                                                jobname,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                RIPPLE_REFRESH_COMPLETE,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                ftransfer_refresh->shards,
                                                                ftransfer_refresh->shardnum);
    snprintf(ftransfer_refresh->base.localdir, RIPPLE_MAXPATH, "%s/%s/%s_%s/%s",
                                                                jobname,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                RIPPLE_REFRESH_COMPLETE);
    snprintf(ftransfer_refresh->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s_%s/%s",
                                                            jobname,
                                                            RIPPLE_REFRESH_REFRESH,
                                                            ftransfer_refresh->schema,
                                                            ftransfer_refresh->table,
                                                            RIPPLE_REFRESH_COMPLETE);
}

void ripple_filetransfer_upload_olrefreshshardspath_set(ripple_filetransfer_refresh* ftransfer_refresh, char* uuid, char* jobname)
{
    snprintf(ftransfer_refresh->base.jobname, 128, "%s", jobname);
    snprintf(ftransfer_refresh->uuid, 37, "%s", uuid);
    snprintf(ftransfer_refresh->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s/%s/%s_%s_%u_%u",
                                                                jobname,
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                RIPPLE_REFRESH_COMPLETE,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                ftransfer_refresh->shards,
                                                                ftransfer_refresh->shardnum);
    snprintf(ftransfer_refresh->base.localdir, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s/%s",
                                                                jobname,
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                RIPPLE_REFRESH_COMPLETE);
    snprintf(ftransfer_refresh->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s/%s",
                                                            jobname,
                                                            RIPPLE_REFRESH_ONLINEREFRESH,
                                                            uuid,
                                                            RIPPLE_REFRESH_REFRESH,
                                                            ftransfer_refresh->schema,
                                                            ftransfer_refresh->table,
                                                            RIPPLE_REFRESH_COMPLETE);
}

void ripple_filetransfer_download_path_set(ripple_filetransfer_increment* filetransfer_inc, char* traildir, char* jobname)
{
    snprintf(filetransfer_inc->base.localpath, RIPPLE_MAXPATH, "%s/%016lX", traildir, filetransfer_inc->base.trail);
    snprintf(filetransfer_inc->base.localdir, RIPPLE_MAXPATH, "%s", traildir);
    snprintf(filetransfer_inc->prefixpath, RIPPLE_MAXPATH, "%s/%s", jobname, RIPPLE_STORAGE_TRAIL_DIR);
}

void ripple_filetransfer_download_olincpath_set(ripple_filetransfer_onlinerefreshinc* filetransfer_inc, char* traildir, char* uuid, char* jobname)
{
    snprintf(filetransfer_inc->base.localpath, RIPPLE_MAXPATH, "%s/%016lX", traildir, filetransfer_inc->base.trail);
    snprintf(filetransfer_inc->base.localdir, RIPPLE_MAXPATH, "%s", traildir);
    snprintf(filetransfer_inc->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s/%s", jobname, RIPPLE_REFRESH_ONLINEREFRESH, uuid, RIPPLE_STORAGE_TRAIL_DIR);
}

void ripple_filetransfer_download_bigtxnincpath_set(ripple_filetransfer_bigtxninc* filetransfer_inc, char* traildir, FullTransactionId xid, char* jobname)
{
    snprintf(filetransfer_inc->base.localpath, RIPPLE_MAXPATH, "%s/%016lX", traildir, filetransfer_inc->base.trail);
    snprintf(filetransfer_inc->base.localdir, RIPPLE_MAXPATH, "%s", traildir);
    snprintf(filetransfer_inc->prefixpath, RIPPLE_MAXPATH, "%s/%s/%lu", jobname, RIPPLE_STORAGE_BIG_TRANSACTION_DIR, xid);
}

void ripple_filetransfer_download_refreshshards_set(ripple_filetransfer_refreshshards* ftransfer_check, char* schema, char* table)
{
    snprintf(ftransfer_check->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s_%s/%s_%s",
                                                                guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                                                RIPPLE_REFRESH_REFRESH,
                                                                schema,
                                                                table,
                                                                schema,
                                                                table);
    snprintf(ftransfer_check->base.localdir, RIPPLE_MAXPATH, "%s/%s/%s_%s",
                                                                guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                                                RIPPLE_REFRESH_REFRESH,
                                                                schema,
                                                                table);
    snprintf(ftransfer_check->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s_%s",
                                                                guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME),
                                                                RIPPLE_REFRESH_REFRESH,
                                                                schema,
                                                                table);
    snprintf(ftransfer_check->schema, NAMEDATALEN, "%s", schema);
    snprintf(ftransfer_check->table, NAMEDATALEN, "%s", table);
}

void ripple_filetransfer_download_olrefreshshards_set(ripple_filetransfer_refreshshards* ftransfer_check, char* uuid, char* schema, char* table)
{
    snprintf(ftransfer_check->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s/%s_%s",
                                                                guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                schema,
                                                                table,
                                                                schema,
                                                                table);
    snprintf(ftransfer_check->base.localdir, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s",
                                                                guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                schema,
                                                                table);
    snprintf(ftransfer_check->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s",
                                                                guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME),
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                schema,
                                                                table);
    snprintf(ftransfer_check->schema, NAMEDATALEN, "%s", schema);
    snprintf(ftransfer_check->table, NAMEDATALEN, "%s", table);
}

ripple_filetransfer_increment* ripple_filetransfer_increment_init(void)
{
    ripple_filetransfer_increment* filetransfer_inc = NULL;
    filetransfer_inc = (ripple_filetransfer_increment*)rmalloc0(sizeof(ripple_filetransfer_increment));
    if(NULL == filetransfer_inc)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(filetransfer_inc, 0, '\0', sizeof(ripple_filetransfer_increment));

    filetransfer_inc->base.type = RIPPLE_FILETRANSFERNODE_TYPE_INCREAMENT;

    return filetransfer_inc;
}

ripple_filetransfer_cleanpath* ripple_filetransfer_cleanpath_init(void)
{
    ripple_filetransfer_cleanpath* cleanpath = NULL;
    cleanpath = (ripple_filetransfer_cleanpath*)rmalloc0(sizeof(ripple_filetransfer_cleanpath));
    if(NULL == cleanpath)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(cleanpath, 0, '\0', sizeof(ripple_filetransfer_cleanpath));

    cleanpath->base.type = RIPPLE_FILETRANSFERNODE_TYPE_DELETEFILE;

    return cleanpath;
}

ripple_filetransfer_onlinerefreshinc* ripple_filetransfer_onlinerefreshinc_init(void)
{
    ripple_filetransfer_onlinerefreshinc* filetransfer_inc = NULL;
    filetransfer_inc = (ripple_filetransfer_onlinerefreshinc*)rmalloc0(sizeof(ripple_filetransfer_onlinerefreshinc));
    if(NULL == filetransfer_inc)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(filetransfer_inc, 0, '\0', sizeof(ripple_filetransfer_onlinerefreshinc));

    filetransfer_inc->base.type = RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_INC;
    filetransfer_inc->base.partial = false;

    return filetransfer_inc;
}

ripple_filetransfer_bigtxninc* ripple_filetransfer_bigtxninc_init(void)
{
    ripple_filetransfer_bigtxninc* bigtxngap = NULL;
    bigtxngap = (ripple_filetransfer_bigtxninc*)rmalloc0(sizeof(ripple_filetransfer_bigtxninc));
    if(NULL == bigtxngap)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(bigtxngap, 0, '\0', sizeof(ripple_filetransfer_bigtxninc));

    bigtxngap->base.type = RIPPLE_FILETRANSFERNODE_TYPE_BIGTXN_INC;
    bigtxngap->base.partial = false;

    return bigtxngap;
}

ripple_filetransfer_refreshshards* ripple_filetransfer_refreshshards_init(void)
{
    ripple_filetransfer_refreshshards* refreshfile = NULL;
    refreshfile = (ripple_filetransfer_refreshshards*)rmalloc0(sizeof(ripple_filetransfer_refreshshards));
    if(NULL == refreshfile)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(refreshfile, 0, '\0', sizeof(ripple_filetransfer_refreshshards));

    refreshfile->base.type = RIPPLE_FILETRANSFERNODE_TYPE_REFRESHSHARDS;
    refreshfile->base.partial = false;

    return refreshfile;
}

ripple_filetransfer_refresh* ripple_filetransfer_refresh_init(void)
{
    ripple_filetransfer_refresh* refresh = NULL;
    refresh = (ripple_filetransfer_refresh*)rmalloc0(sizeof(ripple_filetransfer_refresh));
    if(NULL == refresh)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(refresh, 0, '\0', sizeof(ripple_filetransfer_refresh));

    refresh->base.type = RIPPLE_FILETRANSFERNODE_TYPE_REFRESH;
    refresh->base.partial = false;

    return refresh;
}

bool ripple_filetransfer_node_cmp(void* node1, void* node2)
{
    ripple_filetransfernode* filetransfernode1 = NULL;
    ripple_filetransfernode* filetransfernode2 = NULL;

    if (!node1 || !node2 )
    {
        return false;
    }

    filetransfernode1 = (ripple_filetransfernode*)node1;
    filetransfernode2 = (ripple_filetransfernode*)node2;

    if (filetransfernode1->type != filetransfernode2->type)
    {
        return false;
    }

    if (strlen(filetransfernode1->localpath) != strlen(filetransfernode2->localpath)
        || strcmp(filetransfernode1->localpath, filetransfernode2->localpath) != 0)
    {
        return false;
    }

    return true;
}

/* 任务不在队列，将中加入到对列 */
void ripple_filetransfer_node_add(void* queue_in, void* filetransfernode)
{
    int iret = 0;
    ripple_queue* queue = NULL;
    ripple_queueitem* qitem = NULL;
    ripple_queueitem* head = NULL;
    ripple_filetransfernode* node = NULL;

    queue = (ripple_queue*)queue_in;
    node = (ripple_filetransfernode*)filetransfernode;

    iret = ripple_thread_lock(&queue->lock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    for(qitem = queue->head; NULL != qitem; qitem = head)
    {
        head = qitem->next;
        if(true == ripple_filetransfer_node_cmp(node, qitem->data))
        {
            iret = ripple_thread_unlock(&queue->lock);
            if(0 != iret)
            {
                elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
            }
            rfree(node);
            return;
        }
    }

    iret = ripple_thread_unlock(&queue->lock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }

    ripple_queue_put(queue, filetransfernode);
    return;
}

/* 根据类型生成相对路径 */
static void ripple_filetransfer_metadatafilepath_set(ripple_filetransfernode* node, ripple_filetransfer_metadata* meatadata, char* path)
{
    rmemset1(path, 0, '\0', RIPPLE_MAXPATH);
    rmemset1(meatadata, 0, '\0', sizeof(ripple_filetransfer_metadata));

    meatadata->type = node->type;
    if(RIPPLE_FILETRANSFERNODE_TYPE_INCREAMENT == node->type)
    {
        ripple_filetransfer_increment* ftransfer_inc = NULL;
        ftransfer_inc = (ripple_filetransfer_increment*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s_%lu", node->jobname,
                                                       RIPPLE_FILETRANSFER_DIR,
                                                        "inc",
                                                        ftransfer_inc->base.trail);
        meatadata->trail = ftransfer_inc->base.trail;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESH == node->type)
    {
        ripple_filetransfer_refresh* ftransfer_refresh = NULL;
        ftransfer_refresh = (ripple_filetransfer_refresh*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s_%s_%s_%u_%u", node->jobname,
                                                                RIPPLE_FILETRANSFER_DIR,
                                                                "refresh",
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                ftransfer_refresh->shards,
                                                                ftransfer_refresh->shardnum);
        snprintf(meatadata->schema, NAMEDATALEN, "%s", ftransfer_refresh->schema);
        snprintf(meatadata->table, NAMEDATALEN, "%s", ftransfer_refresh->table);
        meatadata->shards = ftransfer_refresh->shards;
        meatadata->shardnum = ftransfer_refresh->shardnum;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_INC == node->type)
    {
        ripple_filetransfer_onlinerefreshinc* ftransfer_inc = NULL;
        ftransfer_inc = (ripple_filetransfer_onlinerefreshinc*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s_%lu", node->jobname,
                                                       RIPPLE_FILETRANSFER_DIR,
                                                        ftransfer_inc->uuid,
                                                        ftransfer_inc->base.trail);
        snprintf(meatadata->uuid, 37, "%s", ftransfer_inc->uuid);
        meatadata->trail = ftransfer_inc->base.trail;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_SHARDING == node->type)
    {
        ripple_filetransfer_refresh* ftransfer_refresh = NULL;
        ftransfer_refresh = (ripple_filetransfer_refresh*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s_%s_%s_%u_%u", node->jobname,
                                                                RIPPLE_FILETRANSFER_DIR,
                                                                ftransfer_refresh->uuid,
                                                                ftransfer_refresh->schema,
                                                                ftransfer_refresh->table,
                                                                ftransfer_refresh->shards,
                                                                ftransfer_refresh->shardnum);
        snprintf(meatadata->uuid, 37, "%s", ftransfer_refresh->uuid);
        snprintf(meatadata->schema, NAMEDATALEN, "%s", ftransfer_refresh->schema);
        snprintf(meatadata->table, NAMEDATALEN, "%s", ftransfer_refresh->table);
        meatadata->shards = ftransfer_refresh->shards;
        meatadata->shardnum = ftransfer_refresh->shardnum;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESHSHARDS == node->type)
    {
        ripple_filetransfer_refreshshards* refreshfile = NULL;
        refreshfile = (ripple_filetransfer_refreshshards*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s_%s_%s", node->jobname,
                                                        RIPPLE_FILETRANSFER_DIR,
                                                        "refreshards",
                                                        refreshfile->schema,
                                                        refreshfile->table);
        snprintf(meatadata->schema, NAMEDATALEN, "%s", refreshfile->schema);
        snprintf(meatadata->table, NAMEDATALEN, "%s", refreshfile->table);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS == node->type)
    {
        ripple_filetransfer_refreshshards* refreshfile = NULL;
        refreshfile = (ripple_filetransfer_refreshshards*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s/%s_%s", node->jobname,
                                                        RIPPLE_FILETRANSFER_DIR,
                                                        refreshfile->uuid,
                                                        refreshfile->schema,
                                                        refreshfile->table);
        snprintf(meatadata->schema, NAMEDATALEN, "%s", refreshfile->schema);
        snprintf(meatadata->table, NAMEDATALEN, "%s", refreshfile->table);
        snprintf(meatadata->uuid, 37, "%s", refreshfile->uuid);
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_BIGTXN_INC == node->type)
    {
        ripple_filetransfer_bigtxninc* ftransfer_inc = NULL;
        ftransfer_inc = (ripple_filetransfer_bigtxninc*)node;
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%lu_%lu", node->jobname,
                                                       RIPPLE_FILETRANSFER_DIR,
                                                       ftransfer_inc->xid,
                                                       ftransfer_inc->base.trail);
        meatadata->xid = ftransfer_inc->xid;
        meatadata->trail = ftransfer_inc->base.trail;
    }

    //删除
    elog(RLOG_DEBUG, "metadatafilepath:%s", path);
    return;
}

/* 创建任务对应的文件 */
bool ripple_filetransfer_metadatafile_set(void* filetransfernode)
{
    int fd = -1;
    char path[RIPPLE_MAXPATH] = {'\0'};
    ripple_filetransfer_metadata meatadata = {'\0'};
    ripple_filetransfernode* node = NULL;

    if (NULL == filetransfernode)
    {
        return true;
    }

    node = (ripple_filetransfernode*)filetransfernode;

    ripple_filetransfer_metadatafilepath_set(node, &meatadata, path);

    fd = BasicOpenFile(path, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", path, strerror(errno));
        return false;
    }

    FileWrite(fd, (char *)&meatadata, sizeof(ripple_filetransfer_metadata));

    if(0 != FileSync(fd))
    {
        elog(RLOG_WARNING, "could not fsync file %s", path);
        return false;
    }

    if(FileClose(fd))
    {
        elog(RLOG_WARNING, "could not close file %s", path);
        return false;
    }
    return true;
}

/* 删除任务对应的文件 */
bool ripple_filetransfer_metadatafile_remove(void* filetransfernode)
{
    char path[RIPPLE_MAXPATH] = {'\0'};
    ripple_filetransfer_metadata meatadata = {'\0'};
    ripple_filetransfernode* node = NULL;

    if (NULL == filetransfernode)
    {
        return true;
    }

    node = (ripple_filetransfernode*)filetransfernode;

    ripple_filetransfer_metadatafilepath_set(node, &meatadata, path);

    durable_unlink(path, RLOG_DEBUG);
    return true;
}

/* 从filetransfer目录下生成任务 */
void* ripple_filetransfer_makenode_fromfile(char* jobname, char* filename)
{
    int fd = -1;
    char path[RIPPLE_MAXPATH] = {'\0'};
    ripple_filetransfer_metadata meatadata = {'\0'};
    ripple_filetransfernode* node = NULL;

    snprintf(path, NAMEDATALEN, "%s/%s/%s", jobname,
                                            RIPPLE_FILETRANSFER_DIR,
                                            filename);

    fd = BasicOpenFile(path, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", path, strerror(errno));
        return NULL;
    }

    FileRead(fd, (char *)&meatadata, sizeof(ripple_filetransfer_metadata));

    if(RIPPLE_FILETRANSFERNODE_TYPE_INCREAMENT == meatadata.type)
    {
        ripple_filetransfer_increment* filetransfer_inc = NULL;
        filetransfer_inc = ripple_filetransfer_increment_init();
        ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, meatadata.trail);
        ripple_filetransfer_upload_path_set(filetransfer_inc, jobname);
        node = (ripple_filetransfernode*)filetransfer_inc;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESH == meatadata.type)
    {
        ripple_filetransfer_refresh *refresh = NULL;
        refresh = ripple_filetransfer_refresh_init();
        ripple_filetransfer_refresh_set(refresh,
                                        meatadata.schema,
                                        meatadata.table,
                                        meatadata.shards,
                                        meatadata.shardnum);
        ripple_filetransfer_upload_refreshpath_set(refresh, jobname);
        node = (ripple_filetransfernode*)refresh;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_INC == meatadata.type)
    {
        ripple_filetransfer_onlinerefreshinc* filetransfer_inc = NULL;
        filetransfer_inc = ripple_filetransfer_onlinerefreshinc_init();
        ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, meatadata.trail);
        ripple_filetransfer_upload_olincpath_set(filetransfer_inc, meatadata.uuid, jobname);
        node = (ripple_filetransfernode*)filetransfer_inc;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_SHARDING == meatadata.type)
    {
        ripple_filetransfer_refresh *filetransfer_refresh = NULL;
        filetransfer_refresh = ripple_filetransfer_refresh_init();
        filetransfer_refresh->base.type = RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_SHARDING;
        ripple_filetransfer_refresh_set(filetransfer_refresh,
                                        meatadata.schema,
                                        meatadata.table,
                                        meatadata.shards,
                                        meatadata.shardnum);
        ripple_filetransfer_upload_olrefreshshardspath_set(filetransfer_refresh, meatadata.uuid, jobname);
        node = (ripple_filetransfernode*)filetransfer_refresh;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESHSHARDS == meatadata.type)
    {
        ripple_filetransfer_refreshshards *refreshshards = NULL;

        refreshshards = ripple_filetransfer_refreshshards_init();
        snprintf(refreshshards->base.jobname, 128, "%s", jobname);
        snprintf(refreshshards->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s_%s/%s_%s",
                                                                jobname,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                meatadata.schema,
                                                                meatadata.table,
                                                                meatadata.schema,
                                                                meatadata.table);

        snprintf(refreshshards->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s_%s",
                                                            jobname,
                                                            RIPPLE_REFRESH_REFRESH,
                                                            meatadata.schema,
                                                            meatadata.table);
        snprintf(refreshshards->schema, NAMEDATALEN, "%s", meatadata.schema);
        snprintf(refreshshards->table, NAMEDATALEN, "%s", meatadata.table);
        node = (ripple_filetransfernode*)refreshshards;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS == meatadata.type)
    {
        ripple_filetransfer_refreshshards *refreshshards = NULL;

        refreshshards = ripple_filetransfer_refreshshards_init();
        snprintf(refreshshards->base.jobname, 128, "%s", jobname);
        snprintf(refreshshards->uuid, 37, "%s", meatadata.uuid);
        snprintf(refreshshards->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s/%s_%s",
                                                                jobname,
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                meatadata.uuid,
                                                                RIPPLE_REFRESH_REFRESH,
                                                                meatadata.schema,
                                                                meatadata.table,
                                                                meatadata.schema,
                                                                meatadata.table);

        snprintf(refreshshards->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s",
                                                            jobname,
                                                            RIPPLE_REFRESH_ONLINEREFRESH,
                                                            meatadata.uuid,
                                                            RIPPLE_REFRESH_REFRESH,
                                                            meatadata.schema,
                                                            meatadata.table);
        snprintf(refreshshards->schema, NAMEDATALEN, "%s", meatadata.schema);
        snprintf(refreshshards->table, NAMEDATALEN, "%s", meatadata.table);
        node = (ripple_filetransfernode*)refreshshards;
    }
    else if(RIPPLE_FILETRANSFERNODE_TYPE_BIGTXN_INC == meatadata.type)
    {
        ripple_filetransfer_bigtxninc* filetransfer_inc = NULL;
        filetransfer_inc = ripple_filetransfer_bigtxninc_init();
        ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, meatadata.trail);
        ripple_filetransfer_upload_bigtxnincpath_set(filetransfer_inc, meatadata.xid, jobname);
        node = (ripple_filetransfernode*)filetransfer_inc;
    }

    node->type = meatadata.type;
    return (void*)node;
}

void ripple_filetransfer_queuefree(void* filetransfernode)
{
    if (NULL == filetransfernode)
    {
        return;
    }

    rfree(filetransfernode);

    return;
}

