#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "works/network/ripple_refresh_pumpshardingnet.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "onlinerefresh/pump/netsharding/ripple_onlinerefresh_shardingnet.h"

ripple_onlinerefresh_shardingnet *ripple_onlinerefresh_shardingnet_init(void)
{
    ripple_onlinerefresh_shardingnet *p2csharding = NULL;
    p2csharding = rmalloc0(sizeof(ripple_onlinerefresh_shardingnet));

    if(NULL == p2csharding)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    p2csharding->syncstats = NULL;
    p2csharding->taskqueue = NULL;
    return p2csharding;
}

/* 网络故障设置大事务管理线程状态为reset */
static void ripple_onlinerefresh_shardingnet_setreset(ripple_onlinerefresh_shardingnet* shardingnet)
{
    /* 设置大事务管理线程状态为reset */
    shardingnet->callback.setreset(shardingnet->privdata);
    return ;
}

/* 创建 p2cidentity 数据包挂载到 wpackets 上 */
static bool ripple_onlinerefresh_shardingnet_p2cidentity(ripple_netclient* netclient, uint8* no)
{
    uint32 namelen                  = 0;
    uint32 identity_size            = 0;
    uint8* wuptr                    = NULL;
    char* jobname                   = NULL;
    ripple_netpacket* netpacket     = NULL;

    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);
    if (NULL != jobname && '\0' != jobname[0])
    {
        namelen = strlen(jobname);
    }

    identity_size = (RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_IDENTITY_SIZE + namelen);
    netpacket = ripple_netpacket_init();
    if (NULL == netpacket)
    {
        elog(RLOG_WARNING, "onlinefresh refresh init identity packet error");
        goto ripple_onlinerefresh_shardingnet_p2cidentity_error;
    }
    netpacket->max = RIPPLE_MAXALIGN(identity_size);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = identity_size;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);
    if (NULL == netpacket->data)
    {
        elog(RLOG_WARNING, "onlinefresh refresh init identity data error");
        goto ripple_onlinerefresh_shardingnet_p2cidentity_error;
    }

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_IDENTITY);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, identity_size);
    RIPPLE_CONCAT(put, 8bit)(&wuptr, RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_SHARDING);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, namelen);
    if (0 != namelen)
    {
        rmemcpy1(wuptr, 0, jobname, strlen(jobname));
        wuptr += namelen;
    }
    rmemcpy1(wuptr, 0, no, RIPPLE_UUID_LEN);

    if (false == ripple_netclient_addwpacket(netclient, (void*)netpacket))
    {
        elog(RLOG_WARNING, "add onlinefresh refresh identity packet error");
        goto ripple_onlinerefresh_shardingnet_p2cidentity_error;
    }

    return true;
ripple_onlinerefresh_shardingnet_p2cidentity_error:

    ripple_netpacket_destroy(netpacket);
    return false;
}

/* 创建 beginrefresh 数据包挂载到 wpackets 上 */
static void ripple_onlinerefresh_shardingnet_wpacketsadd_begin(ripple_netclient* netclient, char* schema, char* table, uint32 shards, uint32 shardnum, void* no)
{
    uint16 tblen = 0;
    uint16 schlen = 0;
    uint32 wmsglen = 0;

    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;

    if (!schema || !table)
    {
        elog(RLOG_WARNING, "invalid schema or table name");
        return;
    }
    
    tblen = strlen(table);
    schlen = strlen(schema);

    wmsglen = (schlen + tblen + RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_BEGIN_SIZE);

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_BEGIN);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, wmsglen);

    /* 写数据 */
    /* uuid */
    rmemcpy1(wuptr, 0, no, RIPPLE_UUID_LEN);
    wuptr += RIPPLE_UUID_LEN;

    /* schlen */
    RIPPLE_CONCAT(put, 16bit)(&wuptr, schlen);
    /* schema */
    rmemcpy1(wuptr, 0, schema, schlen);
    wuptr += schlen;
    /* tblen */
    RIPPLE_CONCAT(put, 16bit)(&wuptr, tblen);
    /* table */
    rmemcpy1(wuptr, 0, table, tblen);
    wuptr += tblen;
    /* 分片总数 */
    RIPPLE_CONCAT(put, 32bit)(&wuptr, shards);
    /* 分片编号 */
    RIPPLE_CONCAT(put, 32bit)(&wuptr, shardnum);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 创建 发送数据包挂载到 wpackets 上 */
static void ripple_onlinerefresh_shardingnet_wpacketsadd_data(ripple_netclient* netclient, uint64 offset, char* read_buffer, size_t read_size)
{
    uint32 wmsglen = 0;

    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;

    wmsglen = RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA_FIXSIZE + read_size;

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, wmsglen);

    /* 写数据 */
    /* offset */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, offset);
    /* schema */
    rmemcpy1(wuptr, 0, read_buffer, read_size);
    rmemset0(read_buffer, 0, '\0', RIPPLE_FILE_BUFFER_SIZE + RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA_FIXSIZE);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 创建 endrefresh 数据包挂载到 wpackets 上 */
static void ripple_onlinerefresh_shardingnet_wpacketsadd_end(ripple_netclient* netclient)
{
    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;
    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END_SIZE);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END_SIZE;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END_SIZE);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 重置待发送文件信息 */
static bool ripple_onlinerefresh_shardingnet_resetfileinfo(ripple_refresh_pumpshardingnetstate* pnet,
                                                        ripple_refresh_table_sharding* tblsharding)
{
    elog(RLOG_DEBUG, "onlinerefresh refresh worker: %s.%s %4d %4d",
                    tblsharding->schema,
                    tblsharding->table,
                    tblsharding->shardings,
                    tblsharding->sharding_no);

    /* 组装文件名称 */
    snprintf(pnet->filepath,
            RIPPLE_ABSPATH,
            "%s/%s/%s_%s/%s/%s_%s_%d_%d", 
            pnet->refeshpath,
            RIPPLE_REFRESH_REFRESH,
            tblsharding->schema,
            tblsharding->table,
            RIPPLE_REFRESH_COMPLETE,
            tblsharding->schema,
            tblsharding->table,
            tblsharding->shardings,
            tblsharding->sharding_no);

    pnet->fileoffset = 0;

    if(-1 != pnet->fd)
    {
        FileClose(pnet->fd);
        pnet->fd = -1;
    }

    /* 打开文件 */
    pnet->fd = FileOpen(pnet->filepath, O_RDONLY, 0);
    if(0 < pnet->fd)
    {
        pnet->filesize = FileSize(pnet->fd);
        lseek(pnet->fd, 0, SEEK_SET);
        return true;
    }

    /* 打开文件失败, 有两种场景会出现:
     * 1、描述符达到了系统的上限
     * 2、逻辑上出现了问题
     * 
     * 处理方案:
     *  将任务重新放入到队列中
     */
    elog(RLOG_WARNING, "can't openfile: %s, %s, please check!", pnet->filepath, strerror(errno));
    return false;
}


void* ripple_onlinerefresh_shardingnet_main(void* args)
{
    int timeout                                         = 0;
    size_t read_size                                    = 0;
    char* uuid                                          = NULL;
    void* qitem_data                                    = NULL;
    uint8* read_buffer                                  = NULL;                       /* 待发送数据缓存区 */
    ripple_netclient* netclient                         = NULL;
    ripple_refresh_table_sharding *shard                = NULL;
    ripple_refresh_pumpshardingnetstate* clientstate    = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_onlinerefresh_shardingnet *shard_slot        = NULL;
    struct stat st;

    thrnode = (ripple_thrnode *)args;
    shard_slot = (ripple_onlinerefresh_shardingnet *)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh pump sharding net stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 消息中记录的数据长度,默认设置为头长度 */
    clientstate = ripple_refresh_pumpshardingnet_init();
    if(NULL == clientstate)
    {
        elog(RLOG_WARNING, "onlinerefresh pump init sharding net error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* refresh 目录 */
    uuid = uuid2string(&shard_slot->onlinerefreshno);
    snprintf(clientstate->refeshpath,
            RIPPLE_MAXPATH, "%s/%s/%s",
            guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
            RIPPLE_REFRESH_ONLINEREFRESH,
            uuid);
    rfree(uuid);
    netclient = (ripple_netclient*)clientstate;

    /* 申请空间，用于读数据 */
    read_buffer = rmalloc0(RIPPLE_FILE_BUFFER_SIZE + RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA_FIXSIZE);
    if(NULL == read_buffer)
    {
        elog(RLOG_WARNING, "out of memory");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }
    rmemset0(read_buffer, 0, '\0', RIPPLE_FILE_BUFFER_SIZE + RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA_FIXSIZE);

    while(clientstate->state == RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_NOP)
    {
        /* 退出 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 连接服务端 */
        if(RIPPLE_NETCLIENTCONN_STATUS_NOP == clientstate->base.status)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_IDLE;
            if(false == ripple_netclient_tryconn(netclient))
            {
                elog(RLOG_INFO, "can not connect collector");
                sleep(1);
                continue;
            }
            elog(RLOG_INFO, "connect collecotr success");
        }

        /* 添加身份验证 */
        if (false == ripple_onlinerefresh_shardingnet_p2cidentity(netclient, shard_slot->onlinerefreshno.data))
        {
            elog(RLOG_INFO, "add onlinerefresh refresh identity error");
            sleep(1);
            continue;
        }

        /* 设置状态为等待返回身份标识 */
        if (RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_NOP == clientstate->state)
        {
            clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_IDENTITY;
        }
        else
        {
            continue;
        }

        while (true)
        {
            /* 退出 */
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                break;
            }

            /* 处理数据包 */
            if(false == ripple_netclient_desc(netclient))
            {
                /* 数据发送失败, 重置所有的状态 */
                clientstate->base.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
                clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_NOP;
                if (NULL != qitem_data)
                {
                    /* 因为是一个任务, 所以不需要关注加入到队列的尾部还是头部 */
                    ripple_queue_put(shard_slot->taskqueue, qitem_data);
                    qitem_data = NULL;
                }
                ripple_onlinerefresh_shardingnet_setreset(shard_slot);
                ripple_netclient_reset(netclient);
                break;
            }

            /* 当状态为IDLE 开始添加refresh信息 */
            if (RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_IDLE == clientstate->state)
            {
                /* 一个文件处理完成后清理内存 */
                if (NULL != qitem_data)
                {
                    /* 接收到end先删除文件 */
                    if(0 == stat(clientstate->filepath, &st))
                    {
                        durable_unlink(clientstate->filepath, RLOG_DEBUG);
                    }

                    /* 检查状态, 判断是否需要删除文件夹 */
                    ripple_refreshtablesyncstats_markstatdone(shard, shard_slot->syncstats, clientstate->refeshpath);

                    /* 重置 */
                    ripple_refresh_table_sharding_free(shard);
                    shard = NULL;
                }

                /* 获取数据 */
                qitem_data = ripple_queue_get(shard_slot->taskqueue, &timeout);
                if(NULL == qitem_data)
                {
                    if(RIPPLE_ERROR_TIMEOUT == timeout)
                    {
                        clientstate->base.hbtimeout += 1000;
                        clientstate->base.timeout += 1000;
                        if (RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                        {
                            continue;
                        }
                        thrnode->stat = RIPPLE_THRNODE_STAT_IDLE;
                        continue;
                    }

                    /* 出错了 */
                    elog(RLOG_WARNING, "pump onlinerefresh job get file from queue error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_onlinerefresh_shardingnet_main_done;
                }

                /* 获取到了任务 */
                thrnode->stat = RIPPLE_THRNODE_STAT_WORK;
                shard = (ripple_refresh_table_sharding *)qitem_data;
                if(false == ripple_onlinerefresh_shardingnet_resetfileinfo(clientstate, shard))
                {
                    /* 睡眠一秒, 防止出现打印大量日志 */
                    sleep(1);
                    if (NULL != qitem_data)
                    {
                        ripple_queue_put(shard_slot->taskqueue, qitem_data);
                        qitem_data = NULL;
                    }
                    continue;
                }
                clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_SHARDING;
            }

            /* 添加beginrefresh消息 */
            if (RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_SHARDING == clientstate->state)
            {
                ripple_onlinerefresh_shardingnet_wpacketsadd_begin(netclient,
                                                                   shard->schema,
                                                                   shard->table,
                                                                   shard->shardings,
                                                                   shard->sharding_no,
                                                                   shard_slot->onlinerefreshno.data);
                clientstate->base.hbtimeout = 0;
                clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_BEGIN;
            }

            /* 读取数据 */
            if (RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_READFILE == clientstate->state)
            {
                read_size = (clientstate->filesize > RIPPLE_FILE_BUFFER_SIZE) ? RIPPLE_FILE_BUFFER_SIZE : clientstate->filesize;

                /* 文件结束处理 */
                if (0 == clientstate->filesize)
                {
                    /* 关闭文件 */
                    FileClose(clientstate->fd);
                    clientstate->fd = -1;
                    ripple_onlinerefresh_shardingnet_wpacketsadd_end(netclient);
                    clientstate->base.hbtimeout = 0;
                    clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_END;
                }
                else
                {
                    elog(RLOG_DEBUG, "pump onlinerefresh refresh job, queue: %s.%s %4d %4d, file size: %u, read/send size: %u",
                                                            shard->schema,
                                                            shard->table,
                                                            shard->shardings,
                                                            shard->sharding_no,
                                                            clientstate->filesize,
                                                            read_size);
                    /* 无法读取文件的情况下, 报错退出 */
                    if (FileRead(clientstate->fd, (char *)read_buffer, read_size) < 0)
                    {
                        FileClose(clientstate->fd);
                        clientstate->fd = -1;
                        if (NULL != qitem_data)
                        {
                            ripple_queue_put(shard_slot->taskqueue, qitem_data);
                            qitem_data = NULL;
                        }
                        elog(RLOG_WARNING, "can't read file: %s, please check!", clientstate->filepath);
                        clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_NOP;
                        ripple_netclient_reset(netclient);
                        break;
                    }
                    elog(RLOG_DEBUG, "pump refresh worker, queue: %s.%s %4d %4d, send data",
                                                                shard->schema,
                                                                shard->table,
                                                                shard->shardings,
                                                                shard->sharding_no);
                    clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_SENDDATA;
                }
            }

            /* 添加要发送数据 */
            if (RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_SENDDATA == clientstate->state)
            {
                ripple_onlinerefresh_shardingnet_wpacketsadd_data(netclient,
                                                            clientstate->fileoffset,
                                                            (char *)read_buffer,
                                                            read_size);
                clientstate->base.hbtimeout = 0;
                clientstate->filesize -= read_size;
                clientstate->fileoffset += read_size;
                clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_READFILE;
            }

            /* 心跳包组装并发送*/
            if(RIPPLE_NET_PUMP_HBTIME <= clientstate->base.hbtimeout)
            {
                ripple_netclient_wpacketsadd_hb(netclient);
                clientstate->base.hbtimeout = 0;
            }

        }
    }

ripple_onlinerefresh_shardingnet_main_done:
    ripple_refresh_pumpshardingnet_destroy(clientstate);
    rfree(read_buffer);
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_shardingnet_free(void* args)
{
    ripple_onlinerefresh_shardingnet *shard_slot = NULL;
    shard_slot = (ripple_onlinerefresh_shardingnet *)args;

    if (!shard_slot)
    {
        return;
    }

    rfree(shard_slot);
}
