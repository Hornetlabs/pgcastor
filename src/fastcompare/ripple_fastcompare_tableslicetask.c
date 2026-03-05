#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/conn/ripple_conn.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "works/dyworks/ripple_dyworks.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tableslice.h"

/* 网络返回值函数处理流程 */
/*  回调函数处理接收到的信息 */
static bool ripple_fastcompare_tableslicetask_packets_handler(void* netclient, ripple_netpacket* netpacket)
{
    uint8* uptr = NULL;
    uint32 msgtype = RIPPLE_NETMSG_TYPE_NOP;
    uint32 len = 0;
    ripple_netclient* clientstate = NULL;
    ripple_fastcompare_tableslicetask* tableslicetask = NULL;
    uint8 *result = NULL;

    RIPPLE_UNUSED(clientstate);
    RIPPLE_UNUSED(msgtype);

    /* 强转 */
    tableslicetask = (ripple_fastcompare_tableslicetask*)(((char*)netclient) - (offsetof(ripple_fastcompare_tableslicetask, client)));

    clientstate = (ripple_netclient*)netclient;
    uptr = netpacket->data;

    msgtype = RIPPLE_CONCAT(get, 32bit)(&uptr);
    len = RIPPLE_CONCAT(get, 32bit)(&uptr);

    result = rmalloc0(sizeof(uint8) * len);
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(uint8) * len);

    rmemcpy0(result, 0, netpacket->data, sizeof(uint8) * len);

    tableslicetask->netresult = result;

    return true;
}

ripple_fastcompare_tableslicetask *ripple_fastcompare_tableslicetask_init(void)
{
    ripple_fastcompare_tableslicetask *result = NULL;
    char *host = guc_getConfigOption("host");

    result = rmalloc0(sizeof(ripple_fastcompare_tableslicetask));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_tableslicetask));

    result->conninfo = rstrdup(guc_getConfigOption("url"));
    result->task.type = RIPPLE_TASK_TYPE_FASTCOMPARE_TABLESLICE;
    rmemcpy1(result->client.svrhost, 0, host, (strlen(host)>16) ? 16 : strlen(host));
    sprintf(result->client.svrport, "%d", guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT));

    result->client.fd = -1;

    result->client.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);
    /* 申请 base 信息，用于后续的描述符处理 */
    if(false == result->client.ops->create(&result->client.base))
    {
        elog(RLOG_ERROR, "RIPPLE_NETIOMP_TYPE_POLL create error");
    }

    ripple_netclient_reset(&result->client);

    /* 设置回调 */
    result->client.callback = ripple_fastcompare_tableslicetask_packets_handler;
    return result;
}

/* 线程处理入口 */
void *ripple_fastcompare_tableslicetask_main(void* args)
{
    ripple_dyworks_node* dyworknode = NULL;
    ripple_fastcompare_tableslicetask* tableslicetask = NULL;
    ripple_fastcompare_tableslice* slice = NULL;
    ripple_task_slot *slot = NULL;

    dyworknode = (ripple_dyworks_node *)args;
    slot = (ripple_task_slot *) dyworknode->data;
    slot->stat = RIPPLE_TASKSLOT_WORK;
    tableslicetask = (ripple_fastcompare_tableslicetask *)slot->task;

    /* 
     * 连接到目标端
     * 
     */
    while(1)
    {

        if (slot->stat == RIPPLE_TASKSLOT_TERM)
        {
            break;
        }

        if (!slice)
        {
            /* 获取数据 */
            slice = ripple_queue_get(tableslicetask->slicequeue, NULL);
        }

        if (!slice)
        {
            if (slot->stat == RIPPLE_TASKSLOT_TERM)
            {
                break;
            }

            slot->stat = RIPPLE_TASKSLOT_IDLE;
            continue;
        }

        slot->stat = RIPPLE_TASKSLOT_WORK;

        if(RIPPLE_NETCLIENTCONN_STATUS_NOP == tableslicetask->client.status)
        {
            /* 连接目标端 */
            if(false == ripple_netclient_conn(&tableslicetask->client))
            {
                /* 未连接, 睡眠固定时间，重试 */
                sleep(1);
                continue;
            }
        }

        /* 检测连接状态 */
        if(RIPPLE_NETCLIENTCONN_STATUS_INPROCESS == tableslicetask->client.status)
        {
            if(false == ripple_netclient_isconnect(&tableslicetask->client))
            {
                /* 未连接, 睡眠固定时间，重试 */
                tableslicetask->client.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
                sleep(1);
                continue;
            }
        }

        /* 打开数据库链接 */
        if (!tableslicetask->chunkconn)
        {
            tableslicetask->chunkconn = ripple_conn_get(tableslicetask->conninfo);
            if (!tableslicetask->chunkconn)
            {
                dyworknode->status = RIPPLE_WORK_STATUS_EXIT;
                ripple_pthread_exit(NULL);
                return NULL;
            }
        }

        if (!tableslicetask->dataconn)
        {
            tableslicetask->dataconn = ripple_conn_get(tableslicetask->conninfo);
            if (!tableslicetask->dataconn)
            {
                dyworknode->status = RIPPLE_WORK_STATUS_EXIT;
                ripple_pthread_exit(NULL);
                return NULL;
            }
        }

        /* 处理数据 */
        ripple_fastcompare_tableslice_Slice2Chunk(tableslicetask, slice);

        slice = NULL;

        /* 重置 */
        ripple_netclient_reset(&tableslicetask->client);

        /* 设置回调 */
        tableslicetask->client.callback = ripple_fastcompare_tableslicetask_packets_handler;

    }
    slot->stat = RIPPLE_TASKSLOT_EXIT;
    dyworknode->status = RIPPLE_WORK_STATUS_EXIT;
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_fastcompare_tableslicetask_free(void* privdata)
{
    // todo liuzihe
    RIPPLE_UNUSED(privdata);
}
