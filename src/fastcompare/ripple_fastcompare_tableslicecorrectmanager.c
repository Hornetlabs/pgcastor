#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/ripple_conn.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "net/netmsg/ripple_netmsg.h"
#include "works/dyworks/ripple_dyworks.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"
#include "fastcompare/ripple_fastcompare_dispatchmsg.h"

/*  回调函数处理接收到的信息 */
static bool ripple_fastcompare_tableslicecorrectmanager_packets_handler(void* netclient, ripple_netpacket* netpacket)
{
    ripple_fastcompare_tableslicecorrectmanager* tableslicecorrmgr = NULL;

    /* 强转 */
    tableslicecorrmgr = (ripple_fastcompare_tableslicecorrectmanager*)netclient;

    ripple_fastcompare_dispatchmsg_netmsg((void*) tableslicecorrmgr, netpacket->data);

    return true;
}

/* 初始化 */
ripple_fastcompare_tableslicecorrectmanager* riple_fastcompare_tableslicecorrectmanager_init(void)
{
    ripple_fastcompare_tableslicecorrectmanager* tableslicecorrmgr = NULL;

    tableslicecorrmgr = (ripple_fastcompare_tableslicecorrectmanager*)rmalloc0(sizeof(ripple_fastcompare_tableslicecorrectmanager));
    if(NULL == tableslicecorrmgr)
    {
        return false;
    }
    rmemset0(tableslicecorrmgr, 0, '\0', sizeof(ripple_fastcompare_tableslicecorrectmanager));

    tableslicecorrmgr->base.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);
    /* 申请 base 信息，用于后续的描述符处理 */
    if(false == tableslicecorrmgr->base.ops->create(&tableslicecorrmgr->base.base))
    {
        elog(RLOG_ERROR, "RIPPLE_NETIOMP_TYPE_POLL create error");
    }

    tableslicecorrmgr->base.fd = -1;

    ripple_netclient_reset(&tableslicecorrmgr->base);
    tableslicecorrmgr->conninfo = guc_getConfigOption(RIPPLE_CFG_KEY_URL);
    tableslicecorrmgr->correctslice = ripple_fastcompare_tablecorrectslice_init();
    tableslicecorrmgr->datacompare = ripple_fastcompare_init_datacompare();
    tableslicecorrmgr->state = RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_INIT;
    tableslicecorrmgr->base.callback = ripple_fastcompare_tableslicecorrectmanager_packets_handler;
    return tableslicecorrmgr;
}

/* 主流程 */
void* riple_fastcompare_tableslicecorrectmanager_main(void* args)
{
    ripple_dyworks_node* dyworknode = NULL;
    ripple_fastcompare_tableslicecorrectmanager* tableslicecorrmgr = NULL;
    dyworknode = (ripple_dyworks_node *)args;

    tableslicecorrmgr = (ripple_fastcompare_tableslicecorrectmanager*)dyworknode->data;

    dyworknode->status = RIPPLE_WORK_STATUS_READY;

    dyworknode->status = RIPPLE_WORK_STATUS_WORK;

    tableslicecorrmgr->state = RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_WORKING;
    
    /* 
     * 接收数据并处理
     */
    while(1)
    {
        if(RIPPLE_WORK_STATUS_TERM == dyworknode->status)
        {
            dyworknode->status = RIPPLE_WORK_STATUS_EXIT;
            elog(RLOG_WARNING, "table slice correct exit");
            break;
        }

        /* 消息处理 */
        if(false == ripple_netclient_desc(&tableslicecorrmgr->base))
        {
            elog(RLOG_WARNING, "table slice correct exit 1");
            dyworknode->status = RIPPLE_WORK_STATUS_EXIT;
            break;
        }

        if (RIPPLE_FASTCOMPARE_TABLESLICECORRECTMANAGER_STATE_EXIT == tableslicecorrmgr->state)
        {
            dyworknode->status = RIPPLE_WORK_STATUS_EXIT;
            elog(RLOG_WARNING, "table slice correct exit 2");
            break;
        }
        usleep(50000);
    }

    /* TODO 资源回收 */

    ripple_pthread_exit(NULL);
    return NULL;
}

void riple_fastcompare_tableslicecorrectmanager_free(ripple_fastcompare_tableslicecorrectmanager* tableslicecorrmgr)
{
    if (NULL == tableslicecorrmgr)
    {
        return;
    }

    ripple_netclient_destroy((ripple_netclient *)&tableslicecorrmgr->base);

    if (tableslicecorrmgr->conn)
    {
        PQfinish(tableslicecorrmgr->conn);
        tableslicecorrmgr->conn = NULL;
    }

    if (tableslicecorrmgr->correctslice)
    {
        ripple_fastcompare_tablecorrectslice_free(tableslicecorrmgr->correctslice);
    }
    
    if (tableslicecorrmgr->datacompare)
    {
        ripple_fastcompare_datacompare_free(tableslicecorrmgr->datacompare);
    }

    rfree(tableslicecorrmgr);

}
