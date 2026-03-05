#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "works/network/ripple_refresh_pumpshardingnet.h"

/* refresh pumpshardingnetstate初始化 */
ripple_refresh_pumpshardingnetstate* ripple_refresh_pumpshardingnet_init(void)
{
    char* host = NULL;
    ripple_refresh_pumpshardingnetstate *clientstate = NULL;

    clientstate = rmalloc0(sizeof(ripple_refresh_pumpshardingnetstate));
    if(NULL == clientstate)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(clientstate, 0, '\0', sizeof(ripple_refresh_pumpshardingnetstate));

    clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_NOP;
    clientstate->fd = -1;
    clientstate->filesize = 0;
    clientstate->fileoffset = 0;
    clientstate->base.fd = -1;

    /* 设置类型 */
    ripple_netclient_setprotocoltype(&clientstate->base, RIPPLE_NETCLIENT_PROTOCOLTYPE_IPTCP);

    /* 获取监听主机 */
    host = guc_getConfigOption(RIPPLE_CFG_KEY_HOST);
    rmemcpy1(clientstate->base.svrhost, 0, host, (strlen(host)>16) ? 16 : strlen(host));
    if(NULL == clientstate->base.svrhost
        || '\0' == clientstate->base.svrhost[0])
    {
        elog(RLOG_WARNING, "please configure host config options");
        return NULL;
    }

    /* 获取监听端口 */
    sprintf(clientstate->base.svrport, "%d", guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT));
    if(0 == clientstate->base.svrport)
    {
        elog(RLOG_WARNING, "please configure port config options");
        return NULL;
    }

    rmemcpy1(clientstate->base.szport, 0, clientstate->base.svrport, 128);

    elog(RLOG_DEBUG, "clientstate->base.port:%d, %s", clientstate->base.svrport, clientstate->base.szport);

    /* 设置使用的网络模型 */
    clientstate->base.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if(false == clientstate->base.ops->create(&clientstate->base.base))
    {
        elog(RLOG_WARNING, "RIPPLE_NETIOMP_TYPE_POLL create error");
        return NULL;
    }
    clientstate->base.hbtimeout = RIPPLE_NET_PUMP_HBTIME;
    clientstate->base.timeout = 0;
    clientstate->base.base->timeout = RIPPLE_NET_POLLTIMEOUT;
    clientstate->base.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    clientstate->base.wpackets = ripple_queue_init();
    clientstate->base.rpackets = ripple_queue_init();
    clientstate->base.callback = ripple_netclient_packets_handler;

    return clientstate;
}

/* 资源回收 */
void ripple_refresh_pumpshardingnet_destroy(ripple_refresh_pumpshardingnetstate* clientstate)
{
    if(NULL == clientstate)
    {
        return;
    }

    if(-1 != clientstate->fd)
    {
        FileClose(clientstate->fd);
        clientstate->fd = -1;
    }

    ripple_netclient_destroy((ripple_netclient*)&clientstate->base);

    rfree(clientstate);
    clientstate = NULL;
}
