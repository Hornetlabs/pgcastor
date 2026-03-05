#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "queue/ripple_queue.h"
#include "port/net/ripple_net.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "works/network/ripple_refresh_pumpshardingnet.h"
#include "net/netmsg/ripple_netmsg_c2pbeginrefresh.h"

/* 
 * 接收来自collector的beginrefres请求
 *  pump 处理
 */
bool ripple_netmsg_c2pbeginrefresh(void* privdata, uint8* msg)
{
    /*
     * 更新地址信息
     */
    uint32 msgtype = 0;
    uint8* uptr = NULL;
    ripple_refresh_pumpshardingnetstate* clientstate = NULL;

    clientstate = (ripple_refresh_pumpshardingnetstate*)privdata;

    uptr = msg;

    msgtype = RIPPLE_CONCAT(get, 32bit)(&uptr);

    if (RIPPLE_NETMSG_TYPE_C2P_BEGINREFRESH == msgtype)
    {
        /* 接收到beginrefresh设置状态为READFILE开始读数据 */
        clientstate->state = RIPPLE_REFRESH_PUMPSHARDINGNETSTATE_STATE_READFILE;
    }
    else
    {
        elog(RLOG_WARNING, "get invalid msg from connector:%d", msgtype);
        return false;
    }
    return true;
}
