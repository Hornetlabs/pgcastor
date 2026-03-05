#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "queue/ripple_queue.h"
#include "misc/ripple_misc_stat.h"
#include "storage/ripple_file_buffer.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netmsg/ripple_netmsg_c2phb.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "metric/pump/ripple_statework_pump.h"

/* 
 * 接收来自collector的heartbeat
 *  pump 处理
 */
bool ripple_netmsg_c2phb(void* privdata, uint8* msg)
{
    /*
     * 更新地址信息
     */
    uint64 pfileid = 0;
    uint64 cfileid = 0;
    uint8* uptr = NULL;
    int8 type = 0;
    ripple_increment_pumpnetstate* clientstate = NULL;

    uptr = msg;

    /* 偏移出头部位置 */
    uptr += RIPPLE_NETMSG_TYPE_HDR_SIZE;

    /* trail和refresh区分 */
    type = RIPPLE_CONCAT(get, 8bit)(&uptr);

    if(RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT == type)
    {
        clientstate = (ripple_increment_pumpnetstate*)privdata;

        /* collector 端已经写入到 Trail 文件中的对应的 pump 端的 Trail 文件编号 */
        pfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);

        /* collector 端正在写的 Trail 文件中编号 */
        cfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);

        clientstate->callback.setmetricloadtrailno(clientstate->privdata, pfileid);

        elog(RLOG_DEBUG, "recv collector 2 pump heartbeat, %lu.%lu, %lu",
                                                            pfileid,
                                                            clientstate->recpos.trail.offset,
                                                            cfileid);
    }
    else if (RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_INC == type)
    {
         clientstate = (ripple_increment_pumpnetstate*)privdata;

        /* collector 端已经写入到 Trail 文件中的对应的 pump 端的 Trail 文件编号 */
        pfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);

        /* collector 端正在写的 Trail 文件中编号 */
        cfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);

        elog(RLOG_DEBUG, "recv collector 2 pump heartbeat, %lu.%lu, %lu",
                                                            pfileid,
                                                            clientstate->recpos.trail.offset,
                                                            cfileid);
    }

    return true;
}
