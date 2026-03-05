#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "port/net/ripple_net.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netmsg/ripple_netmsg_p2chb.h"

/* 
 * 接收来自pump的heartbeat
 *  collector 处理
 */
bool ripple_netmsg_p2chb(void* privdata, uint8* msg)
{
    /*
     * 不需要做处理
     */
    elog(RLOG_DEBUG, "recv pump 2 collector heartbeat");
    return true;
}
