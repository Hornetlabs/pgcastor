#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "utils/uuid/ripple_uuid.h"
#include "misc/ripple_misc_stat.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netmsg/ripple_netmsg_p2cbigtxnbegin.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

/* 
 * 接收来自pump的heartbeat
 *  collector 处理
 */
bool ripple_netmsg_p2cbigtxnbegin(void* privdata, uint8* msg)
{
    uint64 xid = 0;
    uint8* uptr = NULL;
    StringInfo trailpath = NULL;
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_collectornetclient_bigtxn* bigtxn = NULL;

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;

    bigtxn = (ripple_collectornetclient_bigtxn*)nodesvrstate->data;

    uptr += RIPPLE_NETMSG_TYPE_HDR_SIZE;

    xid = RIPPLE_CONCAT(get, 64bit)(&uptr);

    elog(RLOG_WARNING, "p2c bigtxn begin, xid:%lu", xid);

    trailpath = makeStringInfo();

    bigtxn->xid = (FullTransactionId)xid;

    appendStringInfo(trailpath, "%s/%s/%lu",
                                bigtxn->trailpath,
                                RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                                bigtxn->xid);

    RemoveDir(trailpath->data);

    while(!DirExist(trailpath->data))
    {
        if(true == g_gotsigterm)
        {
            return true;
        }
        /* 创建目录 */
        MakeDir(trailpath->data);
    };

    deleteStringInfo(trailpath);

    elog(RLOG_INFO, "recv pump 2 collector bigtxn begin");
    return true;
}
