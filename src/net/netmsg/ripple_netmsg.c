#include "ripple_app_incl.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netmsg/ripple_netmsg_p2cidentity.h"
#include "net/netmsg/ripple_netmsg_c2pidentity.h"
#include "net/netmsg/ripple_netmsg_p2chb.h"
#include "net/netmsg/ripple_netmsg_c2phb.h"
#include "net/netmsg/ripple_netmsg_p2cdata.h"
#include "net/netmsg/ripple_netmsg_p2cbeginrefresh.h"
#include "net/netmsg/ripple_netmsg_p2crefreshdata.h"
#include "net/netmsg/ripple_netmsg_p2cendrefresh.h"
#include "net/netmsg/ripple_netmsg_c2pbeginrefresh.h"
#include "net/netmsg/ripple_netmsg_c2pendrefresh.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cdata.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cshardingbegin.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cshardingend.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_c2pbeginrefresh.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_c2pendrefresh.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cshardingdata.h"
#include "net/netmsg/ripple_netmsg_p2cbigtxnbegin.h"
#include "net/netmsg/ripple_netmsg_p2cbigtxndata.h"
#include "net/netmsg/ripple_netmsg_p2cbigtxnend.h"

typedef bool (*netmsgs_op)(void* privdata, uint8* msg);

typedef struct RIPPLE_NETMSGS
{
    int                 type;                   /* 消息类型 */
    char*               desc;                   /* 消息描述 */
    netmsgs_op          func;                   /* 消息处理 */
} ripple_netmsgs;

static ripple_netmsgs   m_netmsgsops[] =
{
    {
        RIPPLE_NETMSG_TYPE_NOP,
        "NOP",
        NULL
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_HB,
        "pump 2 collector heartbeat",
        ripple_netmsg_p2chb
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_HB,
        "collector 2 pump heartbeat",
        ripple_netmsg_c2phb
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_IDENTITY,
        "pump 2 collector identity authentication",
        ripple_netmsg_p2cidentity
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_IDENTITY,
        "collector 2 pump identity authentication",
        ripple_netmsg_c2pidentity
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_DATA,
        "pump 2 collector trail file data",
        ripple_netmsg_p2cdata
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_BEGINREFRESH,
        "pump 2 collector begin refresh",
        ripple_netmsg_p2cbeginrefresh
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_BEGINREFRESH,
        "collector 2 pump begin refresh",
        ripple_netmsg_c2pbeginrefresh
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_REFRESHDATA,
        "pump 2 collector refresh file data",
        ripple_netmsg_p2crefreshdata
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_ENDREFRESH,
        "pump 2 collector end refresh",
        ripple_netmsg_p2cendrefresh
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_ENDREFRESH,
        "collector 2 pump end refresh",
        ripple_netmsg_c2pendrefresh
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA,
        "pump 2 collector onlinerefresh trail file data",
        ripple_netmsg_onlinerefresh_p2cdata
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_BEGIN,
        "pump 2 collector onlinerefresh begin refresh",
        ripple_netmsg_onlinerefresh_p2cshardingbegin
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN,
        "collector 2 pump onlinerefresh begin refresh",
        ripple_netmsg_onlinerefresh_c2pbeginrefresh
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA,
        "pump 2 collector onlinerefresh refresh file data",
        ripple_netmsg_onlinerefresh_p2cshardingdata
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END,
        "collector 2 pump onlinerefresh end refresh",
        ripple_netmsg_onlinerefresh_p2cshardingend
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END,
        "collector 2 pump onlinerefresh end refresh",
        ripple_netmsg_onlinerefresh_c2pendrefresh
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_BIGTXN_BEGIN,
        "pump 2 collector begin bigtxn",
        ripple_netmsg_p2cbigtxnbegin
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA,
        "pump 2 collector bigtxn file data",
        ripple_netmsg_p2cbigtxndata
    },
    {
        RIPPLE_NETMSG_TYPE_P2C_BIGTXN_END,
        "pump 2 collector end bigtxn",
        ripple_netmsg_p2cbigtxnend
    },
    {
        RIPPLE_NETMSG_TYPE_C2P_BIGTXN_END,
        "collector 2 pump end bigtxn",
        NULL
    },
    {
        RIPPLE_NETMSG_TYPE_MAX,
        " MAX ",
        NULL
    }
};

/* 消息分发处理 */
bool ripple_netmsg(void* privdata,
                    uint32 msgtype,
                    uint8* msg)
{
    if(RIPPLE_NETMSG_TYPE_MAX < msgtype)
    {
        elog(RLOG_WARNING, "unknown net msgtype:%u", msgtype);
        return false;
    }

    if(NULL == m_netmsgsops[msgtype].func)
    {
        elog(RLOG_WARNING, "unsupport net msgtype:%u", msgtype);
        return false;
    }

    return m_netmsgsops[msgtype].func(privdata, msg);
}
