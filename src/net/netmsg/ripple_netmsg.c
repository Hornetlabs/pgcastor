#include "ripple_app_incl.h"
#include "net/netmsg/ripple_netmsg.h"

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
