#include "app_incl.h"
#include "net/netmsg/netmsg.h"

typedef bool (*netmsgs_op)(void* privdata, uint8* msg);

typedef struct NETMSGS
{
    int                 type;                   /* 消息类型 */
    char*               desc;                   /* 消息描述 */
    netmsgs_op          func;                   /* 消息处理 */
} netmsgs;

static netmsgs   m_netmsgsops[] =
{
    {
        NETMSG_TYPE_NOP,
        "NOP",
        NULL
    },
    {
        NETMSG_TYPE_MAX,
        " MAX ",
        NULL
    }
};

/* 消息分发处理 */
bool netmsg(void* privdata,
                    uint32 msgtype,
                    uint8* msg)
{
    if(NETMSG_TYPE_MAX < msgtype)
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
