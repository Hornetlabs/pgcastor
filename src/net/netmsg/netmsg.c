#include "app_incl.h"
#include "net/netmsg/netmsg.h"

typedef bool (*netmsgs_op)(void* privdata, uint8* msg);

typedef struct NETMSGS
{
    int        type; /* Message type */
    char*      desc; /* Message description */
    netmsgs_op func; /* Message processing */
} netmsgs;

static netmsgs m_netmsgsops[] = {{NETMSG_TYPE_NOP, "NOP", NULL}, {NETMSG_TYPE_MAX, " MAX ", NULL}};

/* Message dispatch processing */
bool netmsg(void* privdata, uint32 msgtype, uint8* msg)
{
    if (NETMSG_TYPE_MAX < msgtype)
    {
        elog(RLOG_WARNING, "unknown net msgtype:%u", msgtype);
        return false;
    }

    if (NULL == m_netmsgsops[msgtype].func)
    {
        elog(RLOG_WARNING, "unsupport net msgtype:%u", msgtype);
        return false;
    }

    return m_netmsgsops[msgtype].func(privdata, msg);
}
