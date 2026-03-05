#include "ripple_app_incl.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"

static ripple_netiompops        m_netiomppollops =
{
    .type = RIPPLE_NETIOMP_TYPE_POLL,
    .reset = ripple_netiomp_pollreset,
    .create = ripple_netiomp_pollcreate,
    .add = ripple_netiomp_polladd,
    .del = ripple_netiomp_polldel,
    .modify = ripple_netiomp_pollmodify,
    .iomp = ripple_netiomp_poll,
    .getevent = ripple_netiomp_getevent,
    .free = ripple_netiomp_free
};

ripple_netiompops* ripple_netiomp_init(int type)
{
    switch (type)
    {
    case RIPPLE_NETIOMP_TYPE_POLL:
        return &m_netiomppollops;
    default:
        elog(RLOG_ERROR, "unsuppor net iomp %d", type);
    }

    return NULL;
}
