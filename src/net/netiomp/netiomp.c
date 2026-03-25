#include "app_incl.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"

static netiompops m_netiomppollops = {.type = NETIOMP_TYPE_POLL,
                                      .reset = netiomp_pollreset,
                                      .create = netiomp_pollcreate,
                                      .add = netiomp_polladd,
                                      .del = netiomp_polldel,
                                      .modify = netiomp_pollmodify,
                                      .iomp = netiomp_poll,
                                      .getevent = netiomp_getevent,
                                      .free = netiomp_free};

netiompops* netiomp_init(int type)
{
    switch (type)
    {
        case NETIOMP_TYPE_POLL:
            return &m_netiomppollops;
        default:
            elog(RLOG_ERROR, "unsuppor net iomp %d", type);
    }

    return NULL;
}
