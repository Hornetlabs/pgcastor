#include "app_incl.h"
#include "port/net/net.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"

/* Reset */
void netiomp_pollreset(netiompbase* base)
{
    if (NULL == base)
    {
        return;
    }
    base->num = 0;

    rmemset1(base->events, 0, '\0', sizeof(struct pollfd) * base->max);
    return;
}

/* Create function */
bool netiomp_pollcreate(netiompbase** refbase)
{
    netiompbase* base = NULL;

    base = (netiompbase*)rmalloc1(sizeof(netiompbase));
    if (NULL == base)
    {
        return false;
    }
    rmemset0(base, 0, '\0', sizeof(netiompbase));

    base->num = 0;
    base->max = MAXFD;
    base->events = (struct pollfd*)rmalloc0(sizeof(struct pollfd) * base->max);
    if (NULL == base->events)
    {
        return false;
    }
    *refbase = base;
    return true;
}

/*
 * Add descriptor and set listening type
 * Parameter description:
 *  base        Add fd to base
 *  fd          Event descriptor to listen
 *  flag        Event to listen
 *
 * Return value:
 *  -1          Error
 *  > 0         Position information
 */
int netiomp_polladd(netiompbase* base, int fd, uint16 flag)
{
    int pos = 0;
    int nnum = 0;
    nnum = base->max;

    while (base->num >= nnum)
    {
        nnum *= 2;
    }

    if (nnum != base->max)
    {
        base->events = rrealloc0(base->events, sizeof(struct pollfd) * nnum);
        base->max = nnum;
    }

    base->events[base->num].fd = fd;
    base->events[base->num].revents = 0;
    base->events[base->num].events |= flag;

    pos = base->num;
    base->num++;

    return pos;
}

/* Delete descriptor, not used in this model */
int netiomp_polldel(netiompbase* base, int fd)
{
    return 0;
}

/* Modify descriptor, not used in this model */
int netiomp_pollmodify(netiompbase* base, int fd)
{
    return 0;
}

/* Listen event */
int netiomp_poll(netiompbase* base)
{
    return poll(base->events, base->num, base->timeout);
}

/* Get triggered event type */
int netiomp_getevent(netiompbase* base, int pos)
{
    return base->events[pos].revents;
}

void netiomp_free(netiompbase* base)
{
    if (NULL == base)
    {
        return;
    }

    if (NULL != base->events)
    {
        rfree(base->events);
        base->events = NULL;
    }

    base->max = 0;
    base->num = 0;
    rfree(base);
    return;
}
