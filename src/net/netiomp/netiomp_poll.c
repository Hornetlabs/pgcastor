#include "ripple_app_incl.h"
#include "port/net/ripple_net.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"

/* 重置 */
void ripple_netiomp_pollreset(ripple_netiompbase* base)
{
    if(NULL == base)
    {
        return;
    }
    base->num = 0;

    rmemset1(base->events, 0, '\0', sizeof(struct pollfd) * base->max);
    return;
}

/* 创建函数 */
bool ripple_netiomp_pollcreate(ripple_netiompbase** refbase)
{
    ripple_netiompbase* base = NULL;

    base = (ripple_netiompbase*)rmalloc1(sizeof(ripple_netiompbase));
    if(NULL == base)
    {
        return false;
    }
    rmemset0(base, 0, '\0', sizeof(ripple_netiompbase));

    base->num = 0;
    base->max = RIPPLE_MAXFD;
    base->events = (struct pollfd*)rmalloc0(sizeof(struct pollfd)* base->max);
    if (NULL == base->events)
    {
        return false;
    }
    *refbase = base;
    return true;
}

/* 
 * 添加描述符并设置监听的类型
 * 参数说明:
 *  base        将fd加入到 base 中
 *  fd          监听的事件描述符
 *  flag        监听的事件
 * 
 * 返回值:
 *  -1          错误
 *  > 0         位置信息
 */
int ripple_netiomp_polladd(ripple_netiompbase* base, int fd, uint16 flag)
{
    int pos = 0;
    int nnum = 0;
    nnum = base->max;

    while(base->num >= nnum)
    {
        nnum *= 2;
    }

    if (nnum != base->max)
    {
       base->events = rrealloc0(base->events, sizeof(struct pollfd)*nnum);
       base->max = nnum;
    }

    base->events[base->num].fd = fd;
    base->events[base->num].revents = 0;
    base->events[base->num].events |= flag;

    pos = base->num;
    base->num++;

    return pos;
}

/* 删除描述符，在此模型中无用 */
int ripple_netiomp_polldel(ripple_netiompbase* base, int fd)
{
    return 0;
}

/* 修改描述符, 在此模型中无用 */
int ripple_netiomp_pollmodify(ripple_netiompbase* base, int fd)
{
    return 0;
}

/* 监听事件 */
int ripple_netiomp_poll(ripple_netiompbase* base)
{
    return ripple_poll(base->events, base->num, base->timeout);
}

/* 获取触发事件的类型 */
int ripple_netiomp_getevent(ripple_netiompbase* base, int pos)
{
    return base->events[pos].revents;
}

void ripple_netiomp_free(ripple_netiompbase* base)
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
