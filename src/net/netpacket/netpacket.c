#include "ripple_app_incl.h"
#include "net/netpacket/ripple_netpacket.h"

/* 初始化netpacket */
ripple_netpacket* ripple_netpacket_init(void)
{
    ripple_netpacket* netpacket = NULL;
    netpacket = (ripple_netpacket*)rmalloc0(sizeof(ripple_netpacket));
    if (NULL == netpacket)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(netpacket, 0, 0, sizeof(ripple_netpacket));

    netpacket->offset = 0;
    netpacket->used = 0;
    netpacket->max = 0;
    netpacket->data = NULL;

    return netpacket;
}

/* 按长度申请netpacket data空间 */
uint8* ripple_netpacket_data_init(int len)
{
    uint8* data = NULL;
    if (0 == len)
    {
        return NULL;
    }

    data = (uint8*)rmalloc0(len);
    if (NULL == data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(data, 0, '\0', len);

    return data;
}

/* 释放队列中的netpacket */
void ripple_netpacket_destroyvoid(void* args)
{
    ripple_netpacket_destroy((ripple_netpacket*)args);
    return;
}

/* netpacket资源回收 */
void ripple_netpacket_destroy(ripple_netpacket* netpacket)
{
    if (NULL == netpacket)
    {
        return;
    }
    
    if (netpacket->data)
    {
        rfree(netpacket->data);
    }

    rfree(netpacket);

    return;
}
