#include "app_incl.h"
#include "net/netpacket/netpacket.h"

/* 初始化netpacket */
netpacket* netpacket_init(void)
{
    netpacket* net_packet = NULL;
    net_packet = (netpacket*)rmalloc0(sizeof(netpacket));
    if (NULL == net_packet)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(net_packet, 0, 0, sizeof(netpacket));

    net_packet->offset = 0;
    net_packet->used = 0;
    net_packet->max = 0;
    net_packet->data = NULL;

    return net_packet;
}

/* 按长度申请netpacket data空间 */
uint8* netpacket_data_init(int len)
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
void netpacket_destroyvoid(void* args)
{
    netpacket_destroy((netpacket*)args);
    return;
}

/* netpacket资源回收 */
void netpacket_destroy(netpacket* net_packet)
{
    if (NULL == net_packet)
    {
        return;
    }
    
    if (net_packet->data)
    {
        rfree(net_packet->data);
    }

    rfree(net_packet);

    return;
}
