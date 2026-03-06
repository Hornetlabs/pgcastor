#ifndef _RIPPLE_NETMSG_H
#define _RIPPLE_NETMSG_H

typedef enum RIPPLE_NETMSG_TYPE
{
    /* 保留 */
    RIPPLE_NETMSG_TYPE_NOP                  = 0x00,

    /* 消息类型在此之前添加 */
    RIPPLE_NETMSG_TYPE_MAX

} ripple_netmsg_type;

/* 网络协议头部固定长度 */
#define RIPPLE_NETMSG_TYPE_HDR_SIZE                     8

/* 消息分发处理 */
bool ripple_netmsg(void* privdata,
                    uint32 msgtype,
                    uint8* msg);

#endif
