#ifndef _NETMSG_H
#define _NETMSG_H

typedef enum NETMSG_TYPE
{
    /* 保留 */
    NETMSG_TYPE_NOP                  = 0x00,

    /* 消息类型在此之前添加 */
    NETMSG_TYPE_MAX

} netmsg_type;

/* 网络协议头部固定长度 */
#define NETMSG_TYPE_HDR_SIZE                     8

/* 消息分发处理 */
bool netmsg(void* privdata,
                    uint32 msgtype,
                    uint8* msg);

#endif
