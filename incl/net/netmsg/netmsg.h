#ifndef _NETMSG_H
#define _NETMSG_H

typedef enum NETMSG_TYPE
{
    /* Reserved */
    NETMSG_TYPE_NOP = 0x00,

    /* Message type added before this */
    NETMSG_TYPE_MAX

} netmsg_type;

/* Fixed length of network protocol header */
#define NETMSG_TYPE_HDR_SIZE 8

/* Message distribution processing */
bool netmsg(void* privdata, uint32 msgtype, uint8* msg);

#endif
