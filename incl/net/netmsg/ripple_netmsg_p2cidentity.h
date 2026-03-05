#ifndef _RIPPLE_NETMSG_P2CIDENTITY_H
#define _RIPPLE_NETMSG_P2CIDENTITY_H

/* 
 * 接收来自pump的addr请求
 *  collector 处理
 */
bool ripple_netmsg_p2cidentity(void* privdata, uint8* msg);

#endif
