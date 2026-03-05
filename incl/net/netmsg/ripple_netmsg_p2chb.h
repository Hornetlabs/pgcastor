#ifndef _RIPPLE_NETMSG_P2CHB_H
#define _RIPPLE_NETMSG_P2CHB_H

/* 
 * 接收来自pump的heartbeat
 *  collector 处理
 */
bool ripple_netmsg_p2chb(void* privdata, uint8* msg);

#endif
