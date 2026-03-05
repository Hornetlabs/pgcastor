#ifndef _RIPPLE_NETMSG_C2PHB_H
#define _RIPPLE_NETMSG_C2PHB_H

/* 
 * 接收来自collector的heartbeat
 *  pump 处理
 */
bool ripple_netmsg_c2phb(void* privdata, uint8* msg);

#endif
