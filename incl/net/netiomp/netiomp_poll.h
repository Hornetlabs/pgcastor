#ifndef _RIPPLE_NETIOMP_POLL_H
#define _RIPPLE_NETIOMP_POLL_H

/* 重置 */
void ripple_netiomp_pollreset(ripple_netiompbase* base);

/* 创建函数 */
bool ripple_netiomp_pollcreate(ripple_netiompbase** refbase);

int ripple_netiomp_polladd(ripple_netiompbase* base, int fd, uint16 flag);

int ripple_netiomp_polldel(ripple_netiompbase* base, int fd);

int ripple_netiomp_pollmodify(ripple_netiompbase* base, int fd);

int ripple_netiomp_poll(ripple_netiompbase* base);

/* 获取触发事件的类型 */
int ripple_netiomp_getevent(ripple_netiompbase* base, int pos);

void ripple_netiomp_free(ripple_netiompbase* base);

#endif
