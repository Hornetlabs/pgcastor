#ifndef _NETIOMP_POLL_H
#define _NETIOMP_POLL_H

/* 重置 */
void netiomp_pollreset(netiompbase* base);

/* 创建函数 */
bool netiomp_pollcreate(netiompbase** refbase);

int netiomp_polladd(netiompbase* base, int fd, uint16 flag);

int netiomp_polldel(netiompbase* base, int fd);

int netiomp_pollmodify(netiompbase* base, int fd);

int netiomp_poll(netiompbase* base);

/* 获取触发事件的类型 */
int netiomp_getevent(netiompbase* base, int pos);

void netiomp_free(netiompbase* base);

#endif
