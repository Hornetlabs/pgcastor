#ifndef _NETIOMP_POLL_H
#define _NETIOMP_POLL_H

/* Reset */
void netiomp_pollreset(netiompbase* base);

/* Create function */
bool netiomp_pollcreate(netiompbase** refbase);

int netiomp_polladd(netiompbase* base, int fd, uint16 flag);

int netiomp_polldel(netiompbase* base, int fd);

int netiomp_pollmodify(netiompbase* base, int fd);

int netiomp_poll(netiompbase* base);

/* Get triggered event type */
int netiomp_getevent(netiompbase* base, int pos);

void netiomp_free(netiompbase* base);

#endif
