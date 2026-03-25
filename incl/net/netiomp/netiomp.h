#ifndef _NETIOMP_H
#define _NETIOMP_H

typedef enum NETIOMP_TYPE
{
    NETIOMP_TYPE_NOP = 0x00,
    NETIOMP_TYPE_SELECT = 0x01,
    NETIOMP_TYPE_POLL = 0x02,
    NETIOMP_TYPE_EPOLL = 0x03
} netiomp_type;

#define MAXFD 8

typedef struct NETIOMPBASE
{
    int            num;
    int            max;
    int            timeout;
    struct pollfd* events;
} netiompbase;

/* Reset */
typedef void (*rnetiompreset)(netiompbase* base);

/* Create */
typedef bool (*rnetiompcreate)(netiompbase** base);

/* Add descriptor and event type */
typedef int (*rnetiompadd)(netiompbase* base, int fd, uint16 flag);

/* Delete descriptor */
typedef int (*rnetiompdel)(netiompbase* base, int fd);

/* Modify descriptor */
typedef int (*rnetiompmodify)(netiompbase* base, int fd);

/* Event listening */
typedef int (*rnetiomp)(netiompbase* base);

/* Get event */
typedef int (*rnetiompgetevent)(netiompbase* base, int pos);

/* MemoryReclaim */
typedef void (*rnetiompfree)(netiompbase* base);

typedef struct NETIOMPOPS
{
    netiomp_type     type;
    rnetiompreset    reset;
    rnetiompcreate   create;
    rnetiompadd      add;
    rnetiompdel      del;
    rnetiompmodify   modify;
    rnetiomp         iomp;
    rnetiompgetevent getevent;
    rnetiompfree     free;
} netiompops;

netiompops* netiomp_init(int type);

#endif
