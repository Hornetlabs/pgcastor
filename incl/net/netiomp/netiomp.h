#ifndef _RIPPLE_NETIOMP_H
#define _RIPPLE_NETIOMP_H


typedef enum RIPPLE_NETIOMP_TYPE
{
    RIPPLE_NETIOMP_TYPE_NOP         = 0x00,
    RIPPLE_NETIOMP_TYPE_SELECT      = 0x01,
    RIPPLE_NETIOMP_TYPE_POLL        = 0x02,
    RIPPLE_NETIOMP_TYPE_EPOLL       = 0x03
} ripple_netiomp_type;

#define RIPPLE_MAXFD                    8

typedef struct RIPPLE_NETIOMPBASE
{
    int                 num;
    int                 max;
    int                 timeout;
    struct pollfd*      events;
} ripple_netiompbase;

/* 重置 */
typedef void (*rnetiompreset)(ripple_netiompbase* base);

/* 创建 */
typedef bool (*rnetiompcreate)(ripple_netiompbase** base);

/* 增加描述符和事件类型 */
typedef int (*rnetiompadd)(ripple_netiompbase* base, int fd, uint16 flag);

/* 删除描述符 */
typedef int (*rnetiompdel)(ripple_netiompbase* base, int fd);

/* 修改描述符 */
typedef int (*rnetiompmodify)(ripple_netiompbase* base, int fd);

/* 事件监听 */
typedef int (*rnetiomp)(ripple_netiompbase* base);

/* 获取事件 */
typedef int (*rnetiompgetevent)(ripple_netiompbase* base, int pos);

/* 内存回收 */
typedef void (*rnetiompfree)(ripple_netiompbase* base);

typedef struct RIPPLE_NETIOMPOPS
{
    ripple_netiomp_type         type;
    rnetiompreset               reset;
    rnetiompcreate              create;
    rnetiompadd                 add;
    rnetiompdel                 del;
    rnetiompmodify              modify;
    rnetiomp                    iomp;
    rnetiompgetevent            getevent;
    rnetiompfree                free;
} ripple_netiompops;

ripple_netiompops* ripple_netiomp_init(int type);

#endif
