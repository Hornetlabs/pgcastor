#ifndef _NETIOMP_H
#define _NETIOMP_H


typedef enum NETIOMP_TYPE
{
    NETIOMP_TYPE_NOP         = 0x00,
    NETIOMP_TYPE_SELECT      = 0x01,
    NETIOMP_TYPE_POLL        = 0x02,
    NETIOMP_TYPE_EPOLL       = 0x03
} netiomp_type;

#define MAXFD                    8

typedef struct NETIOMPBASE
{
    int                 num;
    int                 max;
    int                 timeout;
    struct pollfd*      events;
} netiompbase;

/* 重置 */
typedef void (*rnetiompreset)(netiompbase* base);

/* 创建 */
typedef bool (*rnetiompcreate)(netiompbase** base);

/* 增加描述符和事件类型 */
typedef int (*rnetiompadd)(netiompbase* base, int fd, uint16 flag);

/* 删除描述符 */
typedef int (*rnetiompdel)(netiompbase* base, int fd);

/* 修改描述符 */
typedef int (*rnetiompmodify)(netiompbase* base, int fd);

/* 事件监听 */
typedef int (*rnetiomp)(netiompbase* base);

/* 获取事件 */
typedef int (*rnetiompgetevent)(netiompbase* base, int pos);

/* 内存回收 */
typedef void (*rnetiompfree)(netiompbase* base);

typedef struct NETIOMPOPS
{
    netiomp_type         type;
    rnetiompreset               reset;
    rnetiompcreate              create;
    rnetiompadd                 add;
    rnetiompdel                 del;
    rnetiompmodify              modify;
    rnetiomp                    iomp;
    rnetiompgetevent            getevent;
    rnetiompfree                free;
} netiompops;

netiompops* netiomp_init(int type);

#endif
