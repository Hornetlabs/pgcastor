#ifndef _NETPOOL_H_
#define _NETPOOL_H_

#define NETPOOL_DEFAULTFDSIZE 128

typedef enum NETPOOLENTRY_STAT
{
    NETPOOLENTRY_STAT_NOP = 0x00,
    NETPOOLENTRY_STAT_OK,
    NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL,

    NETPOOLENTRY_STAT_MAX
} netpoolentry_stat;

typedef struct NETPOOLENTRY
{
    rsocket           fd;
    netpoolentry_stat stat;
    char*             host;
    char*             port;
    queue*            rpackets; /* Data read */
    queue*            wpackets; /* Data to send */
} netpoolentry;

/* Service */
typedef struct NETPOOL
{
    int            fdcnt;
    int            fdmax;
    int*           pos;      /* Position */
    int*           errorfds; /* Descriptors with errors */
    netiompbase*   base;     /* IO multiplexing base information                       */
    netiompops*    ops;      /* IO multiplexing model                          */
    netpoolentry** fds;
} netpool;

extern netpoolentry* netpoolentry_init(void);

/* Descriptor settings */
extern void netpoolentry_setfd(netpoolentry* npoolentry, int fd);

/* Set host information */
extern bool netpoolentry_sethost(netpoolentry* npoolentry, char* host);

/* Set port information */
extern bool netpoolentry_setport(netpoolentry* npoolentry, char* port);

extern void netpoolentry_destroy(netpoolentry* npoolentry);

extern netpool* netpool_init(void);

/* Add */
extern bool netpool_add(netpool* npool, netpoolentry* entry);

/* Delete */
extern void netpool_del(netpool* npool, int fd);

/* Get entry from netpool by fd */
extern netpoolentry* netpool_getentrybyfd(netpool* npool, int fd);

/* Create event and receive descriptor, call callback function after trigger */
extern bool netpool_desc(netpool* npool, int* cnt, int** perrorfds);

/* Destroy */
extern void netpool_destroy(netpool* npool);

#endif
