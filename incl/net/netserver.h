#ifndef _NETSERVER_H
#define _NETSERVER_H

#define NETSERVER_HOSTMAXLEN      1024
#define NETSERVER_DEFAULTSOCKSIZE 8

typedef bool (*netserver_handler)(void* netserver, rsocket sock);

typedef enum NETSERVER_TYPE
{
    NETSERVER_TYPE_NOP = 0x00,
    NETSERVER_TYPE_XMANAGER
} netserver_type;

typedef enum NETSERVER_HOSTTYPE
{
    NETSERVER_HOSTTYPE_NOP = 0x00,
    NETSERVER_HOSTTYPE_UNIXDOMAIN,
    NETSERVER_HOSTTYPE_IP
} netserver_hosttype;

typedef struct NETSERVER_HOST
{
    netserver_hosttype type;
    char               host[512]; /* Server listening address                    */
} netserver_host;

/* Service */
typedef struct NETSERVER
{
    netserver_type    type; /* Specific type                             */
    int               fdcnt;
    int               fdmax;         /* Number of descriptors                         */
    int               keepalive;     /* Whether to enable keepalive                   */
    int               keepaliveidle; /* Time before sending keepalive for inactive connection    */
    int               keepaliveinterval; /* Keepalive sending interval                 */
    int               keepalivecount;    /* Keepalive sending count                  */
    int               usertimeout;       /* Wait time after sending                     */
    char              port[128];         /* Server listening port                       */
    dlist*            hosts;             /* Listen                                 */
    int*              pos;               /* Position index in model                   */
    rsocket*          fd;                /* Network descriptor                           */
    netiompbase*      base;     /* IO multiplexing base information                       */
    netiompops*       ops;      /* IO multiplexing model                          */
    netserver_handler callback; /* Callback function                             */
} netserver;

/* Initial settings */
bool netserver_reset(netserver* netserver);

/* Set netserver svrhost */
bool netserver_host_set(netserver* netserver, char* host, netserver_hosttype hosttype);

/* Set netserver svrport */
void netserver_port_set(netserver* netserver, int port);

/* Set type */
void netserver_type_set(netserver* netserver, int type);

/* Create server */
bool netserver_create(netserver* netserver);

/* Create event and receive descriptor, call callback function after trigger */
bool netserver_desc(netserver* netserver);

/* Resource cleanup */
void netserver_free(netserver* netserver);

#endif
