#ifndef _NETCLIENT_H
#define _NETCLIENT_H

typedef bool (*netclient_packets_handler)(void* netclient, netpacket* netpacket);

typedef enum NETCLIENT_PROTOCOLTYPE
{
    NETCLIENT_PROTOCOLTYPE_NOP = 0x00,
    NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN,
    NETCLIENT_PROTOCOLTYPE_IPTCP
} netclient_protocoltype;

typedef enum NETCLIENTCONN_STATUS
{
    NETCLIENTCONN_STATUS_NOP = 0x00,       /* Waiting to connect to server         */
    NETCLIENTCONN_STATUS_INPROCESS = 0x01, /* Connecting to server       */
    NETCLIENTCONN_STATUS_CONNECTED = 0x02  /* Connected to server       */
} netclientconn_status;

typedef struct NETCLIENT
{
    int                       hbtimeout;
    int                       timeout;
    netclient_protocoltype    protocoltype; /* Protocol type */
    netclientconn_status      status;       /* Connection status */
    rsocket                   fd;           /* Network descriptor */
    int                       pos;          /* Position index in model */
    char                      svrhost[128]; /* Server listening address */
    char                      svrport[128]; /* Server listening port */
    netiompbase*              base;         /* IO multiplexing base information */
    netiompops*               ops;          /* IO multiplexing model */
    char                      szport[128];  /* Port */
    queue*                    rpackets;     /* Data read */
    queue*                    wpackets;     /* Data to send */
    netclient_packets_handler callback;     /* Callback function */
} netclient;

/* Reset status, timeout, close descriptor, clean packets memory, set iompbase and iompops etc */
void netclient_reset(netclient* netclient);

void netclient_type_set(netclient* netclient, int type);

void netclient_setprotocoltype(netclient* netclient, netclient_protocoltype protocoltype);

void netclient_settimeout(netclient* netclient, int timeout);

void netclient_sethbtimeout(netclient* netclient, int hbtimeout);

void netclient_setsvrhost(netclient* netclient, char* host);

void netclient_setsvrport(netclient* netclient, char* port);

/* Connect to server */
bool netclient_conn(netclient* netclient);

/* Used to check if connected to target, when status is INPROCESS, check if can convert status to
 * CONNECTED */
bool netclient_isconnect(netclient* netclient);

/*
 * Try to connect to server
 *  conn
 *  sleep(1)
 *  is conn ?
 *
 *  true    Connected
 *  false   Not connected
 */
bool netclient_tryconn(netclient* netclient);

/* Create connection and send data */
bool netclient_senddata(netclient_protocoltype ptype, char* host, char* port, uint8* data, int datalen);

/* Create listen event and wait for event trigger, process triggered event */
bool netclient_desc(netclient* netclient);

/*
 * Create listen event and wait for event trigger, receive or send data, only receive or send, no
 * business processing
 */
bool netclient_desc2(netclient* netclient);

bool netclient_addwpacket(netclient* netclient, void* packet);

bool netclient_wpacketisnull(netclient* netclient);

bool netclient_rpacketisnull(netclient* netclient);

bool netclient_default_packets_handler(void* netclient, netpacket* netpacket);

/* Clean up descriptor/queue */
void netclient_clear(netclient* netclient);

/* Resource cleanup */
void netclient_destroy(netclient* netclient);

#endif
