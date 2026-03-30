#ifndef _PGCASTOR_INT_H_
#define _PGCASTOR_INT_H_

typedef enum PGCASTOR_SOCKTYPE
{
    PGCASTOR_SOCKTYPE_NOP = 0x00,
    PGCASTOR_SOCKTYPE_UNIXDOMAIN,
    PGCASTOR_SOCKTYPE_TCP
} pgcastor_socktype;

typedef struct PGCASTOR_RESULT
{
    /* error code */
    int          rowcnt;
    pgcastorrow* rows;
} pgcastor_result;

/*
 * struct definition
 */
typedef struct PGCASTOR_CONN
{
    /* connection to xmanager descriptor */
    int                 sock;

    /* error code, interface side */
    int                 errcode;

    /* connection status */
    pgcastorconn_status connstatus;

    /* socktype */
    pgcastor_socktype   socktype;

    /*
     * tcp keep alive related
     */
    int                 keepalive;
    int                 keepaliveidle;
    int                 keepaliveinterval;
    int                 keepalivecount;
    int                 usertimeout;

    char                host[512];
    char                port[128];

    /* send data buffer */
    pgcastor_exbuffer   sendmsg;

    /* receive data buffer */
    pgcastor_exbuffer   recvmsg;

    /* error message */
    pgcastor_exbuffer   errmsg;

    /* return result */
    pgcastor_result*    result;
} pgcastor_conn;

#endif
