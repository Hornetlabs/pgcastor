#ifndef _XSYNCH_INT_H_
#define _XSYNCH_INT_H_

typedef enum XSYNCH_SOCKTYPE
{
    XSYNCH_SOCKTYPE_NOP = 0x00,
    XSYNCH_SOCKTYPE_UNIXDOMAIN,
    XSYNCH_SOCKTYPE_TCP
} xsynch_socktype;

typedef struct XSYCNH_RESULT
{
    /* error code */
    int        rowcnt;
    xsynchrow* rows;
} xsynch_result;

/*
 * struct definition
 */
typedef struct XSYNCH_CONN
{
    /* connection to xmanager descriptor */
    int sock;

    /* error code, interface side */
    int errcode;

    /* connection status */
    xsynchconn_status connstatus;

    /* socktype */
    xsynch_socktype socktype;

    /*
     * tcp keep alive related
     */
    int keepalive;
    int keepaliveidle;
    int keepaliveinterval;
    int keepalivecount;
    int usertimeout;

    char host[512];
    char port[128];

    /* send data buffer */
    xsynch_exbuffer sendmsg;

    /* receive data buffer */
    xsynch_exbuffer recvmsg;

    /* error message */
    xsynch_exbuffer errmsg;

    /* return result */
    xsynch_result* result;
} xsynch_conn;

#endif
