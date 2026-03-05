#ifndef _XSYNCH_INT_H_
#define _XSYNCH_INT_H_

typedef enum XSYNCH_SOCKTYPE
{
    XSYNCH_SOCKTYPE_NOP                 = 0x00,
    XSYNCH_SOCKTYPE_UNIXDOMAIN          ,
    XSYNCH_SOCKTYPE_TCP                 
} xsynch_socktype;

typedef struct XSYCNH_RESULT
{
    /* 错误码 */
    int                     rowcnt;
    xsynchrow*              rows;
} xsynch_result;

/*
 * 结构体定义
*/
typedef struct XSYNCH_CONN
{
    /* 连接 xmanager 描述符 */
    int                 sock;

    /* 错误码, interface 端 */
    int                 errcode;

    /* 连接状态 */
    xsynchconn_status   connstatus;

    /* socktype */
    xsynch_socktype     socktype;

    /* 
     * tcp keep alive 相关
     */
    int                 keepalive;
    int                 keepaliveidle;
    int                 keepaliveinterval;
    int                 keepalivecount;
    int                 usertimeout;

    char                host[512];
    char                port[128];

    /* 发送数据缓存 */
    xsynch_exbuffer     sendmsg;

    /* 接收数据缓存 */
    xsynch_exbuffer     recvmsg;

    /* 错误信息 */
    xsynch_exbuffer     errmsg;

    /* 返回结果 */
    xsynch_result*      result;
} xsynch_conn;

#endif
