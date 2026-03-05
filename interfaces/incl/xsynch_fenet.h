#ifndef _XSYNCH_FENET_H_
#define _XSYNCH_FENET_H_

/* 连接 xmanager */
bool xsynch_fenet_conn(xsynch_conn* conn);

/* 查看连接状态 */
bool xsynch_fenet_isconn(xsynch_conn* conn);

/* 发送数据并获取返回结果 */
bool xsynch_fenet_sendcmd(xsynch_conn* conn);

#endif
