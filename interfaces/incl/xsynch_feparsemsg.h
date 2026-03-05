#ifndef _XSYNCH_FEPARSEMSG_H_
#define _XSYNCH_FEPARSEMSG_H_

/*
 * 将接收到的描述符转化为解析结果
*/
bool xsynch_feparsemsg_msg2result(xsynch_exbuffer msg, xsynch_conn* conn);

#endif
