#ifndef _XSYNCH_FEPARSEMSG_H_
#define _XSYNCH_FEPARSEMSG_H_

/*
 * convert received descriptor to parse result
 */
bool xsynch_feparsemsg_msg2result(xsynch_exbuffer msg, xsynch_conn* conn);

#endif
