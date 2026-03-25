#ifndef _XSYNCH_FENET_H_
#define _XSYNCH_FENET_H_

/* connect to xmanager */
bool xsynch_fenet_conn(xsynch_conn* conn);

/* check connection status */
bool xsynch_fenet_isconn(xsynch_conn* conn);

/* send data and get return result */
bool xsynch_fenet_sendcmd(xsynch_conn* conn);

#endif
