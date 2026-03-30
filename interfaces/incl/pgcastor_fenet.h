#ifndef _PGCASTOR_FENET_H_
#define _PGCASTOR_FENET_H_

/* connect to xmanager */
bool pgcastor_fenet_conn(pgcastor_conn* conn);

/* check connection status */
bool pgcastor_fenet_isconn(pgcastor_conn* conn);

/* send data and get return result */
bool pgcastor_fenet_sendcmd(pgcastor_conn* conn);

#endif
