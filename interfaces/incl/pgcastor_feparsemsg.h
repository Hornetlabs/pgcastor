#ifndef _PGCASTOR_FEPARSEMSG_H_
#define _PGCASTOR_FEPARSEMSG_H_

/*
 * convert received descriptor to parse result
 */
bool pgcastor_feparsemsg_msg2result(pgcastor_exbuffer msg, pgcastor_conn* conn);

#endif
