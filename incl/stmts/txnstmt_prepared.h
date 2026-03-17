#ifndef _TXNSTMT_PREPARED_H_
#define _TXNSTMT_PREPARED_H_

txnstmt_prepared* txnstmt_prepared_init(void);

/* 释放 */
void txnstmt_prepared_free(void* data);

#endif
