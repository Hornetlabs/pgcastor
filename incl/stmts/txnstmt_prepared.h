#ifndef _RIPPLE_TXNSTMT_PREPARED_H_
#define _RIPPLE_TXNSTMT_PREPARED_H_

ripple_txnstmt_prepared* ripple_txnstmt_prepared_init(void);

/* 释放 */
void ripple_txnstmt_prepared_free(void* data);

#endif
