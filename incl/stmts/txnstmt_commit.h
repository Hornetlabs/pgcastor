#ifndef _RIPPLE_TXNSTMT_COMMIT_H
#define _RIPPLE_TXNSTMT_COMMIT_H

ripple_commit_stmt* ripple_txnstmt_commit_init(void);

void ripple_txnstmt_commit_free(void* data);

#endif
