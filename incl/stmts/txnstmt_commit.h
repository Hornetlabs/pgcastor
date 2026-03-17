#ifndef _TXNSTMT_COMMIT_H
#define _TXNSTMT_COMMIT_H

commit_stmt* txnstmt_commit_init(void);

void txnstmt_commit_free(void* data);

#endif
