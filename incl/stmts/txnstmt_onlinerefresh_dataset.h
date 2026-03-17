#ifndef TXNSTMT_ONLINEREFRESH_DATASET_H
#define TXNSTMT_ONLINEREFRESH_DATASET_H

extern void *parserwork_build_onlinerefresh_dataset_txn(List *tables_list);
extern void txnstmt_onlinerefresh_dataset_free(void* data);

#endif
