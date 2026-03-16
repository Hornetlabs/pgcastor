#ifndef RIPPLE_TXNSTMT_ONLINEREFRESH_DATASET_H
#define RIPPLE_TXNSTMT_ONLINEREFRESH_DATASET_H

extern void *ripple_parserwork_build_onlinerefresh_dataset_txn(List *tables_list);
extern void ripple_txnstmt_onlinerefresh_dataset_free(void* data);

#endif
