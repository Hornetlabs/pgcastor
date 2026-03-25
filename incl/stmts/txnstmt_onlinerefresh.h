#ifndef _TXNSTMT_ONLINEREFRESH_H
#define _TXNSTMT_ONLINEREFRESH_H

typedef struct txnstmt_onlinerefresh
{
    bool              increment; /* Incremental sync */
    uuid_t*           no;        /* refresh Number */
    FullTransactionId txid;      /* Obtained transaction ID, filter transactions based on this */
    refresh_tables*   refreshtables; /* Tables to sync	*/
} txnstmt_onlinerefresh;

void                   txnstmt_onlinerefresh_begin_free(void* data);
void                   txnstmt_onlinerefresh_end_free(void* data);
void                   txnstmt_onlinerefresh_increment_end_free(void* data);
txnstmt_onlinerefresh* txnstmt_onlinerefresh_init(void);
void txnstmt_onlinerefresh_set_increment(txnstmt_onlinerefresh* refresh, int8 increment);
void txnstmt_onlinerefresh_set_no(txnstmt_onlinerefresh* refresh, uuid_t* uuid);
void txnstmt_onlinerefresh_set_txid(txnstmt_onlinerefresh* refresh, FullTransactionId txid);
void txnstmt_onlinerefresh_set_refreshtables(txnstmt_onlinerefresh* refresh,
                                             refresh_tables*        tables);
#endif
