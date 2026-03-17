#ifndef _TXNSTMT_ONLINEREFRESH_H
#define _TXNSTMT_ONLINEREFRESH_H

typedef struct txnstmt_onlinerefresh
{
    bool                    increment;          /* 增量同步 */
    uuid_t          *no;                 /* refresh 编号 */
    FullTransactionId       txid;               /* 获取的事务号,基于此过滤事务 */
    refresh_tables  *refreshtables;      /* 待同步的表	*/
} txnstmt_onlinerefresh;

void txnstmt_onlinerefresh_begin_free(void* data);
void txnstmt_onlinerefresh_end_free(void* data);
void txnstmt_onlinerefresh_increment_end_free(void* data);
txnstmt_onlinerefresh *txnstmt_onlinerefresh_init(void);
void txnstmt_onlinerefresh_set_increment(txnstmt_onlinerefresh *refresh, int8 increment);
void txnstmt_onlinerefresh_set_no(txnstmt_onlinerefresh *refresh, uuid_t *uuid);
void txnstmt_onlinerefresh_set_txid(txnstmt_onlinerefresh *refresh, FullTransactionId txid);
void txnstmt_onlinerefresh_set_refreshtables(txnstmt_onlinerefresh *refresh, refresh_tables *tables);
#endif
