#ifndef _RIPPLE_TXNSTMT_ONLINEREFRESH_H
#define _RIPPLE_TXNSTMT_ONLINEREFRESH_H

typedef struct ripple_txnstmt_onlinerefresh
{
    bool                    increment;          /* 增量同步 */
    ripple_uuid_t          *no;                 /* refresh 编号 */
    FullTransactionId       txid;               /* 获取的事务号,基于此过滤事务 */
    ripple_refresh_tables  *refreshtables;      /* 待同步的表	*/
} ripple_txnstmt_onlinerefresh;

void ripple_txnstmt_onlinerefresh_begin_free(void* data);
void ripple_txnstmt_onlinerefresh_end_free(void* data);
void ripple_txnstmt_onlinerefresh_increment_end_free(void* data);
ripple_txnstmt_onlinerefresh *ripple_txnstmt_onlinerefresh_init(void);
void ripple_txnstmt_onlinerefresh_set_increment(ripple_txnstmt_onlinerefresh *refresh, int8 increment);
void ripple_txnstmt_onlinerefresh_set_no(ripple_txnstmt_onlinerefresh *refresh, ripple_uuid_t *uuid);
void ripple_txnstmt_onlinerefresh_set_txid(ripple_txnstmt_onlinerefresh *refresh, FullTransactionId txid);
void ripple_txnstmt_onlinerefresh_set_refreshtables(ripple_txnstmt_onlinerefresh *refresh, ripple_refresh_tables *tables);
#endif
