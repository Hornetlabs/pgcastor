#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "stmts/ripple_txnstmt.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"

void ripple_txnstmt_onlinerefresh_begin_free(void* data)
{
    ripple_txnstmt_onlinerefresh *onlinerefresh = NULL;

    if (NULL == data)
    {
        return;
    }

    onlinerefresh = (ripple_txnstmt_onlinerefresh *)data;

    if (onlinerefresh->no)
    {
        rfree(onlinerefresh->no);
    }

    if (onlinerefresh->refreshtables)
    {
        ripple_refresh_freetables(onlinerefresh->refreshtables);
    }

    rfree(data);
    return;
}

void ripple_txnstmt_onlinerefresh_end_free(void* data)
{
    rfree(data);
    return;
}

void ripple_txnstmt_onlinerefresh_increment_end_free(void* data)
{
    rfree(data);
    return;
}

ripple_txnstmt_onlinerefresh *ripple_txnstmt_onlinerefresh_init(void)
{
    ripple_txnstmt_onlinerefresh *result = NULL;

    result = rmalloc0(sizeof(ripple_txnstmt_onlinerefresh));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_txnstmt_onlinerefresh));
    return result;
}

void ripple_txnstmt_onlinerefresh_set_increment(ripple_txnstmt_onlinerefresh *refresh, int8 increment)
{
    refresh->increment = increment;
}

void ripple_txnstmt_onlinerefresh_set_no(ripple_txnstmt_onlinerefresh *refresh, ripple_uuid_t *uuid)
{
    refresh->no = uuid;
}

void ripple_txnstmt_onlinerefresh_set_txid(ripple_txnstmt_onlinerefresh *refresh, FullTransactionId txid)
{
    refresh->txid = txid;
}

void ripple_txnstmt_onlinerefresh_set_refreshtables(ripple_txnstmt_onlinerefresh *refresh, ripple_refresh_tables *tables)
{
    refresh->refreshtables = tables;
}
