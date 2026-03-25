#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "stmts/txnstmt.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"

void txnstmt_onlinerefresh_begin_free(void* data)
{
    txnstmt_onlinerefresh* onlinerefresh = NULL;

    if (NULL == data)
    {
        return;
    }

    onlinerefresh = (txnstmt_onlinerefresh*)data;

    if (onlinerefresh->no)
    {
        rfree(onlinerefresh->no);
    }

    if (onlinerefresh->refreshtables)
    {
        refresh_freetables(onlinerefresh->refreshtables);
    }

    rfree(data);
    return;
}

void txnstmt_onlinerefresh_end_free(void* data)
{
    rfree(data);
    return;
}

void txnstmt_onlinerefresh_increment_end_free(void* data)
{
    rfree(data);
    return;
}

txnstmt_onlinerefresh* txnstmt_onlinerefresh_init(void)
{
    txnstmt_onlinerefresh* result = NULL;

    result = rmalloc0(sizeof(txnstmt_onlinerefresh));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(txnstmt_onlinerefresh));
    return result;
}

void txnstmt_onlinerefresh_set_increment(txnstmt_onlinerefresh* refresh, int8 increment)
{
    refresh->increment = increment;
}

void txnstmt_onlinerefresh_set_no(txnstmt_onlinerefresh* refresh, uuid_t* uuid)
{
    refresh->no = uuid;
}

void txnstmt_onlinerefresh_set_txid(txnstmt_onlinerefresh* refresh, FullTransactionId txid)
{
    refresh->txid = txid;
}

void txnstmt_onlinerefresh_set_refreshtables(txnstmt_onlinerefresh* refresh, refresh_tables* tables)
{
    refresh->refreshtables = tables;
}
