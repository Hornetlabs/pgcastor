#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_dml.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"

void ripple_txnstmt_dmlfree(void* data)
{
    if(NULL == data)
    {
        return;
    }

    heap_free_trans_result((xk_pg_parser_translog_tbcolbase*)data);
}
