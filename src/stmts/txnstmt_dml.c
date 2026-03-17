#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_dml.h"
#include "works/parserwork/wal/decode_heap_util.h"

void txnstmt_dmlfree(void* data)
{
    if(NULL == data)
    {
        return;
    }

    heap_free_trans_result((xk_pg_parser_translog_tbcolbase*)data);
}
