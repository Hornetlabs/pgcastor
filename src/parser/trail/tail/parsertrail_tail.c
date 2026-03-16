#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/tail/ripple_parsertrail_tail.h"

/* 
 * Trail 尾部应用
 */
bool ripple_parsertrail_trailtailapply( ripple_parsertrail* parsertrail, void* data)
{
    /* 数据清理 */
    ripple_fftrail_invalidprivdata(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    return true;
}

/* 
 * 清理数据
 */
void ripple_parsertrail_trailtailclean( ripple_parsertrail* parsertrail, void* data)
{
    RIPPLE_UNUSED(parsertrail);

    /* 释放data */
    if (data)
    {
        rfree(data);
    }
}
