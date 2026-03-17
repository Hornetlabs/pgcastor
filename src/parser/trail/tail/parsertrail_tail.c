#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/tail/parsertrail_tail.h"

/* 
 * Trail 尾部应用
 */
bool parsertrail_trailtailapply( parsertrail* parsertrail, void* data)
{
    /* 数据清理 */
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    return true;
}

/* 
 * 清理数据
 */
void parsertrail_trailtailclean( parsertrail* parsertrail, void* data)
{
    UNUSED(parsertrail);

    /* 释放data */
    if (data)
    {
        rfree(data);
    }
}
