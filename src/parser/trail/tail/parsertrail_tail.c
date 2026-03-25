#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
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
 * Trail tail application
 */
bool parsertrail_trailtailapply(parsertrail* parsertrail, void* data)
{
    /* Data cleanup */
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    return true;
}

/*
 * Clean data
 */
void parsertrail_trailtailclean(parsertrail* parsertrail, void* data)
{
    UNUSED(parsertrail);

    /* Free data */
    if (data)
    {
        rfree(data);
    }
}
