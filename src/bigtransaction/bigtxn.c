#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_smgr.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/ripple_bigtxn.h"

bool ripple_bigtxn_reset(ripple_bigtxn* bigtxn)
{
    /* 初始化 buffer 信息 */
    rmemset1(&bigtxn->fbuffer, 0, '\0', sizeof(ripple_file_buffer));
    bigtxn->txndicts = NULL;
    bigtxn->fbuffer.bufid = RIPPLE_INVALID_BUFFERID;
    bigtxn->fbuffer.data = rmalloc0(RIPPLE_FILE_BUFFER_SIZE);
    if(NULL == bigtxn->fbuffer.data)
    {
        elog(RLOG_WARNING, "big transaction capture serial out of memory");
        return false;
    }
    rmemset0(bigtxn->fbuffer.data, 0, '\0', RIPPLE_FILE_BUFFER_SIZE);

    return true;
}

/* 清理大事务txn */
void ripple_bigtxn_clean(ripple_bigtxn *htxn)
{
    if (htxn)
    {
        if (htxn->txndicts)
        {
            ripple_cache_sysdicts_txnsysdicthisfree(htxn->txndicts);
        }
        /* fdata不在这里释放 */
        if (htxn->fbuffer.data)
        {
            rfree(htxn->fbuffer.data);
            htxn->fbuffer.data = NULL;
        }
        if (htxn->fbuffer.privdata)
        {
            rfree(htxn->fbuffer.privdata);
            htxn->fbuffer.privdata = NULL;
        }
    }
}
