#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "queue/queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/smgr.h"
#include "storage/ffsmgr.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"

bool bigtxn_reset(bigtxn* bigtxn)
{
    /* 初始化 buffer 信息 */
    rmemset1(&bigtxn->fbuffer, 0, '\0', sizeof(file_buffer));
    bigtxn->txndicts = NULL;
    bigtxn->fbuffer.bufid = INVALID_BUFFERID;
    bigtxn->fbuffer.data = rmalloc0(FILE_BUFFER_SIZE);
    if(NULL == bigtxn->fbuffer.data)
    {
        elog(RLOG_WARNING, "big transaction capture serial out of memory");
        return false;
    }
    rmemset0(bigtxn->fbuffer.data, 0, '\0', FILE_BUFFER_SIZE);

    return true;
}

/* 清理大事务txn */
void bigtxn_clean(bigtxn *htxn)
{
    if (htxn)
    {
        if (htxn->txndicts)
        {
            cache_sysdicts_txnsysdicthisfree(htxn->txndicts);
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
