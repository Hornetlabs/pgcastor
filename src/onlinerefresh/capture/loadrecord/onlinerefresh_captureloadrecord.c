#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "port/net/net.h"
#include "net/netiomp/netiomp.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/dlist/dlist.h"
#include "threads/threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "misc/misc_stat.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "snapshot/snapshot.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/splitwork/wal/wal_define.h"
#include "onlinerefresh/capture/loadrecord/onlinerefresh_captureloadrecord.h"

onlinerefresh_captureloadrecord *onlinerefresh_captureloadrecord_init(void)
{
    onlinerefresh_captureloadrecord *result = NULL;

    result = rmalloc0(sizeof(onlinerefresh_captureloadrecord));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    result = rmemset0(result, 0, 0, sizeof(onlinerefresh_captureloadrecord));
    
    result->splitwalctx = splitwal_init();

    return result;
}
