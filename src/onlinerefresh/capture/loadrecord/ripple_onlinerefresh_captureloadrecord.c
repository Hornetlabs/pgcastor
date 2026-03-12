#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "port/net/ripple_net.h"
#include "net/netiomp/ripple_netiomp.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "misc/ripple_misc_stat.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "snapshot/ripple_snapshot.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "onlinerefresh/capture/ripple_onlinerefresh_capture.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "works/splitwork/wal/ripple_wal_define.h"
#include "onlinerefresh/capture/loadrecord/ripple_onlinerefresh_captureloadrecord.h"

ripple_onlinerefresh_captureloadrecord *ripple_onlinerefresh_captureloadrecord_init(void)
{
    ripple_onlinerefresh_captureloadrecord *result = NULL;

    result = rmalloc0(sizeof(ripple_onlinerefresh_captureloadrecord));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    result = rmemset0(result, 0, 0, sizeof(ripple_onlinerefresh_captureloadrecord));
    
    result->splitwalctx = ripple_splitwal_init();

    return result;
}
