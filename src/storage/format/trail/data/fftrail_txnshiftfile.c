#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "catalog/ripple_catalog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_cache_sysidcts.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_txnshiftfile.h"


/* 将lsn信息写入ripple_file_buffer.extra */
bool ripple_fftrail_txnshiftfile(void* data, void* state)
{
    ripple_txnstmt* rstmt = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_ffsmgr_state* ffsmgrstate = NULL;
    ripple_txnstmt_shiftfile* stmt = NULL;
    ripple_ff_txndata*  txndata = NULL;

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;

    ffsmgrstate = (ripple_ffsmgr_state*)state;
    stmt = (ripple_txnstmt_shiftfile* )rstmt->stmt;

    fbuffer = ripple_file_buffer_getbybufid(ffsmgrstate->callback.getfilebuffer(ffsmgrstate->privdata), ffsmgrstate->bufid);
    fbuffer->extra.chkpoint.redolsn.wal.lsn = stmt->redolsn;
    fbuffer->extra.rewind.restartlsn.wal.lsn = stmt->restartlsn;
    fbuffer->extra.rewind.confirmlsn.wal.lsn = stmt->confirmlsn;

    return true;
}

