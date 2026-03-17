#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "cache/cache_sysidcts.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_txnshiftfile.h"


/* 将lsn信息写入file_buffer.extra */
bool fftrail_txnshiftfile(void* data, void* state)
{
    txnstmt* rstmt = NULL;
    file_buffer* fbuffer = NULL;
    ffsmgr_state* ffsmgrstate = NULL;
    txnstmt_shiftfile* stmt = NULL;
    ff_txndata*  txndata = NULL;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;

    ffsmgrstate = (ffsmgr_state*)state;
    stmt = (txnstmt_shiftfile* )rstmt->stmt;

    fbuffer = file_buffer_getbybufid(ffsmgrstate->callback.getfilebuffer(ffsmgrstate->privdata), ffsmgrstate->bufid);
    fbuffer->extra.chkpoint.redolsn.wal.lsn = stmt->redolsn;
    fbuffer->extra.rewind.restartlsn.wal.lsn = stmt->restartlsn;
    fbuffer->extra.rewind.confirmlsn.wal.lsn = stmt->confirmlsn;

    return true;
}

