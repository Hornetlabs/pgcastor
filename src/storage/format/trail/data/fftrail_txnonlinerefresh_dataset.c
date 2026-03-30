#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "refresh/refresh_tables.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_dataset.h"

/* Set linked list in online refresh dataset statement */
bool fftrail_txnonlinerefresh_dataset(void* data, void* state)

{
    txnstmt*      rstmt = NULL; /* Content to write to trail file */
    ff_txndata*   txndata = NULL;
    ffsmgr_state* ffstate = NULL; /* state data info */
    List*         dataset_list = NULL;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    dataset_list = (List*)rstmt->stmt;
    ffstate = (ffsmgr_state*)state;

    /* Save list */
    if (NULL != ffstate->callback.setredosysdicts)
    {
        ffstate->callback.setonlinerefreshdataset(ffstate->privdata, (void*)dataset_list);
    }

    return true;
}
