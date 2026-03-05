#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "refresh/ripple_refresh_tables.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefresh_dataset.h"

/* online refresh dataset 语句将链表设置到中 */
bool ripple_fftrail_txnonlinerefresh_dataset(void* data, void* state)

{
    ripple_txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    ripple_ff_txndata*  txndata = NULL;
    ripple_ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    List *dataset_list = NULL;

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;
    dataset_list = (List*)rstmt->stmt;
    ffstate = (ripple_ffsmgr_state*)state;

    /* 保存 lsit */
    if (NULL != ffstate->callback.setredosysdicts)
    {
        ffstate->callback.setonlinerefreshdataset(ffstate->privdata, (void*)dataset_list);
    }

    return true;
}
