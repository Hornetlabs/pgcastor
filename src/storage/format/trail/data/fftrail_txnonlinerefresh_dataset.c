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

/* online refresh dataset 语句将链表设置到中 */
bool fftrail_txnonlinerefresh_dataset(void* data, void* state)

{
    txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    ff_txndata*  txndata = NULL;
    ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    List *dataset_list = NULL;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    dataset_list = (List*)rstmt->stmt;
    ffstate = (ffsmgr_state*)state;

    /* 保存 lsit */
    if (NULL != ffstate->callback.setredosysdicts)
    {
        ffstate->callback.setonlinerefreshdataset(ffstate->privdata, (void*)dataset_list);
    }

    return true;
}
