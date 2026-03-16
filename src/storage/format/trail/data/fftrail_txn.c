#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "storage/trail/data/ripple_fftrail_tbmetadata.h"
#include "storage/trail/data/ripple_fftrail_txn.h"
#include "storage/trail/data/ripple_fftrail_txninsert.h"
#include "storage/trail/data/ripple_fftrail_txndelete.h"
#include "storage/trail/data/ripple_fftrail_txnupdate.h"
#include "storage/trail/data/ripple_fftrail_txnddl.h"
#include "storage/trail/data/ripple_fftrail_txnmetadata.h"
#include "storage/trail/data/ripple_fftrail_txnmultiinsert.h"
#include "storage/trail/data/ripple_fftrail_txnshiftfile.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "storage/trail/data/ripple_fftrail_txnrefresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefresh_begin.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefresh_end.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefresh_increment_end.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefresh_dataset.h"
#include "storage/trail/data/ripple_fftrail_txnbigtxn_begin.h"
#include "storage/trail/data/ripple_fftrail_txnbigtxn_end.h"
#include "storage/trail/data/ripple_fftrail_txnonlinerefreshabandon.h"
#include "storage/trail/data/ripple_fftrail_txncommit.h"


/* 数据库信息序列化 */
bool ripple_fftrail_txn_serial(void* data, void* state)
{
    ripple_ff_txndata*  txndata = NULL;
    ripple_txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    xk_pg_parser_translog_tbcolbase* tbcolbase = NULL;

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;

    /* 根据 type 的类型写入不同的数据 */
    if (rstmt->type == RIPPLE_TXNSTMT_TYPE_DDL)
    {
        /* 数据落盘 */
        ripple_fftrail_txnddl_serial(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_METADATA)
    {
        /* 遍历设置将相关的系统表设置为无效，并应用到全局中 */
        ripple_fftrail_txnmetadata(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_SHIFTFILE)
    {
        /* 将lsn信息写入buffer */
        ripple_fftrail_txnshiftfile(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_REFRESH)
    {
        /* 将refreshtables落盘 */
        ripple_fftrail_txnrefresh_serial(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_BEGIN)
    {
        ripple_fftrail_txnonlinerefresh_begin_serial(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END)
    {
        ripple_fftrail_txnonlinerefresh_end_serial(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END)
    {
        ripple_fftrail_txnonlinerefresh_increment_end_serial(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_DATASET)
    {
        ripple_fftrail_txnonlinerefresh_dataset(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_BIGTXN_BEGIN)
    {
        ripple_fftrail_txnbigtxn_begin_serial(data, state);
        return true;
    }
    else if (rstmt->type == RIPPLE_TXNSTMT_TYPE_BIGTXN_END)
    {
        ripple_fftrail_txnbigtxn_end_serial(data, state);
        return true;
    }
    else if (RIPPLE_TXNSTMT_TYPE_ONLINEREFRESHABANDON == rstmt->type)
    {
        ripple_fftrail_txnonlinerefreshabandon_serial(data, state);
        return true;
    }
    else if (RIPPLE_TXNSTMT_TYPE_ABANDON == rstmt->type)
    {
        return true;
    }
    else if (RIPPLE_TXNSTMT_TYPE_COMMIT == rstmt->type)
    {
        ripple_fftrail_txncommit_serial(data, state);
        return true;
    }

    tbcolbase = (xk_pg_parser_translog_tbcolbase*)rstmt->stmt;

    switch (tbcolbase->m_dmltype)
    {
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT:
            ripple_fftrail_txninsert_serial(data, state);
            break;
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE:
            ripple_fftrail_txndelete_serial(data, state);
            break;
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE:
            ripple_fftrail_txnupdate_serial(data, state);
            break;
        case XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT:
            ripple_fftrail_txnmultiinsert_serial(data, state);
            break;
        default:
            elog(RLOG_ERROR, "unknown dmltype, %d", tbcolbase->m_dmltype);
            break;
    }

    return true;
}
