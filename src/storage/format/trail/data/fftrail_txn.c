#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_tbmetadata.h"
#include "storage/trail/data/fftrail_txn.h"
#include "storage/trail/data/fftrail_txninsert.h"
#include "storage/trail/data/fftrail_txndelete.h"
#include "storage/trail/data/fftrail_txnupdate.h"
#include "storage/trail/data/fftrail_txnddl.h"
#include "storage/trail/data/fftrail_txnmetadata.h"
#include "storage/trail/data/fftrail_txnmultiinsert.h"
#include "storage/trail/data/fftrail_txnshiftfile.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "storage/trail/data/fftrail_txnrefresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_begin.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_end.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_increment_end.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_dataset.h"
#include "storage/trail/data/fftrail_txnbigtxn_begin.h"
#include "storage/trail/data/fftrail_txnbigtxn_end.h"
#include "storage/trail/data/fftrail_txnonlinerefreshabandon.h"
#include "storage/trail/data/fftrail_txncommit.h"


/* 数据库信息序列化 */
bool fftrail_txn_serial(void* data, void* state)
{
    ff_txndata*  txndata = NULL;
    txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    pg_parser_translog_tbcolbase* tbcolbase = NULL;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;

    /* 根据 type 的类型写入不同的数据 */
    if (rstmt->type == TXNSTMT_TYPE_DDL)
    {
        /* 数据落盘 */
        fftrail_txnddl_serial(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_METADATA)
    {
        /* 遍历设置将相关的系统表设置为无效，并应用到全局中 */
        fftrail_txnmetadata(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_SHIFTFILE)
    {
        /* 将lsn信息写入buffer */
        fftrail_txnshiftfile(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_REFRESH)
    {
        /* 将refreshtables落盘 */
        fftrail_txnrefresh_serial(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_ONLINEREFRESH_BEGIN)
    {
        fftrail_txnonlinerefresh_begin_serial(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_ONLINEREFRESH_END)
    {
        fftrail_txnonlinerefresh_end_serial(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END)
    {
        fftrail_txnonlinerefresh_increment_end_serial(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_ONLINEREFRESH_DATASET)
    {
        fftrail_txnonlinerefresh_dataset(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_BIGTXN_BEGIN)
    {
        fftrail_txnbigtxn_begin_serial(data, state);
        return true;
    }
    else if (rstmt->type == TXNSTMT_TYPE_BIGTXN_END)
    {
        fftrail_txnbigtxn_end_serial(data, state);
        return true;
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESHABANDON == rstmt->type)
    {
        fftrail_txnonlinerefreshabandon_serial(data, state);
        return true;
    }
    else if (TXNSTMT_TYPE_ABANDON == rstmt->type)
    {
        return true;
    }
    else if (TXNSTMT_TYPE_COMMIT == rstmt->type)
    {
        fftrail_txncommit_serial(data, state);
        return true;
    }

    tbcolbase = (pg_parser_translog_tbcolbase*)rstmt->stmt;

    switch (tbcolbase->m_dmltype)
    {
        case PG_PARSER_TRANSLOG_DMLTYPE_INSERT:
            fftrail_txninsert_serial(data, state);
            break;
        case PG_PARSER_TRANSLOG_DMLTYPE_DELETE:
            fftrail_txndelete_serial(data, state);
            break;
        case PG_PARSER_TRANSLOG_DMLTYPE_UPDATE:
            fftrail_txnupdate_serial(data, state);
            break;
        case PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT:
            fftrail_txnmultiinsert_serial(data, state);
            break;
        default:
            elog(RLOG_ERROR, "unknown dmltype, %d", tbcolbase->m_dmltype);
            break;
    }

    return true;
}
