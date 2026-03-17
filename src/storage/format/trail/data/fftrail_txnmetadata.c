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
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_txn.h"
#include "storage/trail/data/fftrail_txnmetadata.h"
#include "storage/trail/data/fftrail_txncommit.h"

/* 在私有域中删除 */
static bool fftrail_txnmetadata_remove(fftrail_privdata* privdata,
                                                catalogdata* catalogdata,
                                                Oid dboid)
{
    bool found = false;
    List*   ls = NULL;
    ListCell* lc = NULL;
    fftrail_table_serialentry* sentry = NULL;
    fftrail_table_serialentry* lcsentry = NULL;
    fftrail_table_serialkey skey = { 0 };

    if(CATALOG_TYPE_ATTRIBUTE != catalogdata->type
        && CATALOG_TYPE_CLASS != catalogdata->type
        && CATALOG_TYPE_INDEX != catalogdata->type)
    {
        return true;
    }

    skey.dbid = dboid;

    if(CATALOG_TYPE_ATTRIBUTE == catalogdata->type)
    {
        catalog_attribute_value* attrvalue = NULL;
        
        attrvalue = (catalog_attribute_value*)catalogdata->catalog;
        skey.tbid = attrvalue->attrelid;
    }
    else if (CATALOG_TYPE_CLASS == catalogdata->type)
    {
        catalog_class_value* classvalue = NULL;
        classvalue = (catalog_class_value*)catalogdata->catalog;
        skey.tbid = classvalue->oid;
    }
    else if (CATALOG_TYPE_INDEX == catalogdata->type)
    {
        catalog_index_value* indexvalue = NULL;
        indexvalue = (catalog_index_value*)catalogdata->catalog;
        skey.tbid = indexvalue->oid;
    }

    sentry = hash_search(privdata->tables, &skey, HASH_REMOVE, &found);
    if(false == found)
    {
        return true;
    }

    /* 在 链表中 删除 */
    foreach(lc, privdata->tbentrys)
    {
        lcsentry = (fftrail_table_serialentry*)lfirst(lc);
        if(sentry == lcsentry)
        {
            continue;
        }
        ls = lappend(ls, lcsentry);
    }

    list_free(privdata->tbentrys);
    privdata->tbentrys = ls;
    return true;
}

/* 系统字典应用及清理 */
bool fftrail_txnmetadata(void* data, void* state)
{
    bool equal = false;
    ListCell* lc = NULL;
    txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    ff_txndata*  txndata = NULL;
    ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    txnstmt_metadata* metadatastmt = NULL;
    catalogdata* catalog_data = NULL;
    fftrail_privdata* privdata = NULL;
    Oid dboid = InvalidOid;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    metadatastmt = (txnstmt_metadata*)rstmt->stmt;
    ffstate = (ffsmgr_state*)state;

    /* 在privdata中清理数据 */
    privdata = (fftrail_privdata*)ffstate->fdata->ffdata;
    lc = metadatastmt->begin;

    dboid = ffstate->callback.getdboid(ffstate->privdata);

    while(1)
    {
        /* 只有 update 和 delete 才需要更新 trail 文件的系统表信息 */
        catalog_data = (catalogdata*)lfirst(lc);
        switch (catalog_data->op)
        {
            case CATALOG_OP_NOP:
                break;
            default:
                /* 应用 */
                fftrail_txnmetadata_remove(privdata, catalog_data, dboid);
                break;
        }

        /* 应用到系统表中 */
        ffstate->callback.catalog2transcache(ffstate->privdata, (void*)lc);
        
        /* 保存两个 checkpoint 之间的系统表变更 */
        if (NULL != ffstate->callback.setredosysdicts)
        {
            ffstate->callback.setredosysdicts(ffstate->privdata, (void*)catalog_data);
            lfirst(lc) = NULL;
        }

        /* 只有一个 */
        if(lc == metadatastmt->end
            || true == equal)
        {
            break;
        }

        /* 校验是否到达最后一个 */
        lc = lc->next;
        if(lc == metadatastmt->end)
        {
            equal = true;
        }
    }

    return true;
}

