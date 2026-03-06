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
#include "storage/trail/data/ripple_fftrail_data.h"
#include "storage/trail/data/ripple_fftrail_txn.h"
#include "storage/trail/data/ripple_fftrail_txnmetadata.h"
#include "storage/trail/data/ripple_fftrail_txncommit.h"
#include "works/ripple_workthreadmgr.h"

/* 在私有域中删除 */
static bool ripple_fftrail_txnmetadata_remove(ripple_fftrail_privdata* privdata,
                                                ripple_catalogdata* catalogdata,
                                                Oid dboid)
{
    bool found = false;
    List*   ls = NULL;
    ListCell* lc = NULL;
    ripple_fftrail_table_serialentry* sentry = NULL;
    ripple_fftrail_table_serialentry* lcsentry = NULL;
    ripple_fftrail_table_serialkey skey = { 0 };

    if(RIPPLE_CATALOG_TYPE_ATTRIBUTE != catalogdata->type
        && RIPPLE_CATALOG_TYPE_CLASS != catalogdata->type
        && RIPPLE_CATALOG_TYPE_INDEX != catalogdata->type)
    {
        return true;
    }

    skey.dbid = dboid;

    if(RIPPLE_CATALOG_TYPE_ATTRIBUTE == catalogdata->type)
    {
        ripple_catalog_attribute_value* attrvalue = NULL;
        
        attrvalue = (ripple_catalog_attribute_value*)catalogdata->catalog;
        skey.tbid = attrvalue->attrelid;
    }
    else if (RIPPLE_CATALOG_TYPE_CLASS == catalogdata->type)
    {
        ripple_catalog_class_value* classvalue = NULL;
        classvalue = (ripple_catalog_class_value*)catalogdata->catalog;
        skey.tbid = classvalue->oid;
    }
    else if (RIPPLE_CATALOG_TYPE_INDEX == catalogdata->type)
    {
        ripple_catalog_index_value* indexvalue = NULL;
        indexvalue = (ripple_catalog_index_value*)catalogdata->catalog;
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
        lcsentry = (ripple_fftrail_table_serialentry*)lfirst(lc);
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
bool ripple_fftrail_txnmetadata(void* data, void* state)
{
    bool equal = false;
    ListCell* lc = NULL;
    ripple_txnstmt* rstmt = NULL;                      /* 需要写入 trail 文件的内容 */
    ripple_ff_txndata*  txndata = NULL;
    ripple_ffsmgr_state* ffstate = NULL;            /* state 数据信息 */
    ripple_txnstmt_metadata* metadatastmt = NULL;
    ripple_catalogdata* catalogdata = NULL;
    ripple_fftrail_privdata* privdata = NULL;
    Oid dboid = InvalidOid;

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;
    metadatastmt = (ripple_txnstmt_metadata*)rstmt->stmt;
    ffstate = (ripple_ffsmgr_state*)state;

    /* 在privdata中清理数据 */
    privdata = (ripple_fftrail_privdata*)ffstate->fdata->ffdata;
    lc = metadatastmt->begin;

    dboid = ffstate->callback.getdboid(ffstate->privdata);

    while(1)
    {
        /* 只有 update 和 delete 才需要更新 trail 文件的系统表信息 */
        catalogdata = (ripple_catalogdata*)lfirst(lc);
        switch (catalogdata->op)
        {
            case RIPPLE_CATALOG_OP_NOP:
                break;
            default:
                /* 应用 */
                ripple_fftrail_txnmetadata_remove(privdata, catalogdata, dboid);
                break;
        }

        /* 应用到系统表中 */
        ffstate->callback.catalog2transcache(ffstate->privdata, (void*)lc);
        
        /* 保存两个 checkpoint 之间的系统表变更 */
        if (NULL != ffstate->callback.setredosysdicts)
        {
            ffstate->callback.setredosysdicts(ffstate->privdata, (void*)catalogdata);
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

