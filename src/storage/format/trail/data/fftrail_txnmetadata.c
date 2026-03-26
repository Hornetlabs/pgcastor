#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
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

/* remove from private data field */
static bool fftrail_txnmetadata_remove(fftrail_privdata* privdata,
                                       catalogdata*      catalogdata,
                                       Oid               dboid)
{
    bool                       found = false;
    List*                      ls = NULL;
    ListCell*                  lc = NULL;
    fftrail_table_serialentry* sentry = NULL;
    fftrail_table_serialentry* lcsentry = NULL;
    fftrail_table_serialkey    skey = {0};

    if (CATALOG_TYPE_ATTRIBUTE != catalogdata->type && CATALOG_TYPE_CLASS != catalogdata->type &&
        CATALOG_TYPE_INDEX != catalogdata->type)
    {
        return true;
    }

    skey.dbid = dboid;

    if (CATALOG_TYPE_ATTRIBUTE == catalogdata->type)
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
    if (false == found)
    {
        return true;
    }

    /* in linked listmiddle delete */
    foreach (lc, privdata->tbentrys)
    {
        lcsentry = (fftrail_table_serialentry*)lfirst(lc);
        if (sentry == lcsentry)
        {
            continue;
        }
        ls = lappend(ls, lcsentry);
    }

    list_free(privdata->tbentrys);
    privdata->tbentrys = ls;
    return true;
}

/* systemsystemchardictionaryshould useand clean */
bool fftrail_txnmetadata(void* data, void* state)
{
    bool              equal = false;
    ListCell*         lc = NULL;
    txnstmt*          rstmt = NULL; /* need needwrite trail file innercontent */
    ff_txndata*       txndata = NULL;
    ffsmgr_state*     ffstate = NULL; /* state datainfo */
    txnstmt_metadata* metadatastmt = NULL;
    catalogdata*      catalog_data = NULL;
    fftrail_privdata* privdata = NULL;
    Oid               dboid = INVALIDOID;

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    metadatastmt = (txnstmt_metadata*)rstmt->stmt;
    ffstate = (ffsmgr_state*)state;

    /* inprivdatamiddlecleandata */
    privdata = (fftrail_privdata*)ffstate->fdata->ffdata;
    lc = metadatastmt->begin;

    dboid = ffstate->callback.getdboid(ffstate->privdata);

    while (1)
    {
        /* only has update and delete talentneed needupdate trail file systemsystemtableinfo */
        catalog_data = (catalogdata*)lfirst(lc);
        switch (catalog_data->op)
        {
            case CATALOG_OP_NOP:
                break;
            default:
                /* should use */
                fftrail_txnmetadata_remove(privdata, catalog_data, dboid);
                break;
        }

        /* should useto systemsystemtablemiddle */
        ffstate->callback.catalog2transcache(ffstate->privdata, (void*)lc);

        /* savetwo checkpoint ofbetween systemsystemtablechangemore */
        if (NULL != ffstate->callback.setredosysdicts)
        {
            ffstate->callback.setredosysdicts(ffstate->privdata, (void*)catalog_data);
            lfirst(lc) = NULL;
        }

        /* only has one */
        if (lc == metadatastmt->end || true == equal)
        {
            break;
        }

        /* verifyis whetherto reachlast */
        lc = lc->next;
        if (lc == metadatastmt->end)
        {
            equal = true;
        }
    }

    return true;
}
