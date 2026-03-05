#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_relmapfile.h"


/* 将 relmap 应用到 hash 中 */
void ripple_relmapfile_catalogdata2transcache(ripple_cache_sysdicts* sysdicts,
                                                ripple_catalogdata* catalogdata)
{
    int index = 0;
    RelFileNode relfilenode = { 0 };
    ripple_replmapfile* relmapfile = NULL;
    ripple_catalog_class_value* classInHash = NULL;
    ripple_relfilenode2oid* prelfilenode2oid = NULL;

    if(NULL == sysdicts)
    {
        return;
    }

    if(RIPPLE_CATALOG_TYPE_RELMAPFILE != catalogdata->type)
    {
        elog(RLOG_ERROR, "xsynch logical error");
    }
    relmapfile = (ripple_replmapfile*)catalogdata->catalog;

    for(index = 0; index < relmapfile->num; index++)
    {
        /* 根据 oid 在 by_class 中获取 class 数据，然后更改 class 中的 relfilenode */
        classInHash = hash_search(sysdicts->by_class, &(relmapfile->mapping + index)->mapoid, HASH_FIND, NULL);
        if(NULL == classInHash)
        {
            continue;
        }

        if(classInHash->ripple_class->relfilenode == (relmapfile->mapping + index)->mapfilenode)
        {
            continue;
        }

        relfilenode.relNode = classInHash->ripple_class->relfilenode;
        relfilenode.dbNode = ripple_misc_controldata_database_get(NULL);
        relfilenode.spcNode = classInHash->ripple_class->reltablespace;

        classInHash->ripple_class->relfilenode = (relmapfile->mapping + index)->mapfilenode;

        /* 在格式化时，by_relfilenode为空 */
        if(NULL != sysdicts->by_relfilenode)
        {
            /* 更改 relfilenode2oid */
            hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_REMOVE, NULL);

            /* 添加新的 */
            relfilenode.relNode = classInHash->ripple_class->relfilenode;
            prelfilenode2oid = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_ENTER, NULL);
            if(NULL == prelfilenode2oid)
            {
                elog(RLOG_ERROR, "put relfilenode2oid hash error, %u.%u.%u, oid:%u",
                                    relfilenode.spcNode,
                                    relfilenode.dbNode,
                                    relfilenode.relNode,
                                    classInHash->ripple_class->oid);
            }
            rmemcpy1(&prelfilenode2oid->relfilenode, 0, &relfilenode, sizeof(RelFileNode));
            prelfilenode2oid->oid = classInHash->ripple_class->oid;
        }
    }
}



/* 释放 */
void ripple_relmapfile_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_replmapfile* relmapfile = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    if(NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    /* catalog 内存释放 */
    relmapfile = (ripple_replmapfile*)catalogdata->catalog;
    if(NULL != relmapfile->mapping)
    {
        rfree(relmapfile->mapping);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

