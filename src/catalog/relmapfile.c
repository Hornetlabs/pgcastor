#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/relmapfile.h"

/* Apply relmap to hash */
void relmapfile_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    int                  index = 0;
    RelFileNode          relfilenode = {0};
    replmapfile*         relmapfile = NULL;
    catalog_class_value* classInHash = NULL;
    relfilenode2oid*     prelfilenode2oid = NULL;

    if (NULL == sysdicts)
    {
        return;
    }

    if (CATALOG_TYPE_RELMAPFILE != catalogdata->type)
    {
        elog(RLOG_ERROR, "xsynch logical error");
    }
    relmapfile = (replmapfile*)catalogdata->catalog;

    for (index = 0; index < relmapfile->num; index++)
    {
        /* Get class data from by_class by oid, then change relfilenode in class */
        classInHash = hash_search(sysdicts->by_class, &(relmapfile->mapping + index)->mapoid,
                                  HASH_FIND, NULL);
        if (NULL == classInHash)
        {
            continue;
        }

        if (classInHash->class->relfilenode == (relmapfile->mapping + index)->mapfilenode)
        {
            continue;
        }

        relfilenode.relNode = classInHash->class->relfilenode;
        relfilenode.dbNode = misc_controldata_database_get(NULL);
        relfilenode.spcNode = classInHash->class->reltablespace;

        classInHash->class->relfilenode = (relmapfile->mapping + index)->mapfilenode;

        /* by_relfilenode is empty during formatting */
        if (NULL != sysdicts->by_relfilenode)
        {
            /* Update relfilenode2oid */
            hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_REMOVE, NULL);

            /* Add new */
            relfilenode.relNode = classInHash->class->relfilenode;
            prelfilenode2oid =
                hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_ENTER, NULL);
            if (NULL == prelfilenode2oid)
            {
                elog(RLOG_ERROR, "put relfilenode2oid hash error, %u.%u.%u, oid:%u",
                     relfilenode.spcNode, relfilenode.dbNode, relfilenode.relNode,
                     classInHash->class->oid);
            }
            rmemcpy1(&prelfilenode2oid->relfilenode, 0, &relfilenode, sizeof(RelFileNode));
            prelfilenode2oid->oid = classInHash->class->oid;
        }
    }
}

/* Release */
void relmapfile_catalogdatafree(catalogdata* catalogdata)
{
    replmapfile* relmapfile = NULL;
    if (NULL == catalogdata)
    {
        return;
    }

    if (NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    /* Catalog memory release */
    relmapfile = (replmapfile*)catalogdata->catalog;
    if (NULL != relmapfile->mapping)
    {
        rfree(relmapfile->mapping);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
