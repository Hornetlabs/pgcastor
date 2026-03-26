#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/conn.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"

void namespace_getfromdb(PGconn* conn, cache_sysdicts* sysdicts)
{
    int                          i, j;
    HASHCTL                      hash_ctl;
    bool                         found = false;
    PGresult*                    res = NULL;
    catalog_namespace_value*     entry = NULL;
    pg_sysdict_Form_pg_namespace namespace = NULL;
    const char*                  query = "SELECT rel.oid,rel.nspname FROM pg_namespace rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_namespace_value);
    sysdicts->by_namespace =
        hash_create("catalog_sysdicts_namespace", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    res = conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_namespace query failed");
    }

    /* Print row data */
    for (i = 0; i < PQntuples(res); i++)
    {
        namespace = (pg_sysdict_Form_pg_namespace)rmalloc0(sizeof(pg_parser_sysdict_pgnamespace));
        if (NULL == namespace)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(namespace, 0, '\0', sizeof(pg_parser_sysdict_pgnamespace));
        j = 0;
        sscanf(PQgetvalue(res, i, j++), "%u", &namespace->oid);
        strcpy(namespace->nspname.data, PQgetvalue(res, i, j++));

        entry = hash_search(sysdicts->by_namespace, &namespace->oid, HASH_ENTER, &found);
        if (found)
        {
            elog(RLOG_ERROR,
                 "namespace_oid:%u already exist in by_namespace",
                 entry->namespace->oid);
        }
        entry->oid = namespace->oid;
        entry->namespace = namespace;
    }

    PQclear(res);
    return;
}

void namespacedata_write(List* namespace, uint64* offset, sysdict_header_array* array)
{
    int                          fd;
    uint64                       page_num = 0;
    uint64                       page_offset = 0;
    ListCell*                    cell = NULL;
    char                         buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_namespace riplenamespace;

    array->type = CATALOG_TYPE_NAMESPACE;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }

    foreach (cell, namespace)
    {
        riplenamespace = (pg_sysdict_Form_pg_namespace)lfirst(cell);

        if (page_offset + sizeof(pg_parser_sysdict_pgnamespace) > FILE_BLK_SIZE)
        {
            if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
            {
                elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
                osal_file_close(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += FILE_BLK_SIZE;
            page_offset = 0;
        }
        rmemcpy1(buffer, page_offset, riplenamespace, sizeof(pg_parser_sysdict_pgnamespace));
        page_offset += sizeof(pg_parser_sysdict_pgnamespace);
    }

    if (page_offset > 0)
    {
        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
        {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }

    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_FILE);
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    array->len = page_num;
}

HTAB* namespacecache_load(sysdict_header_array* array)
{
    int                          r = 0;
    int                          fd = -1;
    HTAB*                        namespacehtab;
    HASHCTL                      hash_ctl;
    bool                         found = false;
    uint64                       fileoffset = 0;
    char                         buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_namespace namespace;
    catalog_namespace_value*     entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_namespace_value);
    namespacehtab = hash_create("catalog_namespace_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_NAMESPACE - 1].len == array[CATALOG_TYPE_NAMESPACE - 1].offset)
    {
        return namespacehtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_NAMESPACE - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0)
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgnamespace) < FILE_BLK_SIZE)
        {
            namespace =
                (pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(pg_parser_sysdict_pgnamespace));
            if (NULL == namespace)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(namespace, 0, '\0', sizeof(pg_parser_sysdict_pgnamespace));
            rmemcpy0(namespace, 0, buffer + offset, sizeof(pg_parser_sysdict_pgnamespace));
            entry = hash_search(namespacehtab, &namespace->oid, HASH_ENTER, &found);
            if (found)
            {
                elog(RLOG_ERROR,
                     "namespace_oid:%u already exist in by_namespace",
                     entry->namespace->oid);
            }
            entry->oid = namespace->oid;
            entry->namespace = namespace;
            offset += sizeof(pg_parser_sysdict_pgnamespace);
            if (fileoffset + offset == array[CATALOG_TYPE_NAMESPACE - 1].len)
            {
                if (osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return namespacehtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return namespacehtab;
}

/* colvalue2namespace */
catalogdata* namespace_colvalue2namespace(void* in_colvalue)
{
    catalogdata*                    catalog_data = NULL;
    catalog_namespace_value*        namespacevalue = NULL;
    pg_sysdict_Form_pg_namespace    pgnamespace = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* Value conversion */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if (NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    namespacevalue = (catalog_namespace_value*)rmalloc0(sizeof(catalog_namespace_value));
    if (NULL == namespacevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(namespacevalue, 0, '\0', sizeof(catalog_namespace_value));
    catalog_data->catalog = namespacevalue;
    catalog_data->type = CATALOG_TYPE_NAMESPACE;

    pgnamespace = (pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(pg_parser_sysdict_pgnamespace));
    if (NULL == pgnamespace)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgnamespace, 0, '\0', sizeof(pg_parser_sysdict_pgnamespace));
    namespacevalue->namespace = pgnamespace;

    /* nspname 1 */
    rmemcpy1(
        pgnamespace->nspname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgnamespace->oid);
    namespacevalue->oid = pgnamespace->oid;

    return catalog_data;
}

/* catalogdata2transcache */
void namespace_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool                         found = false;
    catalog_namespace_value*     newcatalog = NULL;
    catalog_namespace_value*     catalogInHash = NULL;
    pg_sysdict_Form_pg_namespace duppgnsp = NULL;

    if (NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_namespace_value*)catalogdata->catalog;
    if (CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_namespace, &newcatalog->oid, HASH_ENTER, &found);
        if (true == found)
        {
            elog(RLOG_WARNING,
                 "by_namespace hash duplicate oid, %u, %s",
                 catalogInHash->namespace->oid,
                 catalogInHash->namespace->nspname.data);

            if (NULL != catalogInHash->namespace)
            {
                rfree(catalogInHash->namespace);
            }
        }

        duppgnsp = (pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(pg_parser_sysdict_pgnamespace));
        if (NULL == duppgnsp)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(duppgnsp, 0, newcatalog->namespace, sizeof(pg_parser_sysdict_pgnamespace));

        catalogInHash->oid = newcatalog->oid;
        catalogInHash->namespace = duppgnsp;
    }
    else if (CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_namespace, &newcatalog->oid, HASH_REMOVE, &found);
        if (NULL != catalogInHash)
        {
            if (NULL != catalogInHash->namespace)
            {
                rfree(catalogInHash->namespace);
            }
        }
    }
    else if (CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_namespace, &newcatalog->oid, HASH_FIND, &found);
        if (NULL == catalogInHash)
        {
            elog(RLOG_WARNING,
                 "namespace %s,%u can not fond in namespace hash",
                 newcatalog->namespace->oid,
                 newcatalog->namespace->nspname.data);
            return;
        }
        rfree(catalogInHash->namespace);

        duppgnsp = (pg_sysdict_Form_pg_namespace)rmalloc1(sizeof(pg_parser_sysdict_pgnamespace));
        if (NULL == duppgnsp)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(duppgnsp, 0, newcatalog->namespace, sizeof(pg_parser_sysdict_pgnamespace));

        catalogInHash->namespace = duppgnsp;
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void namespacecache_write(HTAB* namespacecache, uint64* offset, sysdict_header_array* array)
{
    int                          fd;
    uint64                       page_num = 0;
    uint64                       page_offset = 0;
    HASH_SEQ_STATUS              status;
    char                         buffer[FILE_BLK_SIZE];
    catalog_namespace_value*     entry = NULL;
    pg_sysdict_Form_pg_namespace riplenamespace = NULL;

    array[CATALOG_TYPE_NAMESPACE - 1].type = CATALOG_TYPE_NAMESPACE;
    array[CATALOG_TYPE_NAMESPACE - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status, namespacecache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        riplenamespace = entry->namespace;

        if (page_offset + sizeof(pg_parser_sysdict_pgnamespace) > FILE_BLK_SIZE)
        {
            if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
            {
                elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
                osal_file_close(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += FILE_BLK_SIZE;
            page_offset = 0;
        }
        rmemcpy1(buffer, page_offset, riplenamespace, sizeof(pg_parser_sysdict_pgnamespace));
        page_offset += sizeof(pg_parser_sysdict_pgnamespace);
    }

    if (page_offset > 0)
    {
        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
        {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }

    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_TMP_FILE);
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_TMP_FILE);
    }
    array[CATALOG_TYPE_NAMESPACE - 1].len = page_num;
}

void namespace_catalogdatafree(catalogdata* catalogdata)
{
    catalog_namespace_value* catalog = NULL;
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
    catalog = (catalog_namespace_value*)catalogdata->catalog;
    if (NULL != catalog->namespace)
    {
        rfree(catalog->namespace);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

/* Get pg_namespace data by oid */
void* namespace_getbyoid(Oid oid, HTAB* by_namespace)
{
    bool                     found = false;
    catalog_namespace_value* nspentry = NULL;
    nspentry = hash_search(by_namespace, &oid, HASH_FIND, &found);
    if (false == found)
    {
        return NULL;
    }
    return (void*)nspentry->namespace;
}
