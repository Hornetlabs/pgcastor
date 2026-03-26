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
#include "catalog/type.h"

static bool bool_judgment(char* str)
{
    if (str[0] == 't' || str[0] == 'T')
    {
        return true;
    }
    else if (str[0] == 'f' || str[0] == 'F')
    {
        return false;
    }
    return -1;
}

void type_getfromdb(PGconn* conn, cache_sysdicts* sysdicts)
{
    int                     i, j;
    HASHCTL                 hash_ctl;
    bool                    found = false;
    PGresult*               res = NULL;
    catalog_type_value*     entry = NULL;
    pg_sysdict_Form_pg_type type = NULL;
    const char*             query =
        "SELECT rel.oid,rel.typname, rel.typlen,rel.typbyval, rel.typtype, rel.typdelim, "
        "rel.typelem, rel.typoutput::oid, rel.typrelid, rel.typalign, rel.typstorage FROM pg_type "
        "rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(catalog_type_value);
    sysdicts->by_type =
        hash_create("catalog_sysdicts_type", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    res = conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_type query failed");
    }

    /* Print row data */
    for (i = 0; i < PQntuples(res); i++)
    {
        type = (pg_sysdict_Form_pg_type)rmalloc0(sizeof(pg_parser_sysdict_pgtype));
        if (NULL == type)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(type, 0, '\0', sizeof(pg_parser_sysdict_pgtype));
        j = 0;
        sscanf(PQgetvalue(res, i, j++), "%u", &type->oid);
        strcpy(type->typname.data, PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%hd", &type->typlen);
        type->typbyval = bool_judgment(PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%c", &type->typtype);
        sscanf(PQgetvalue(res, i, j++), "%c", &type->typdelim);
        sscanf(PQgetvalue(res, i, j++), "%u", &type->typelem);
        sscanf(PQgetvalue(res, i, j++), "%u", &type->typoutput);
        sscanf(PQgetvalue(res, i, j++), "%u", &type->typrelid);
        sscanf(PQgetvalue(res, i, j++), "%c", &type->typalign);
        sscanf(PQgetvalue(res, i, j++), "%c", &type->typstorage);

        entry = hash_search(sysdicts->by_type, &type->oid, HASH_ENTER, &found);
        if (found)
        {
            elog(RLOG_ERROR, "type_oid:%u already exist in by_type", entry->type->oid);
        }
        entry->oid = type->oid;
        entry->type = type;
    }

    PQclear(res);
    return;
}

void typedata_write(List* type_list, uint64* offset, sysdict_header_array* array)
{
    int                     fd;
    uint64                  page_num = 0;
    uint64                  page_offset = 0;
    ListCell*               cell = NULL;
    char                    buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_type type;

    array->type = CATALOG_TYPE_TYPE;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }

    foreach (cell, type_list)
    {
        type = (pg_sysdict_Form_pg_type)lfirst(cell);
        if (page_offset + sizeof(pg_parser_sysdict_pgtype) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, type, sizeof(pg_parser_sysdict_pgtype));
        page_offset += sizeof(pg_parser_sysdict_pgtype);
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

HTAB* typecache_load(sysdict_header_array* array)
{
    int                     r = 0;
    int                     fd = -1;
    HTAB*                   typehtab;
    HASHCTL                 hash_ctl;
    bool                    found = false;
    uint64                  fileoffset = 0;
    char                    buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_type type;
    catalog_type_value*     entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_type_value);
    typehtab = hash_create("catalog_type_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_TYPE - 1].len == array[CATALOG_TYPE_TYPE - 1].offset)
    {
        return typehtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_TYPE - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0)
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgtype) < FILE_BLK_SIZE)
        {
            type = (pg_sysdict_Form_pg_type)rmalloc1(sizeof(pg_parser_sysdict_pgtype));
            if (NULL == type)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(type, 0, '\0', sizeof(pg_parser_sysdict_pgtype));
            rmemcpy0(type, 0, buffer + offset, sizeof(pg_parser_sysdict_pgtype));
            entry = hash_search(typehtab, &type->oid, HASH_ENTER, &found);
            if (found)
            {
                elog(RLOG_ERROR, "type_oid:%u already exist in by_type", entry->type->oid);
            }
            entry->oid = type->oid;
            entry->type = type;
            offset += sizeof(pg_parser_sysdict_pgtype);
            if (fileoffset + offset == array[CATALOG_TYPE_TYPE - 1].len)
            {
                if (osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return typehtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return typehtab;
}

/* colvalue2type */
catalogdata* type_colvalue2type(void* in_colvalue)
{
    catalogdata*                    catalog_data = NULL;
    catalog_type_value*             typevalue = NULL;
    pg_sysdict_Form_pg_type         pgtype = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* Value conversion */
    catalog_data = (catalogdata*)rmalloc0(sizeof(catalogdata));
    if (NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    typevalue = (catalog_type_value*)rmalloc0(sizeof(catalog_type_value));
    if (NULL == typevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(typevalue, 0, '\0', sizeof(catalog_type_value));
    catalog_data->catalog = typevalue;
    catalog_data->type = CATALOG_TYPE_TYPE;

    pgtype = (pg_sysdict_Form_pg_type)rmalloc1(sizeof(pg_parser_sysdict_pgtype));
    if (NULL == pgtype)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgtype, 0, '\0', sizeof(pg_parser_sysdict_pgtype));
    typevalue->type = pgtype;

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgtype->oid);
    typevalue->oid = pgtype->oid;

    /* typalign 21 */
    pgtype->typalign = ((char*)((colvalue + 21)->m_value))[0];

    /* typbyval 5 */
    pgtype->typbyval = ((char*)((colvalue + 5)->m_value))[0] == 't' ? true : false;

    /* typdelim 10 */
    pgtype->typdelim = ((char*)((colvalue + 10)->m_value))[0];

    /* typelem 12 */
    sscanf((char*)((colvalue + 12)->m_value), "%u", &pgtype->typelem);

    /* typlen 4 */
    sscanf((char*)((colvalue + 4)->m_value), "%hd", &pgtype->typlen);

    /* typname 1 */
    rmemcpy1(pgtype->typname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* typoutput 15 */
    sscanf((char*)((colvalue + 15)->m_value), "%u", &pgtype->typoutput);

    /* typrelid 11 */
    sscanf((char*)((colvalue + 11)->m_value), "%u", &pgtype->typrelid);

    /* typstorage 22 */
    pgtype->typstorage = ((char*)((colvalue + 22)->m_value))[0];

    /* typtype 6 */
    pgtype->typtype = ((char*)((colvalue + 6)->m_value))[0];

    return catalog_data;
}

/* colvalue2type----pg14 */
catalogdata* type_colvalue2type_pg14(void* in_colvalue)
{
    catalogdata*                    catalog_data = NULL;
    catalog_type_value*             typevalue = NULL;
    pg_sysdict_Form_pg_type         pgtype = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* Value conversion */
    catalog_data = (catalogdata*)rmalloc0(sizeof(catalogdata));
    if (NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    typevalue = (catalog_type_value*)rmalloc0(sizeof(catalog_type_value));
    if (NULL == typevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(typevalue, 0, '\0', sizeof(catalog_type_value));
    catalog_data->catalog = typevalue;
    catalog_data->type = CATALOG_TYPE_TYPE;

    pgtype = (pg_sysdict_Form_pg_type)rmalloc1(sizeof(pg_parser_sysdict_pgtype));
    if (NULL == pgtype)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgtype, 0, '\0', sizeof(pg_parser_sysdict_pgtype));
    typevalue->type = pgtype;

    /* oid 1 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgtype->oid);
    typevalue->oid = pgtype->oid;

    /* typalign 22 */
    pgtype->typalign = ((char*)((colvalue + 22)->m_value))[0];

    /* typbyval 5 */
    pgtype->typbyval = ((char*)((colvalue + 5)->m_value))[0] == 't' ? true : false;

    /* typdelim 10 */
    pgtype->typdelim = ((char*)((colvalue + 10)->m_value))[0];

    /* typelem 13 */
    sscanf((char*)((colvalue + 13)->m_value), "%u", &pgtype->typelem);

    /* typlen 4 */
    sscanf((char*)((colvalue + 4)->m_value), "%hd", &pgtype->typlen);

    /* typname 1 */
    rmemcpy1(pgtype->typname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* typoutput 16 */
    sscanf((char*)((colvalue + 16)->m_value), "%u", &pgtype->typoutput);

    /* typrelid 11 */
    sscanf((char*)((colvalue + 11)->m_value), "%u", &pgtype->typrelid);

    /* typstorage 23 */
    pgtype->typstorage = ((char*)((colvalue + 22)->m_value))[0];

    /* typtype 6 */
    pgtype->typtype = ((char*)((colvalue + 6)->m_value))[0];

    return catalog_data;
}

/* catalogdata2transcache */
void type_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool                found = false;
    catalog_type_value* newcatalog = NULL;
    catalog_type_value* catalogInHash = NULL;

    if (NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_type_value*)catalogdata->catalog;
    if (CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_type, &newcatalog->oid, HASH_ENTER, &found);
        if (true == found)
        {
            elog(RLOG_DEBUG,
                 "by_type hash duplicate oid, %u, %s",
                 catalogInHash->type->oid,
                 catalogInHash->type->typname.data);
            if (NULL != catalogInHash->type)
            {
                rfree(catalogInHash->type);
            }
        }
        catalogInHash->oid = newcatalog->oid;

        catalogInHash->type = (pg_sysdict_Form_pg_type)rmalloc1(sizeof(pg_parser_sysdict_pgtype));
        if (NULL == catalogInHash->type)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInHash->type, 0, newcatalog->type, sizeof(pg_parser_sysdict_pgtype));
    }
    else if (CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_type, &newcatalog->oid, HASH_REMOVE, &found);
        if (NULL != catalogInHash)
        {
            if (NULL != catalogInHash->type)
            {
                rfree(catalogInHash->type);
            }
        }
    }
    else if (CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_type, &newcatalog->oid, HASH_FIND, &found);
        if (NULL == catalogInHash)
        {
            elog(RLOG_WARNING,
                 "type %s,%u can not fond in type hash",
                 newcatalog->type->oid,
                 newcatalog->type->typname.data);
            return;
        }

        rfree(catalogInHash->type);

        catalogInHash->type = (pg_sysdict_Form_pg_type)rmalloc1(sizeof(pg_parser_sysdict_pgtype));
        if (NULL == catalogInHash->type)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInHash->type, 0, newcatalog->type, sizeof(pg_parser_sysdict_pgtype));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}
void typecache_write(HTAB* typecache, uint64* offset, sysdict_header_array* array)
{
    int                     fd;
    uint64                  page_num = 0;
    uint64                  page_offset = 0;
    HASH_SEQ_STATUS         status;
    char                    buffer[FILE_BLK_SIZE];
    catalog_type_value*     entry = NULL;
    pg_sysdict_Form_pg_type type = NULL;

    array[CATALOG_TYPE_TYPE - 1].type = CATALOG_TYPE_TYPE;
    array[CATALOG_TYPE_TYPE - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status, typecache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        type = entry->type;

        if (page_offset + sizeof(pg_parser_sysdict_pgtype) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, type, sizeof(pg_parser_sysdict_pgtype));
        page_offset += sizeof(pg_parser_sysdict_pgtype);
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
    array[CATALOG_TYPE_TYPE - 1].len = page_num;
}

/* Get pg_type data by oid */
void* type_getbyoid(Oid oid, HTAB* by_type)
{
    bool                found = false;
    catalog_type_value* typeentry = NULL;
    typeentry = hash_search(by_type, &oid, HASH_FIND, &found);
    if (false == found)
    {
        return NULL;
    }
    return (void*)typeentry->type;
}

void type_catalogdatafree(catalogdata* catalogdata)
{
    catalog_type_value* catalog = NULL;
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
    catalog = (catalog_type_value*)catalogdata->catalog;
    if (NULL != catalog->type)
    {
        rfree(catalog->type);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
