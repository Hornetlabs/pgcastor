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
#include "catalog/enum.h"

void enum_getfromdb(PGconn* conn, cache_sysdicts* sysdicts)
{
    int                     i, j;
    HASHCTL                 hash_ctl;
    bool                    found = false;
    PGresult*               res = NULL;
    pg_sysdict_Form_pg_enum enum_obj;
    catalog_enum_value*     entry = NULL;
    const char*             query = "SELECT rel.oid,rel.enumtypid, rel.enumlabel FROM pg_enum rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(pg_parser_sysdict_pgenum);
    sysdicts->by_enum = hash_create("catalog_enum_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    res = conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_enum query failed");
    }

    // Print row data
    for (i = 0; i < PQntuples(res); i++)
    {
        enum_obj = (pg_sysdict_Form_pg_enum)rmalloc0(sizeof(pg_parser_sysdict_pgenum));
        if (NULL == enum_obj)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(enum_obj, 0, '\0', sizeof(pg_parser_sysdict_pgenum));
        j = 0;
        sscanf(PQgetvalue(res, i, j++), "%u", &enum_obj->oid);
        sscanf(PQgetvalue(res, i, j++), "%u", &enum_obj->enumtypid);
        strcpy(enum_obj->enumlabel.data, PQgetvalue(res, i, j++));

        entry = (catalog_enum_value*)hash_search(
            sysdicts->by_enum, &enum_obj->enumtypid, HASH_ENTER, &found);
        if (!found)
        {
            entry->enums = NIL;
        }
        entry->enumtypid = enum_obj->enumtypid;
        entry->enums = lappend(entry->enums, enum_obj);
    }

    PQclear(res);

    return;
}

void enumdata_write(List* enum_list, uint64* offset, sysdict_header_array* array)
{
    int                     fd;
    uint64                  page_num = 0;
    uint64                  page_offset = 0;
    ListCell*               cell = NULL;
    char                    buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_enum enum_data = NULL;

    array->type = CATALOG_TYPE_ENUM;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }

    foreach (cell, enum_list)
    {
        enum_data = (pg_sysdict_Form_pg_enum)lfirst(cell);

        if (page_offset + sizeof(pg_parser_sysdict_pgenum) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, enum_data, sizeof(pg_parser_sysdict_pgenum));
        page_offset += sizeof(pg_parser_sysdict_pgenum);
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

HTAB* enumcache_load(sysdict_header_array* array)
{
    int                     r = 0;
    int                     fd = -1;
    HTAB*                   enumhtab;
    HASHCTL                 hash_ctl;
    bool                    found = false;
    uint64                  fileoffset = 0;
    char                    buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_enum rippleenum;
    catalog_enum_value*     entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_enum_value);
    enumhtab = hash_create("catalog_enum_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_ENUM - 1].len == array[CATALOG_TYPE_ENUM - 1].offset)
    {
        return enumhtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_ENUM - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0)
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgclass) < FILE_BLK_SIZE)
        {
            rippleenum = (pg_sysdict_Form_pg_enum)rmalloc1(sizeof(pg_parser_sysdict_pgenum));
            if (NULL == rippleenum)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(rippleenum, 0, '\0', sizeof(pg_parser_sysdict_pgenum));
            rmemcpy0(rippleenum, 0, buffer + offset, sizeof(pg_parser_sysdict_pgenum));
            entry = (catalog_enum_value*)hash_search(
                enumhtab, &rippleenum->enumtypid, HASH_ENTER, &found);
            if (!found)
            {
                entry->enums = NIL;
            }
            entry->enumtypid = rippleenum->enumtypid;
            entry->enums = lappend(entry->enums, rippleenum);
            offset += sizeof(pg_parser_sysdict_pgenum);
            if (fileoffset + offset == array[CATALOG_TYPE_ENUM - 1].len)
            {
                if (osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return enumhtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return enumhtab;
}

/* colvalue2enum */
catalogdata* enum_colvalue2enum(void* in_colvalue)
{
    catalogdata*                    catalog_data = NULL;
    catalog_enum_value*             enumvalue = NULL;
    pg_sysdict_Form_pg_enum         pgenum = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;
    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;
    /* Value conversion */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if (NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));
    enumvalue = (catalog_enum_value*)rmalloc1(sizeof(catalog_enum_value));
    if (NULL == enumvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(enumvalue, 0, '\0', sizeof(catalog_enum_value));
    catalog_data->catalog = enumvalue;
    catalog_data->type = CATALOG_TYPE_ENUM;
    pgenum = (pg_sysdict_Form_pg_enum)rmalloc1(sizeof(pg_parser_sysdict_pgenum));
    if (NULL == pgenum)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgenum, 0, '\0', sizeof(pg_parser_sysdict_pgenum));
    enumvalue->enums = lappend(enumvalue->enums, pgenum);
    /* enumlabel 3 */
    rmemcpy1(
        pgenum->enumlabel.data, 0, (char*)((colvalue + 3)->m_value), (colvalue + 3)->m_valueLen);

    /* enumtypid 1 */
    sscanf((char*)((colvalue + 1)->m_value), "%u", &pgenum->enumtypid);
    enumvalue->enumtypid = pgenum->enumtypid;

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgenum->oid);

    return catalog_data;
}

/* catalogdata2transcache */
void enum_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool                    found = false;
    ListCell*               lc = NULL;
    List*                   newenumlist = NULL;
    catalog_enum_value*     newcatalog = NULL;
    catalog_enum_value*     catalogInHash = NULL;
    pg_sysdict_Form_pg_enum pgenum = NULL;
    pg_sysdict_Form_pg_enum pgenumInHash = NULL;
    pg_sysdict_Form_pg_enum duppgenum = NULL;

    if (NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    /* Get attribute structure from catalog */
    newcatalog = (catalog_enum_value*)(catalogdata->catalog);
    pgenum = (pg_sysdict_Form_pg_enum)lfirst(list_head(newcatalog->enums));

    elog(RLOG_DEBUG, "op:%d, %u, %s", catalogdata->op, pgenum->enumtypid, pgenum->enumlabel.data);
    if (CATALOG_OP_INSERT == catalogdata->op)
    {
        /* Insert */
        catalogInHash = hash_search(sysdicts->by_enum, &pgenum->enumtypid, HASH_ENTER, &found);
        if (false == found)
        {
            catalogInHash->enumtypid = pgenum->enumtypid;
            catalogInHash->enums = NIL;
        }

        duppgenum = (pg_sysdict_Form_pg_enum)rmalloc1(sizeof(pg_parser_sysdict_pgenum));
        if (NULL == duppgenum)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(duppgenum, 0, pgenum, sizeof(pg_parser_sysdict_pgenum));

        catalogInHash->enums = lappend(catalogInHash->enums, duppgenum);
    }
    else if (CATALOG_OP_UPDATE == catalogdata->op)
    {
        /* Update */
        catalogInHash = hash_search(sysdicts->by_enum, &pgenum->enumtypid, HASH_FIND, &found);
        if (NULL == catalogInHash)
        {
            elog(RLOG_WARNING,
                 "pgenum %u, %s can not fond in enum hash",
                 pgenum->enumtypid,
                 pgenum->enumlabel.data);
            return;
        }

        /* Traverse and update */
        foreach (lc, catalogInHash->enums)
        {
            pgenumInHash = (pg_sysdict_Form_pg_enum)lfirst(lc);
            if (strlen(pgenumInHash->enumlabel.data) != strlen(pgenum->enumlabel.data) ||
                0 != strcmp(pgenumInHash->enumlabel.data, pgenum->enumlabel.data))
            {
                continue;
            }

            /* Delete old and set new */
            rmemcpy0(pgenumInHash, 0, pgenum, sizeof(pg_parser_sysdict_pgenum));
            return;
        }

        /* No match found, which indicates an issue, directly append and log WARNING */
        elog(RLOG_WARNING,
             "enum %u, %s can not fond in enum hash",
             pgenum->enumtypid,
             pgenum->enumlabel.data);

        duppgenum = (pg_sysdict_Form_pg_enum)rmalloc1(sizeof(pg_parser_sysdict_pgenum));
        if (NULL == duppgenum)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(duppgenum, 0, pgenum, sizeof(pg_parser_sysdict_pgenum));

        catalogInHash->enums = lappend(catalogInHash->enums, duppgenum);
    }
    else if (CATALOG_OP_DELETE == catalogdata->op)
    {
        /* Delete */
        catalogInHash = hash_search(sysdicts->by_enum, &pgenum->enumtypid, HASH_FIND, &found);
        if (NULL == catalogInHash)
        {
            elog(RLOG_WARNING,
                 "enum %u, %s can not fond in enum hash",
                 pgenum->enumtypid,
                 pgenum->enumlabel.data);
            return;
        }

        foreach (lc, catalogInHash->enums)
        {
            pgenumInHash = (pg_sysdict_Form_pg_enum)lfirst(lc);
            if (strlen(pgenumInHash->enumlabel.data) != strlen(pgenum->enumlabel.data) ||
                0 != strcmp(pgenumInHash->enumlabel.data, pgenum->enumlabel.data))
            {
                newenumlist = lappend(newenumlist, pgenumInHash);
                continue;
            }
            rfree(pgenumInHash);
        }

        list_free(catalogInHash->enums);
        catalogInHash->enums = newenumlist;

        if (NULL == catalogInHash->enums)
        {
            /* If it is the last item, directly remove the hash entry */
            hash_search(sysdicts->by_enum, &catalogInHash->enumtypid, HASH_REMOVE, NULL);
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}
void enumcache_write(HTAB* enumscache, uint64* offset, sysdict_header_array* array)
{
    int                     fd;
    uint64                  page_num = 0;
    uint64                  page_offset = 0;
    List*                   enum_list = NULL;
    ListCell*               cell = NULL;
    HASH_SEQ_STATUS         status;
    char                    buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_enum rippleenum;
    catalog_enum_value*     entry = NULL;

    array[CATALOG_TYPE_ENUM - 1].type = CATALOG_TYPE_ENUM;
    array[CATALOG_TYPE_ENUM - 1].offset = *offset;
    page_num = *offset;

    hash_seq_init(&status, enumscache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        foreach (cell, entry->enums)
        {
            enum_list = lappend(enum_list, lfirst(cell));
        }
    }

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    foreach (cell, enum_list)
    {
        rippleenum = (pg_sysdict_Form_pg_enum)lfirst(cell);

        if (page_offset + sizeof(pg_parser_sysdict_pgenum) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, rippleenum, sizeof(pg_parser_sysdict_pgenum));
        page_offset += sizeof(pg_parser_sysdict_pgenum);
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
    array[CATALOG_TYPE_ENUM - 1].len = page_num;

    list_free(enum_list);
}

void enum_catalogdatafree(catalogdata* catalogdata)
{
    catalog_enum_value* catalog = NULL;
    if (NULL == catalogdata)
    {
        return;
    }

    if (NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    catalog = (catalog_enum_value*)catalogdata->catalog;
    list_free_deep(catalog->enums);

    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
