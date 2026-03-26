#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/conn.h"
#include "utils/hash/hash_utils.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/authid.h"

void authid_getfromdb(PGconn* conn, cache_sysdicts* sysdicts)
{
    int                       i, j;
    bool                      found = false;
    HASHCTL                   hash_ctl;
    PGresult*                 res = NULL;
    pg_sysdict_Form_pg_authid authid;
    catalog_authid_value*     entry = NULL;

    const char*               query =
        "SELECT rel.oid, rel.rolname, rel.rolsuper, rel.rolinherit, rel.rolcreaterole, "
        "rel.rolcreatedb, rel.rolcanlogin, rel.rolreplication, rel.rolbypassrls, rel.rolconnlimit "
        "FROM pg_authid rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(catalog_authid_value);
    sysdicts->by_authid =
        hash_create("catalog_sysdict_authid", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    res = conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "authid query failed");
    }

    for (i = 0; i < PQntuples(res); i++)
    {
        authid = (pg_sysdict_Form_pg_authid)rmalloc0(sizeof(pg_parser_sysdict_pgauthid));
        if (NULL == authid)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(authid, 0, '\0', sizeof(pg_parser_sysdict_pgauthid));
        j = 0;
        sscanf(PQgetvalue(res, i, j++), "%u", &authid->oid);
        strcpy(authid->rolname.data, PQgetvalue(res, i, j++));
        authid->rolsuper = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolinherit = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolcreaterole = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolcreatedb = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolcanlogin = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolreplication = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolbypassrls = strcmp(PQgetvalue(res, i, j++), "t") == 0 ? true : false;
        authid->rolconnlimit = atoi(PQgetvalue(res, i, j++));

        entry = hash_search(sysdicts->by_authid, &authid->oid, HASH_ENTER, &found);
        if (found)
        {
            elog(RLOG_ERROR, "authid_oid:%u already exist in by_authid", entry->authid->oid);
        }
        entry->oid = authid->oid;
        entry->authid = authid;
    }

    PQclear(res);

    return;
}

void authiddata_write(List* authid_list, uint64* offset, sysdict_header_array* array)
{
    int                       fd;
    uint64                    page_num = 0;
    uint64                    page_offset = 0;
    ListCell*                 cell = NULL;
    char                      buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_authid authid = NULL;

    array->type = CATALOG_TYPE_AUTHID;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }
    foreach (cell, authid_list)
    {
        authid = (pg_sysdict_Form_pg_authid)lfirst(cell);

        if (page_offset + sizeof(pg_parser_sysdict_pgauthid) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, authid, sizeof(pg_parser_sysdict_pgauthid));
        page_offset += sizeof(pg_parser_sysdict_pgauthid);
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

HTAB* authidcache_load(sysdict_header_array* array)
{
    int                       r = 0;
    int                       fd = -1;
    HTAB*                     authidhtab;
    HASHCTL                   hash_ctl;
    bool                      found = false;
    uint64                    fileoffset = 0;
    char                      buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_authid authid;
    catalog_authid_value*     entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_authid_value);
    authidhtab = hash_create("catalog_authid_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_AUTHID - 1].len == array[CATALOG_TYPE_AUTHID - 1].offset)
    {
        return authidhtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_AUTHID - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0)
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgauthid) < FILE_BLK_SIZE)
        {
            authid = (pg_sysdict_Form_pg_authid)rmalloc1(sizeof(pg_parser_sysdict_pgauthid));
            if (NULL == authid)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(authid, 0, '\0', sizeof(pg_parser_sysdict_pgauthid));
            rmemcpy0(authid, 0, buffer + offset, sizeof(pg_parser_sysdict_pgauthid));
            entry = hash_search(authidhtab, &authid->oid, HASH_ENTER, &found);
            if (found)
            {
                elog(RLOG_ERROR, "authid_oid:%u already exist in by_authid", entry->authid->oid);
            }
            entry->oid = authid->oid;
            entry->authid = authid;
            offset += sizeof(pg_parser_sysdict_pgauthid);
            if (fileoffset + offset == array[CATALOG_TYPE_AUTHID - 1].len)
            {
                if (osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return authidhtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }
    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return authidhtab;
}

void authidcache_write(HTAB* authidcache, uint64* offset, sysdict_header_array* array)
{
    int                       fd;
    uint64                    page_num = 0;
    uint64                    page_offset = 0;
    HASH_SEQ_STATUS           status;
    char                      buffer[FILE_BLK_SIZE];
    catalog_authid_value*     entry = NULL;
    pg_sysdict_Form_pg_authid authid = NULL;

    array[CATALOG_TYPE_AUTHID - 1].type = CATALOG_TYPE_AUTHID;
    array[CATALOG_TYPE_AUTHID - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }
    hash_seq_init(&status, authidcache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        authid = entry->authid;

        if (page_offset + sizeof(pg_parser_sysdict_pgauthid) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, authid, sizeof(pg_parser_sysdict_pgauthid));
        page_offset += sizeof(pg_parser_sysdict_pgauthid);
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
    array[CATALOG_TYPE_AUTHID - 1].len = page_num;
}

/* colvalue2authid */
catalogdata* authid_colvalue2authid(void* in_colvalue)
{
    catalogdata*                    catalog_data = NULL;
    catalog_authid_value*           authidvalue = NULL;
    pg_sysdict_Form_pg_authid       pgauthid = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* Value conversion */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if (NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    authidvalue = (catalog_authid_value*)rmalloc1(sizeof(catalog_authid_value));
    if (NULL == authidvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(authidvalue, 0, '\0', sizeof(catalog_authid_value));
    catalog_data->catalog = authidvalue;
    catalog_data->type = CATALOG_TYPE_AUTHID;

    pgauthid = (pg_sysdict_Form_pg_authid)rmalloc1(sizeof(pg_parser_sysdict_pgauthid));
    if (NULL == pgauthid)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgauthid, 0, '\0', sizeof(pg_parser_sysdict_pgauthid));
    authidvalue->authid = pgauthid;

    /* oid 1 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgauthid->oid);
    authidvalue->oid = pgauthid->oid;

    /* rolbypassrls 8 */
    pgauthid->rolbypassrls = ((char*)((colvalue + 8)->m_value))[0] == 't' ? true : false;

    /* rolcanlogin 6 */
    pgauthid->rolcanlogin = ((char*)((colvalue + 6)->m_value))[0] == 't' ? true : false;

    /* rolconnlimit 9 */
    sscanf((char*)((colvalue + 9)->m_value), "%d", &pgauthid->rolconnlimit);

    /* rolcreatedb 5 */
    pgauthid->rolcreatedb = ((char*)((colvalue + 5)->m_value))[0] == 't' ? true : false;

    /* rolcreaterole 4 */
    pgauthid->rolcreaterole = ((char*)((colvalue + 4)->m_value))[0] == 't' ? true : false;

    /* rolinherit 3 */
    pgauthid->rolinherit = ((char*)((colvalue + 4)->m_value))[0] == 't' ? true : false;

    /* rolname 1 */
    rmemcpy1(
        pgauthid->rolname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* rolreplication 7 */
    pgauthid->rolreplication = ((char*)((colvalue + 7)->m_value))[0] == 't' ? true : false;

    /* rolsuper 2 */
    pgauthid->rolsuper = ((char*)((colvalue + 2)->m_value))[0] == 't' ? true : false;

    return catalog_data;
}

/* catalogdata2transcache */
void authid_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool                  found = false;
    catalog_authid_value* newcatalog = NULL;
    catalog_authid_value* catalogInHash = NULL;

    if (NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_authid_value*)catalogdata->catalog;
    if (CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_authid, &newcatalog->oid, HASH_ENTER, &found);
        if (true == found)
        {
            elog(RLOG_WARNING,
                 "by_authid hash duplicate oid, %u, %u",
                 catalogInHash->authid->oid,
                 catalogInHash->authid->rolname.data);

            if (NULL != catalogInHash->authid)
            {
                rfree(catalogInHash->authid);
            }
        }
        catalogInHash->oid = newcatalog->oid;
        catalogInHash->authid =
            (pg_sysdict_Form_pg_authid)rmalloc1(sizeof(pg_parser_sysdict_pgauthid));
        if (NULL == catalogInHash->authid)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInHash->authid, 0, newcatalog->authid, sizeof(pg_parser_sysdict_pgauthid));
    }
    else if (CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_authid, &newcatalog->oid, HASH_REMOVE, &found);
        if (NULL != catalogInHash)
        {
            if (NULL != catalogInHash->authid)
            {
                rfree(catalogInHash->authid);
            }
        }
    }
    else if (CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_authid, &newcatalog->oid, HASH_FIND, &found);
        if (NULL == catalogInHash)
        {
            elog(RLOG_WARNING,
                 "authid hash duplicate oid, %u, %s",
                 newcatalog->authid->oid,
                 newcatalog->authid->rolname.data);
            return;
        }
        rfree(catalogInHash->authid);

        catalogInHash->authid =
            (pg_sysdict_Form_pg_authid)rmalloc1(sizeof(pg_parser_sysdict_pgauthid));
        if (NULL == catalogInHash->authid)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInHash->authid, 0, newcatalog->authid, sizeof(pg_parser_sysdict_pgauthid));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

/* Release */
void authid_catalogdatafree(catalogdata* catalogdata)
{
    catalog_authid_value* catalog = NULL;
    if (NULL == catalogdata)
    {
        return;
    }

    if (NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    /* catalog memory release */
    catalog = (catalog_authid_value*)catalogdata->catalog;
    if (NULL != catalog->authid)
    {
        rfree(catalog->authid);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
