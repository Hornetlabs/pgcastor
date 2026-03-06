#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/ripple_conn.h"
#include "utils/hash/hash_utils.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_authid.h"

void ripple_authid_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
    int i, j;
    bool found = false;
    HASHCTL hash_ctl;
    PGresult *res = NULL;
    xk_pg_sysdict_Form_pg_authid ripple_authid;
    ripple_catalog_authid_value *entry = NULL;

    const char *query = "SELECT rel.oid, rel.rolname, rel.rolsuper, rel.rolinherit, rel.rolcreaterole, rel.rolcreatedb, rel.rolcanlogin, rel.rolreplication, rel.rolbypassrls, rel.rolconnlimit FROM pg_authid rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ripple_catalog_authid_value);
    sysdicts->by_authid = hash_create("ripple_catalog_sysdict_authid", 2048, &hash_ctl,
                                        HASH_ELEM | HASH_BLOBS);

    res = ripple_conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "authid query failed");
    }
    

    // 打印行数据
    for (i = 0; i < PQntuples(res); i++)
    {
        ripple_authid = (xk_pg_sysdict_Form_pg_authid)rmalloc0(sizeof(xk_pg_parser_sysdict_pgauthid));
        if(NULL == ripple_authid)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(ripple_authid, 0, '\0', sizeof(xk_pg_parser_sysdict_pgauthid));
        j=0;
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_authid->oid);
        strcpy(ripple_authid->rolname.data ,PQgetvalue(res, i, j++));
        ripple_authid->rolsuper = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolinherit = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolcreaterole = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolcreatedb = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolcanlogin = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolreplication = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolbypassrls = strcmp(PQgetvalue(res, i, j++),"t") == 0 ? true : false;
        ripple_authid->rolconnlimit = atoi(PQgetvalue(res, i, j++));
        
        entry = hash_search(sysdicts->by_authid, &ripple_authid->oid, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "authid_oid:%u already exist in by_authid", entry->ripple_authid->oid);
        }
        entry->oid = ripple_authid->oid;
        entry->ripple_authid = ripple_authid;
    }

    PQclear(res);

    return;
}

void ripple_authiddata_write(List* authid_list, uint64 *offset, ripple_sysdict_header_array* array)
{
    int	 fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    ListCell*	cell = NULL;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_authid ripple_authid = NULL;

    array->type = RIPPLE_CATALOG_TYPE_AUTHID;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
    }
    foreach(cell, authid_list)
    {
        ripple_authid = (xk_pg_sysdict_Form_pg_authid) lfirst(cell);

        if(page_offset + sizeof(xk_pg_parser_sysdict_pgauthid) > RIPPLE_FILE_BLK_SIZE)
        {
            if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
                elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_FILE);
                FileClose(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += RIPPLE_FILE_BLK_SIZE;
            page_offset = 0;
        }
        rmemcpy1(buffer, page_offset, ripple_authid, sizeof(xk_pg_parser_sysdict_pgauthid));
        page_offset += sizeof(xk_pg_parser_sysdict_pgauthid);
    }
    if (page_offset > 0) {
        if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
            elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_FILE);
            FileClose(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += RIPPLE_FILE_BLK_SIZE;
    }
    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", RIPPLE_SYSDICTS_FILE);
    }
    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    array->len = page_num;

}

HTAB* ripple_authidcache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* authidhtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_authid authid;
    ripple_catalog_authid_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_authid_value);
    authidhtab = hash_create("ripple_catalog_authid_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

	if (array[RIPPLE_CATALOG_TYPE_AUTHID - 1].len == array[RIPPLE_CATALOG_TYPE_AUTHID - 1].offset)
	{
		return authidhtab;
	}

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_AUTHID - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgauthid) < RIPPLE_FILE_BLK_SIZE)
        {
            authid = (xk_pg_sysdict_Form_pg_authid)rmalloc1(sizeof(xk_pg_parser_sysdict_pgauthid));
            if(NULL == authid)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(authid, 0, '\0', sizeof(xk_pg_parser_sysdict_pgauthid));
            rmemcpy0(authid, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgauthid));
            entry = hash_search(authidhtab, &authid->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "authid_oid:%u already exist in by_authid", entry->ripple_authid->oid);
            }
            entry->oid = authid->oid;
            entry->ripple_authid = authid;
            offset += sizeof(xk_pg_parser_sysdict_pgauthid);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_AUTHID - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return authidhtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }
    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }
    
    return authidhtab;
}

void ripple_authidcache_write(HTAB* authidcache, uint64 *offset, ripple_sysdict_header_array* array)
{
    int	 fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    HASH_SEQ_STATUS status;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    ripple_catalog_authid_value *entry = NULL;
    xk_pg_sysdict_Form_pg_authid authid = NULL;

    array[RIPPLE_CATALOG_TYPE_AUTHID - 1].type = RIPPLE_CATALOG_TYPE_AUTHID;
    array[RIPPLE_CATALOG_TYPE_AUTHID - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }
    hash_seq_init(&status,authidcache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        authid = entry->ripple_authid;

        if(page_offset + sizeof(xk_pg_parser_sysdict_pgauthid) > RIPPLE_FILE_BLK_SIZE)
        {
            if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
                elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
                FileClose(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += RIPPLE_FILE_BLK_SIZE;
            page_offset = 0;
        }
        rmemcpy1(buffer, page_offset, authid, sizeof(xk_pg_parser_sysdict_pgauthid));
        page_offset += sizeof(xk_pg_parser_sysdict_pgauthid);
    }
    if (page_offset > 0) {
        if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, *offset) != RIPPLE_FILE_BLK_SIZE) {
            elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
            FileClose(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += RIPPLE_FILE_BLK_SIZE;
    }
    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }
    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }
    array[RIPPLE_CATALOG_TYPE_AUTHID - 1].len = page_num;
}

/* colvalue2authid */
ripple_catalogdata* ripple_authid_colvalue2authid(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_authid_value* authidvalue = NULL;
    xk_pg_sysdict_Form_pg_authid pgauthid = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    authidvalue = (ripple_catalog_authid_value*)rmalloc1(sizeof(ripple_catalog_authid_value));
    if(NULL == authidvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(authidvalue, 0, '\0', sizeof(ripple_catalog_authid_value));
    catalogdata->catalog = authidvalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_AUTHID;

    pgauthid = (xk_pg_sysdict_Form_pg_authid)rmalloc1(sizeof(xk_pg_parser_sysdict_pgauthid));
    if(NULL == pgauthid)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgauthid, 0, '\0', sizeof(xk_pg_parser_sysdict_pgauthid));
    authidvalue->ripple_authid = pgauthid;

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
    rmemcpy1(pgauthid->rolname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* rolreplication 7 */
    pgauthid->rolreplication = ((char*)((colvalue + 7)->m_value))[0] == 't' ? true : false;

    /* rolsuper 2 */
    pgauthid->rolsuper = ((char*)((colvalue + 2)->m_value))[0] == 't' ? true : false;

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_authid_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_authid_value* newcatalog = NULL;
    ripple_catalog_authid_value* catalogInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (ripple_catalog_authid_value*)catalogdata->catalog;
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_authid, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_authid hash duplicate oid, %u, %u",
                                catalogInHash->ripple_authid->oid,
                                catalogInHash->ripple_authid->rolname.data);

            if(NULL != catalogInHash->ripple_authid)
            {
                rfree(catalogInHash->ripple_authid);
            }
        }
        catalogInHash->oid = newcatalog->oid;
        catalogInHash->ripple_authid = (xk_pg_sysdict_Form_pg_authid)rmalloc1(sizeof(xk_pg_parser_sysdict_pgauthid));
        if(NULL == catalogInHash->ripple_authid)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInHash->ripple_authid, 0, newcatalog->ripple_authid, sizeof(xk_pg_parser_sysdict_pgauthid));
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_authid, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->ripple_authid)
            {
                rfree(catalogInHash->ripple_authid);
            }
        }
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_authid, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "ripple_authid hash duplicate oid, %u, %s",
                                newcatalog->ripple_authid->oid,
                                newcatalog->ripple_authid->rolname.data);
            return;
        }
        rfree(catalogInHash->ripple_authid);

        catalogInHash->ripple_authid = (xk_pg_sysdict_Form_pg_authid)rmalloc1(sizeof(xk_pg_parser_sysdict_pgauthid));
        if(NULL == catalogInHash->ripple_authid)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInHash->ripple_authid, 0, newcatalog->ripple_authid, sizeof(xk_pg_parser_sysdict_pgauthid));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}


/* 释放 */
void ripple_authid_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_authid_value* catalog = NULL;
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
    catalog = (ripple_catalog_authid_value*)catalogdata->catalog;
    if(NULL != catalog->ripple_authid)
    {
        rfree(catalog->ripple_authid);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
