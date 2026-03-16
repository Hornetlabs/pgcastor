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
#include "catalog/ripple_control.h"
#include "catalog/ripple_database.h"


void ripple_database_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
    int        i, j;
    HASHCTL hash_ctl;
    HASHCTL oid_hash_ctl;
    bool found = false;
    PGresult *res = NULL;
    xk_pg_sysdict_Form_pg_database ripple_database;
    ripple_catalog_database_value *entry = NULL;
    ripple_catalog_datname2oid_value *oid_entry = NULL;
    const char *query = "SELECT rel.oid, rel.datname, rel.datdba, rel.encoding, rel.datcollate, rel.datctype FROM pg_database rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ripple_catalog_database_value);
    sysdicts->by_database = hash_create("ripple_catalog_sysdicts_database", 2048, &hash_ctl,
                                        HASH_ELEM | HASH_BLOBS);

    rmemset1(&oid_hash_ctl, 0, '\0', sizeof(oid_hash_ctl));
    oid_hash_ctl.keysize = sizeof(xk_pg_parser_NameData);
    oid_hash_ctl.entrysize = sizeof(ripple_catalog_datname2oid_value);
    sysdicts->by_datname2oid = hash_create("ripple_catalog_datname2oid_value", 2048, &oid_hash_ctl,
                                            HASH_ELEM | HASH_BLOBS);

    res = ripple_conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_database query failed");
    }

    // 打印行数据
    for (i = 0; i < PQntuples(res); i++) 
    {
        ripple_database = (xk_pg_sysdict_Form_pg_database)rmalloc0(sizeof(xk_pg_parser_sysdict_pgdatabase));
        if(NULL == ripple_database)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(ripple_database, 0, '\0', sizeof(xk_pg_parser_sysdict_pgdatabase));
        j=0;
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_database->oid);
        strcpy(ripple_database->datname.data ,PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_database->datdba);
        sscanf(PQgetvalue(res, i, j++), "%d", &ripple_database->encoding);
        strcpy(ripple_database->datcollate.data ,PQgetvalue(res, i, j++));
        strcpy(ripple_database->datctype.data ,PQgetvalue(res, i, j++));

        entry = hash_search(sysdicts->by_database, &ripple_database->oid, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "database_oid:%u already exist in by_database", entry->ripple_database->oid);
        }
        entry->oid = ripple_database->oid;
        entry->ripple_database = ripple_database;

        oid_entry = (ripple_catalog_datname2oid_value *)hash_search(sysdicts->by_datname2oid, &ripple_database->datname, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "database_name:%s already exist in by_datname2oid", oid_entry->datname.data);
        }
        oid_entry->oid = ripple_database->oid;
        rmemcpy1(oid_entry->datname.data, 0, ripple_database->datname.data, sizeof(ripple_database->datname.data));
    }

    PQclear(res);

    return;
}

void ripple_databasedata_write(List* database_list, uint64 *offset, ripple_sysdict_header_array* array)
{
    int  fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    ListCell*	cell = NULL;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_database ripple_database = NULL;

    array->type = RIPPLE_CATALOG_TYPE_DATABASE;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
    }

    foreach(cell, database_list)
    {
        ripple_database = (xk_pg_sysdict_Form_pg_database) lfirst(cell);

        if(page_offset + sizeof(xk_pg_parser_sysdict_pgdatabase) > RIPPLE_FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, ripple_database, sizeof(xk_pg_parser_sysdict_pgdatabase));
        page_offset += sizeof(xk_pg_parser_sysdict_pgdatabase);
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

HTAB* ripple_databasecache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* databasehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_database database;
    ripple_catalog_database_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_database_value);
    databasehtab = hash_create("ripple_catalog_database_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    /*
    * Read data...
    */
    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_DATABASE - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgdatabase) < RIPPLE_FILE_BLK_SIZE)
        {
            database = (xk_pg_sysdict_Form_pg_database)rmalloc1(sizeof(xk_pg_parser_sysdict_pgdatabase));
            if(NULL == database)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(database, 0, '\0', sizeof(xk_pg_parser_sysdict_pgdatabase));
            rmemcpy0(database, 0,buffer + offset, sizeof(xk_pg_parser_sysdict_pgdatabase));
            entry = hash_search(databasehtab, &database->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "database_oid:%u already exist in by_database", entry->ripple_database->oid);
            }
            entry->oid = database->oid;
            entry->ripple_database = database;
            offset += sizeof(xk_pg_parser_sysdict_pgdatabase);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_DATABASE - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return databasehtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return databasehtab;
}

HTAB* ripple_datname2oid_cache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* databasehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_parser_sysdict_pgdatabase database;
    ripple_catalog_datname2oid_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(xk_pg_parser_NameData);
    hash_ctl.entrysize = sizeof(ripple_catalog_datname2oid_value);
    databasehtab = hash_create("ripple_catalog_datname2oid_value", 2048, &hash_ctl,
                                    HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_DATABASE - 1].len == array[RIPPLE_CATALOG_TYPE_DATABASE - 1].offset)
    {
        return databasehtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_DATABASE - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgdatabase) < RIPPLE_FILE_BLK_SIZE)
        {
            rmemset1(&database, 0, '\0', sizeof(xk_pg_parser_sysdict_pgdatabase));
            rmemcpy1(&database, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgdatabase));
            entry = (ripple_catalog_datname2oid_value *)hash_search(databasehtab, &database.datname, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "database_name:%s already exist in by_datname2oid", entry->datname.data);
            }
            entry->oid = database.oid;
            rmemcpy1(entry->datname.data, 0, database.datname.data, sizeof(database.datname.data));
            offset += sizeof(xk_pg_parser_sysdict_pgdatabase);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_DATABASE - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return databasehtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return databasehtab;
}

void ripple_databasecache_write(HTAB* databasecache, uint64 *offset, ripple_sysdict_header_array* array)
{
    int fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    HASH_SEQ_STATUS status;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    ripple_catalog_database_value *entry = NULL;
    xk_pg_sysdict_Form_pg_database database = NULL;

    array[RIPPLE_CATALOG_TYPE_DATABASE - 1].type = RIPPLE_CATALOG_TYPE_DATABASE;
    array[RIPPLE_CATALOG_TYPE_DATABASE - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status,databasecache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        database = entry->ripple_database;
        if(page_offset + sizeof(xk_pg_parser_sysdict_pgdatabase) > RIPPLE_FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, database, sizeof(xk_pg_parser_sysdict_pgdatabase));
        page_offset += sizeof(xk_pg_parser_sysdict_pgdatabase);
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

    array[RIPPLE_CATALOG_TYPE_DATABASE - 1].len = page_num;

}

/* colvalue2database */
ripple_catalogdata* ripple_database_colvalue2database(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_database_value* databasevalue = NULL;
    xk_pg_sysdict_Form_pg_database pgdatabase = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    databasevalue = (ripple_catalog_database_value*)rmalloc1(sizeof(ripple_catalog_database_value));
    if(NULL == databasevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(databasevalue, 0, '\0', sizeof(ripple_catalog_database_value));
    catalogdata->catalog = databasevalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_DATABASE;

    pgdatabase = (xk_pg_sysdict_Form_pg_database)rmalloc1(sizeof(xk_pg_parser_sysdict_pgdatabase));
    if(NULL == pgdatabase)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgdatabase, 0, '\0', sizeof(xk_pg_parser_sysdict_pgdatabase));
    databasevalue->ripple_database = pgdatabase;

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgdatabase->oid);
    databasevalue->oid = pgdatabase->oid;

    /* datname 1 */
    rmemcpy1(pgdatabase->datname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* datdba 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgdatabase->datdba);

    /* encoding 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%d", &pgdatabase->encoding);

    /* datcollate 4 */
    rmemcpy1(pgdatabase->datcollate.data, 0, (char*)((colvalue + 4)->m_value), (colvalue + 4)->m_valueLen);

    /* datcollate 5 */
    rmemcpy1(pgdatabase->datctype.data, 0, (char*)((colvalue + 5)->m_value), (colvalue + 5)->m_valueLen);

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_database_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_database_value* newcatalog = NULL;
    ripple_catalog_database_value* catalogInOid2Hash = NULL;
    ripple_catalog_datname2oid_value* catalogInDatname2Hash = NULL;
    HASH_SEQ_STATUS status;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (ripple_catalog_database_value*)catalogdata->catalog;
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInOid2Hash = hash_search(sysdicts->by_database, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_database hash duplicate oid, %u, %s",
                                catalogInOid2Hash->ripple_database->oid,
                                catalogInOid2Hash->ripple_database->datname.data);

            if(NULL != catalogInOid2Hash->ripple_database)
            {
                rfree(catalogInOid2Hash->ripple_database);
            }
        }
        catalogInOid2Hash->oid = newcatalog->oid;
        catalogInOid2Hash->ripple_database = (xk_pg_sysdict_Form_pg_database)rmalloc1(sizeof(xk_pg_parser_sysdict_pgdatabase));
        if(NULL == catalogInOid2Hash->ripple_database)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInOid2Hash->ripple_database, 0, newcatalog->ripple_database, sizeof(xk_pg_parser_sysdict_pgdatabase));
        //datname2oid
        catalogInDatname2Hash = hash_search(sysdicts->by_datname2oid, &newcatalog->ripple_database->datname, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_datname2oid hash duplicate oid, %u, %s",
                                catalogInDatname2Hash->oid,
                                catalogInDatname2Hash->datname.data);
        }
        rmemcpy1(catalogInDatname2Hash->datname.data, 0, newcatalog->ripple_database->datname.data, sizeof(newcatalog->ripple_database->datname.data));
        catalogInDatname2Hash->oid = newcatalog->oid;
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInOid2Hash = hash_search(sysdicts->by_database, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInOid2Hash)
        {
            if(NULL != catalogInOid2Hash->ripple_database)
            {
                rfree(catalogInOid2Hash->ripple_database);
            }
        }
        //datname2oid
        catalogInDatname2Hash = hash_search(sysdicts->by_datname2oid, &newcatalog->ripple_database->datname, HASH_REMOVE, &found);
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInOid2Hash = hash_search(sysdicts->by_database, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInOid2Hash)
        {
            elog(RLOG_WARNING, "ripple_database hash duplicate oid, %u, %s",
                                newcatalog->ripple_database->oid,
                                newcatalog->ripple_database->datname.data);
            return;
        }
        rfree(catalogInOid2Hash->ripple_database);

        catalogInOid2Hash->ripple_database = (xk_pg_sysdict_Form_pg_database)rmalloc1(sizeof(xk_pg_parser_sysdict_pgdatabase));
        if(NULL == catalogInOid2Hash->ripple_database)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInOid2Hash->ripple_database, 0, newcatalog->ripple_database, sizeof(xk_pg_parser_sysdict_pgdatabase));
        //datname2database
        hash_seq_init(&status,sysdicts->by_datname2oid);
        while ((catalogInDatname2Hash = hash_seq_search(&status)) != NULL)
        {
            if(catalogInDatname2Hash->oid == newcatalog->oid)
            {
                hash_search(sysdicts->by_datname2oid, &catalogInDatname2Hash->datname, HASH_REMOVE, &found);
                catalogInDatname2Hash = hash_search(sysdicts->by_datname2oid, &newcatalog->ripple_database->datname, HASH_ENTER, &found);
                if(true == found)
                {
                    elog(RLOG_WARNING, "by_datname2oid hash duplicate oid, %u, %s",
                                        catalogInDatname2Hash->oid,
                                        catalogInDatname2Hash->datname.data);
                }
                rmemcpy1(catalogInDatname2Hash->datname.data, 0, newcatalog->ripple_database->datname.data, sizeof(newcatalog->ripple_database->datname.data));
                catalogInDatname2Hash->oid = newcatalog->oid;
                break;
            }
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}


/* 释放 */
void ripple_database_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_database_value* catalog = NULL;
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
    catalog = (ripple_catalog_database_value*)catalogdata->catalog;
    if(NULL != catalog->ripple_database)
    {
        rfree(catalog->ripple_database);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

/* 获取dboid */
Oid ripple_database_getdbid(HTAB* by_database)
{
    HASH_SEQ_STATUS status;
    ripple_catalog_database_value *entry = NULL;
    hash_seq_init(&status,by_database);

    while ((entry = hash_seq_search(&status)) != NULL)
    {
        return entry->oid;
    }

    return InvalidOid;
}

/* 获取数据库的名称 */
char* ripple_database_getdbname(Oid dbid, HTAB* by_database)
{
    bool found = false;
    ripple_catalog_database_value* dbentry = NULL;
    dbentry = hash_search(by_database, &dbid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return dbentry->ripple_database->datname.data;
}
