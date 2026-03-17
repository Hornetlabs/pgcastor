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
#include "catalog/control.h"
#include "catalog/database.h"


void database_getfromdb(PGconn *conn, cache_sysdicts* sysdicts)
{
    int        i, j;
    HASHCTL hash_ctl;
    HASHCTL oid_hash_ctl;
    bool found = false;
    PGresult *res = NULL;
    pg_sysdict_Form_pg_database database;
    catalog_database_value *entry = NULL;
    catalog_datname2oid_value *oid_entry = NULL;
    const char *query = "SELECT rel.oid, rel.datname, rel.datdba, rel.encoding, rel.datcollate, rel.datctype FROM pg_database rel;";

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(catalog_database_value);
    sysdicts->by_database = hash_create("catalog_sysdicts_database", 2048, &hash_ctl,
                                        HASH_ELEM | HASH_BLOBS);

    rmemset1(&oid_hash_ctl, 0, '\0', sizeof(oid_hash_ctl));
    oid_hash_ctl.keysize = sizeof(pg_parser_NameData);
    oid_hash_ctl.entrysize = sizeof(catalog_datname2oid_value);
    sysdicts->by_datname2oid = hash_create("catalog_datname2oid_value", 2048, &oid_hash_ctl,
                                            HASH_ELEM | HASH_BLOBS);

    res = conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_database query failed");
    }

    // 打印行数据
    for (i = 0; i < PQntuples(res); i++) 
    {
        database = (pg_sysdict_Form_pg_database)rmalloc0(sizeof(pg_parser_sysdict_pgdatabase));
        if(NULL == database)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(database, 0, '\0', sizeof(pg_parser_sysdict_pgdatabase));
        j=0;
        sscanf(PQgetvalue(res, i, j++), "%u", &database->oid);
        strcpy(database->datname.data ,PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%u", &database->datdba);
        sscanf(PQgetvalue(res, i, j++), "%d", &database->encoding);
        strcpy(database->datcollate.data ,PQgetvalue(res, i, j++));
        strcpy(database->datctype.data ,PQgetvalue(res, i, j++));

        entry = hash_search(sysdicts->by_database, &database->oid, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "database_oid:%u already exist in by_database", entry->database->oid);
        }
        entry->oid = database->oid;
        entry->database = database;

        oid_entry = (catalog_datname2oid_value *)hash_search(sysdicts->by_datname2oid, &database->datname, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "database_name:%s already exist in by_datname2oid", oid_entry->datname.data);
        }
        oid_entry->oid = database->oid;
        rmemcpy1(oid_entry->datname.data, 0, database->datname.data, sizeof(database->datname.data));
    }

    PQclear(res);

    return;
}

void databasedata_write(List* database_list, uint64 *offset, sysdict_header_array* array)
{
    int  fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    ListCell*	cell = NULL;
    char buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_database database = NULL;

    array->type = CATALOG_TYPE_DATABASE;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }

    foreach(cell, database_list)
    {
        database = (pg_sysdict_Form_pg_database) lfirst(cell);

        if(page_offset + sizeof(pg_parser_sysdict_pgdatabase) > FILE_BLK_SIZE)
        {
            if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
                elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
                osal_file_close(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += FILE_BLK_SIZE;
            page_offset = 0;
        }
        rmemcpy1(buffer, page_offset, database, sizeof(pg_parser_sysdict_pgdatabase));
        page_offset += sizeof(pg_parser_sysdict_pgdatabase);
    }

    if (page_offset > 0) {
        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }

    if(0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_FILE);
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    array->len = page_num;

}

HTAB* databasecache_load(sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* databasehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_database database;
    catalog_database_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_database_value);
    databasehtab = hash_create("catalog_database_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    /*
    * Read data...
    */
    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_DATABASE - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgdatabase) < FILE_BLK_SIZE)
        {
            database = (pg_sysdict_Form_pg_database)rmalloc1(sizeof(pg_parser_sysdict_pgdatabase));
            if(NULL == database)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(database, 0, '\0', sizeof(pg_parser_sysdict_pgdatabase));
            rmemcpy0(database, 0,buffer + offset, sizeof(pg_parser_sysdict_pgdatabase));
            entry = hash_search(databasehtab, &database->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "database_oid:%u already exist in by_database", entry->database->oid);
            }
            entry->oid = database->oid;
            entry->database = database;
            offset += sizeof(pg_parser_sysdict_pgdatabase);
            if (fileoffset + offset == array[CATALOG_TYPE_DATABASE - 1].len)
            {
                if(osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return databasehtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return databasehtab;
}

HTAB* datname2oid_cache_load(sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* databasehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[FILE_BLK_SIZE];
    pg_parser_sysdict_pgdatabase database;
    catalog_datname2oid_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(pg_parser_NameData);
    hash_ctl.entrysize = sizeof(catalog_datname2oid_value);
    databasehtab = hash_create("catalog_datname2oid_value", 2048, &hash_ctl,
                                    HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_DATABASE - 1].len == array[CATALOG_TYPE_DATABASE - 1].offset)
    {
        return databasehtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE,
                        O_RDWR | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_DATABASE - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgdatabase) < FILE_BLK_SIZE)
        {
            rmemset1(&database, 0, '\0', sizeof(pg_parser_sysdict_pgdatabase));
            rmemcpy1(&database, 0, buffer + offset, sizeof(pg_parser_sysdict_pgdatabase));
            entry = (catalog_datname2oid_value *)hash_search(databasehtab, &database.datname, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "database_name:%s already exist in by_datname2oid", entry->datname.data);
            }
            entry->oid = database.oid;
            rmemcpy1(entry->datname.data, 0, database.datname.data, sizeof(database.datname.data));
            offset += sizeof(pg_parser_sysdict_pgdatabase);
            if (fileoffset + offset == array[CATALOG_TYPE_DATABASE - 1].len)
            {
                if(osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return databasehtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    return databasehtab;
}

void databasecache_write(HTAB* databasecache, uint64 *offset, sysdict_header_array* array)
{
    int fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    HASH_SEQ_STATUS status;
    char buffer[FILE_BLK_SIZE];
    catalog_database_value *entry = NULL;
    pg_sysdict_Form_pg_database database = NULL;

    array[CATALOG_TYPE_DATABASE - 1].type = CATALOG_TYPE_DATABASE;
    array[CATALOG_TYPE_DATABASE - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status,databasecache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        database = entry->database;
        if(page_offset + sizeof(pg_parser_sysdict_pgdatabase) > FILE_BLK_SIZE)
        {
            if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
                elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
                osal_file_close(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += FILE_BLK_SIZE;
            page_offset = 0;
        }
        rmemcpy1(buffer, page_offset, database, sizeof(pg_parser_sysdict_pgdatabase));
        page_offset += sizeof(pg_parser_sysdict_pgdatabase);
    }

    if (page_offset > 0) {
        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE) {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }

    if(0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_TMP_FILE);
    }

    if(osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_TMP_FILE);
    }

    array[CATALOG_TYPE_DATABASE - 1].len = page_num;

}

/* colvalue2database */
catalogdata* database_colvalue2database(void* in_colvalue)
{
    catalogdata* catalog_data = NULL;
    catalog_database_value* databasevalue = NULL;
    pg_sysdict_Form_pg_database pgdatabase = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if(NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    databasevalue = (catalog_database_value*)rmalloc1(sizeof(catalog_database_value));
    if(NULL == databasevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(databasevalue, 0, '\0', sizeof(catalog_database_value));
    catalog_data->catalog = databasevalue;
    catalog_data->type = CATALOG_TYPE_DATABASE;

    pgdatabase = (pg_sysdict_Form_pg_database)rmalloc1(sizeof(pg_parser_sysdict_pgdatabase));
    if(NULL == pgdatabase)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgdatabase, 0, '\0', sizeof(pg_parser_sysdict_pgdatabase));
    databasevalue->database = pgdatabase;

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

    return catalog_data;
}

/* catalogdata2transcache */
void database_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool found = false;
    catalog_database_value* newcatalog = NULL;
    catalog_database_value* catalogInOid2Hash = NULL;
    catalog_datname2oid_value* catalogInDatname2Hash = NULL;
    HASH_SEQ_STATUS status;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_database_value*)catalogdata->catalog;
    if(CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInOid2Hash = hash_search(sysdicts->by_database, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_database hash duplicate oid, %u, %s",
                                catalogInOid2Hash->database->oid,
                                catalogInOid2Hash->database->datname.data);

            if(NULL != catalogInOid2Hash->database)
            {
                rfree(catalogInOid2Hash->database);
            }
        }
        catalogInOid2Hash->oid = newcatalog->oid;
        catalogInOid2Hash->database = (pg_sysdict_Form_pg_database)rmalloc1(sizeof(pg_parser_sysdict_pgdatabase));
        if(NULL == catalogInOid2Hash->database)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInOid2Hash->database, 0, newcatalog->database, sizeof(pg_parser_sysdict_pgdatabase));
        //datname2oid
        catalogInDatname2Hash = hash_search(sysdicts->by_datname2oid, &newcatalog->database->datname, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_WARNING, "by_datname2oid hash duplicate oid, %u, %s",
                                catalogInDatname2Hash->oid,
                                catalogInDatname2Hash->datname.data);
        }
        rmemcpy1(catalogInDatname2Hash->datname.data, 0, newcatalog->database->datname.data, sizeof(newcatalog->database->datname.data));
        catalogInDatname2Hash->oid = newcatalog->oid;
    }
    else if(CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInOid2Hash = hash_search(sysdicts->by_database, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInOid2Hash)
        {
            if(NULL != catalogInOid2Hash->database)
            {
                rfree(catalogInOid2Hash->database);
            }
        }
        //datname2oid
        catalogInDatname2Hash = hash_search(sysdicts->by_datname2oid, &newcatalog->database->datname, HASH_REMOVE, &found);
    }
    else if(CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInOid2Hash = hash_search(sysdicts->by_database, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInOid2Hash)
        {
            elog(RLOG_WARNING, "database hash duplicate oid, %u, %s",
                                newcatalog->database->oid,
                                newcatalog->database->datname.data);
            return;
        }
        rfree(catalogInOid2Hash->database);

        catalogInOid2Hash->database = (pg_sysdict_Form_pg_database)rmalloc1(sizeof(pg_parser_sysdict_pgdatabase));
        if(NULL == catalogInOid2Hash->database)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(catalogInOid2Hash->database, 0, newcatalog->database, sizeof(pg_parser_sysdict_pgdatabase));
        //datname2database
        hash_seq_init(&status,sysdicts->by_datname2oid);
        while ((catalogInDatname2Hash = hash_seq_search(&status)) != NULL)
        {
            if(catalogInDatname2Hash->oid == newcatalog->oid)
            {
                hash_search(sysdicts->by_datname2oid, &catalogInDatname2Hash->datname, HASH_REMOVE, &found);
                catalogInDatname2Hash = hash_search(sysdicts->by_datname2oid, &newcatalog->database->datname, HASH_ENTER, &found);
                if(true == found)
                {
                    elog(RLOG_WARNING, "by_datname2oid hash duplicate oid, %u, %s",
                                        catalogInDatname2Hash->oid,
                                        catalogInDatname2Hash->datname.data);
                }
                rmemcpy1(catalogInDatname2Hash->datname.data, 0, newcatalog->database->datname.data, sizeof(newcatalog->database->datname.data));
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
void database_catalogdatafree(catalogdata* catalogdata)
{
    catalog_database_value* catalog = NULL;
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
    catalog = (catalog_database_value*)catalogdata->catalog;
    if(NULL != catalog->database)
    {
        rfree(catalog->database);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

/* 获取dboid */
Oid database_getdbid(HTAB* by_database)
{
    HASH_SEQ_STATUS status;
    catalog_database_value *entry = NULL;
    hash_seq_init(&status,by_database);

    while ((entry = hash_seq_search(&status)) != NULL)
    {
        return entry->oid;
    }

    return InvalidOid;
}

/* 获取数据库的名称 */
char* database_getdbname(Oid dbid, HTAB* by_database)
{
    bool found = false;
    catalog_database_value* dbentry = NULL;
    dbentry = hash_search(by_database, &dbid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return dbentry->database->datname.data;
}
