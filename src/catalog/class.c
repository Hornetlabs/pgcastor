#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/conn.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/class.h"
#include "catalog/attribute.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"

/* Generate system dictionary */
void class_attribute_getfromdb(PGconn* conn, cache_sysdicts* sysdicts)
{
    int                          i, j;
    bool                         found = false;
    PGresult*                    res = NULL;
    ListCell*                    cell = NULL;
    List*                        classlist = NIL;
    pg_sysdict_Form_pg_class     class = NULL;
    pg_sysdict_Form_pg_class     classtoast = NULL;
    pg_sysdict_Form_pg_attribute attribute = NULL;
    char                         sql_exec[MAX_EXEC_SQL_LEN] = {'\0'};

    HASHCTL                      class_hash_ctl;
    HASHCTL                      attr_hash_ctl;
    catalog_class_value*         class_entry = NULL;
    catalog_attribute_value*     attr_entry = NULL;

    rmemset1(&class_hash_ctl, 0, '\0', sizeof(class_hash_ctl));
    class_hash_ctl.keysize = sizeof(Oid);
    class_hash_ctl.entrysize = sizeof(catalog_class_value);
    sysdicts->by_class = hash_create("catalog_sysdict_class", 2048, &class_hash_ctl, HASH_ELEM | HASH_BLOBS);

    rmemset1(&attr_hash_ctl, 0, '\0', sizeof(attr_hash_ctl));
    attr_hash_ctl.keysize = sizeof(Oid);
    attr_hash_ctl.entrysize = sizeof(catalog_attribute_value);
    sysdicts->by_attribute = hash_create("catalog_sysdict_attribute", 2048, &attr_hash_ctl, HASH_ELEM | HASH_BLOBS);

    sprintf(sql_exec,
            "SELECT rel.oid, \n"
            "rel.relname, \n"
            "rel.relnamespace, \n"
            "rel.reltype, \n"
            "case rel.relfilenode when  0 then pg_relation_filenode(rel.oid) else rel.relfilenode "
            "end relfilenode, \n"
            "rel.relkind, \n"
            "rel.relnatts, \n"
            "rel.reltoastrelid, \n"
            "rel.reltablespace, \n"
            "rel.relreplident, \n"
            "rel.relpersistence, \n"
            "case when cont.oid is not null then 1 else 0 end as relhaspk, \n"
            "rel.relhasindex,\n"
            "rel.relowner, \n"
            "nsp.nspname \n"
            "FROM pg_class rel\n"
            "LEFT JOIN pg_constraint cont \n"
            "ON rel.oid = cont.conrelid and cont.contype = 'p' \n "
            "LEFT JOIN pg_namespace nsp \n"
            "ON rel.relnamespace = nsp.oid \n"
            "WHERE relkind not in ('v', 'i', 'c', 'I');");

    res = conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_class query failed");
    }

    for (i = 0; i < PQntuples(res); i++)
    {
        class = (pg_sysdict_Form_pg_class)rmalloc0(sizeof(pg_parser_sysdict_pgclass));
        if (NULL == class)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(class, 0, '\0', sizeof(pg_parser_sysdict_pgclass));
        j = 0;
        sscanf(PQgetvalue(res, i, j++), "%u", &class->oid);
        strcpy(class->relname.data, PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%u", &class->relnamespace);
        sscanf(PQgetvalue(res, i, j++), "%u", &class->reltype);
        sscanf(PQgetvalue(res, i, j++), "%u", &class->relfilenode);
        sscanf(PQgetvalue(res, i, j++), "%c", &class->relkind);
        sscanf(PQgetvalue(res, i, j++), "%hd", &class->relnatts);
        sscanf(PQgetvalue(res, i, j++), "%u", &class->reltoastrelid);
        sscanf(PQgetvalue(res, i, j++), "%u", &class->reltablespace);
        if (0 == class->reltablespace)
        {
            class->reltablespace = PG_DFAULT_TABLESPACE;
        }
        sscanf(PQgetvalue(res, i, j++), "%c", &class->relreplident);
        sscanf(PQgetvalue(res, i, j++), "%c", &class->relpersistence);
        if (strcmp(PQgetvalue(res, i, j++), "1") == 0)
        {
            class->relhaspk = true;
        }
        if (strcmp(PQgetvalue(res, i, j++), "t") == 0)
        {
            class->relhasindex = true;
        }
        sscanf(PQgetvalue(res, i, j++), "%u", &class->relowner);
        strcpy(class->nspname.data, PQgetvalue(res, i, j++));

        classlist = lappend(classlist, class);
    }
    PQclear(res);

    foreach (cell, classlist)
    {
        classtoast = (pg_sysdict_Form_pg_class)lfirst(cell);

        class_entry = hash_search(sysdicts->by_class, &classtoast->oid, HASH_ENTER, &found);
        if (found)
        {
            elog(RLOG_ERROR, "class_oid:%u already exist in by_class", class_entry->class->oid);
        }
        class_entry->oid = classtoast->oid;
        class_entry->class = classtoast;

        rmemset1(sql_exec, 0, '\0', MAX_EXEC_SQL_LEN);
        sprintf(sql_exec,
                "SELECT rel.attrelid, \n"
                "rel.attname, \n"
                "rel.atttypid, \n"
                "rel.attstattarget, \n"
                "rel.attlen, \n"
                "rel.attnum, \n"
                "rel.attndims, \n"
                "rel.attcacheoff, \n"
                "rel.atttypmod, \n"
                "rel.attbyval, \n"
                "rel.attstorage, \n"
                "rel.attalign, \n"
                "rel.attnotnull, \n"
                "rel.atthasdef, \n"
                "rel.atthasmissing, \n"
                "rel.attidentity, \n"
                "rel.attgenerated,\n"
                "rel.attisdropped, \n"
                "rel.attislocal, \n"
                "rel.attinhcount, \n"
                "rel.attcollation \n"
                "FROM pg_attribute rel \n"
                "where rel.attrelid = '%u' and rel.attnum > 0;",
                classtoast->oid);
        res = conn_exec(conn, sql_exec);
        if (NULL == res)
        {
            elog(RLOG_ERROR, "pg_attribute query failed");
        }

        for (i = 0; i < PQntuples(res); i++)
        {
            attribute = (pg_sysdict_Form_pg_attribute)rmalloc0(sizeof(pg_parser_sysdict_pgattributes));
            if (NULL == attribute)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(attribute, 0, '\0', sizeof(pg_parser_sysdict_pgattributes));
            j = 0;

            sscanf(PQgetvalue(res, i, j++), "%u", &attribute->attrelid);
            strcpy(attribute->attname.data, PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%u", &attribute->atttypid);
            sscanf(PQgetvalue(res, i, j++), "%d", &attribute->attstattarget);
            sscanf(PQgetvalue(res, i, j++), "%hd", &attribute->attlen);
            sscanf(PQgetvalue(res, i, j++), "%hd", &attribute->attnum);
            sscanf(PQgetvalue(res, i, j++), "%d", &attribute->attndims);
            sscanf(PQgetvalue(res, i, j++), "%d", &attribute->attcacheoff);
            sscanf(PQgetvalue(res, i, j++), "%d", &attribute->atttypmod);
            attribute->attbyval = bool_judgment(PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%c", &attribute->attstorage);
            sscanf(PQgetvalue(res, i, j++), "%c", &attribute->attalign);
            attribute->attnotnull = bool_judgment(PQgetvalue(res, i, j++));
            attribute->atthasdef = bool_judgment(PQgetvalue(res, i, j++));
            attribute->atthasmissing = bool_judgment(PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%c", &attribute->attidentity);
            sscanf(PQgetvalue(res, i, j++), "%c", &attribute->attgenerated);
            attribute->attisdropped = bool_judgment(PQgetvalue(res, i, j++));
            attribute->attislocal = bool_judgment(PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%d", &attribute->attinhcount);
            sscanf(PQgetvalue(res, i, j++), "%u", &attribute->attcollation);

            attr_entry =
                (catalog_attribute_value*)hash_search(sysdicts->by_attribute, &attribute->attrelid, HASH_ENTER, &found);
            if (!found)
            {
                attr_entry->attrs = NIL;
            }
            attr_entry->attrelid = attribute->attrelid;
            attr_entry->attrs = lappend(attr_entry->attrs, attribute);
        }
        PQclear(res);
    }
    list_free(classlist);

    return;
}

void classdata_write(List* class, uint64* offset, sysdict_header_array* array)
{
    int                      fd;
    uint64                   page_num = 0;
    uint64                   page_offset = 0;
    ListCell*                cell = NULL;
    char                     buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_class class_data = NULL;

    array->type = CATALOG_TYPE_CLASS;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }

    foreach (cell, class)
    {
        class_data = (pg_sysdict_Form_pg_class)lfirst(cell);
        if (page_offset + sizeof(pg_parser_sysdict_pgclass) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, class_data, sizeof(pg_parser_sysdict_pgclass));
        page_offset += sizeof(pg_parser_sysdict_pgclass);
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

HTAB* classcache_load(sysdict_header_array* array)
{
    int                      r = 0;
    int                      fd = -1;
    HTAB*                    classhtab;
    HASHCTL                  hash_ctl;
    bool                     found = false;
    uint64                   fileoffset = 0;

    char                     buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_class class;
    catalog_class_value*     entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_class_value);
    classhtab = hash_create("catalog_class_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_CLASS - 1].len == array[CATALOG_TYPE_CLASS - 1].offset)
    {
        return classhtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_CLASS - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0)
    {
        uint64 offset = 0;

        while (offset + sizeof(pg_parser_sysdict_pgclass) < FILE_BLK_SIZE)
        {
            class = (pg_sysdict_Form_pg_class)rmalloc1(sizeof(pg_parser_sysdict_pgclass));
            if (NULL == class)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(class, 0, '\0', sizeof(pg_parser_sysdict_pgclass));
            rmemcpy0(class, 0, buffer + offset, sizeof(pg_parser_sysdict_pgclass));
            entry = hash_search(classhtab, &class->oid, HASH_ENTER, &found);
            if (found)
            {
                elog(RLOG_ERROR, "class_oid:%u already exist in by_class", entry->class->oid);
            }
            entry->oid = class->oid;
            entry->class = class;
            offset += sizeof(pg_parser_sysdict_pgclass);
            if (fileoffset + offset == array[CATALOG_TYPE_CLASS - 1].len)
            {
                if (osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return classhtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }
    return classhtab;
}

/* colvalue2class */
catalogdata* class_colvalue2class(void* in_colvalue)
{
    catalogdata*                    catalogclass = NULL;
    pg_sysdict_Form_pg_class        pgclass = NULL;
    catalog_class_value*            classvalue = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* convert colvalue */
    catalogclass = (catalogdata*)rmalloc0(sizeof(catalogdata));
    if (NULL == catalogclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogclass, 0, '\0', sizeof(catalogdata));

    classvalue = (catalog_class_value*)rmalloc0(sizeof(catalog_class_value));
    if (NULL == classvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(classvalue, 0, '\0', sizeof(catalog_class_value));
    catalogclass->catalog = classvalue;
    catalogclass->type = CATALOG_TYPE_CLASS;

    pgclass = (pg_sysdict_Form_pg_class)rmalloc0(sizeof(pg_parser_sysdict_pgclass));
    if (NULL == pgclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgclass, 0, '\0', sizeof(pg_parser_sysdict_pgclass));
    classvalue->class = pgclass;

    /* oid  0*/
    sscanf((char*)(colvalue->m_value), "%u", &pgclass->oid);
    classvalue->oid = pgclass->oid;

    /* relfilenode 7 */
    sscanf((char*)((colvalue + 7)->m_value), "%u", &pgclass->relfilenode);

    /* relkind 16 */
    pgclass->relkind = ((char*)((colvalue + 16)->m_value))[0];

    if (PG_SYSDICT_RELKIND_VIEW == pgclass->relkind || PG_SYSDICT_RELKIND_PARTITIONED_INDEX == pgclass->relkind)
    {
        /* Memory release, no logging */
        rfree(pgclass);
        rfree(classvalue);
        rfree(catalogclass);
        return NULL;
    }

    /* relname 1*/
    rmemcpy1(pgclass->relname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* relnamespace 2*/
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgclass->relnamespace);

    /* relnatts 17 */
    sscanf((char*)((colvalue + 17)->m_value), "%hd", &pgclass->relnatts);

    /* relpersistence 15 */
    pgclass->relpersistence = ((char*)((colvalue + 15)->m_value))[0];

    /* relpersistence 25 */
    pgclass->relreplident = ((char*)((colvalue + 25)->m_value))[0];

    /* relhasindex 13 */
    pgclass->relhasindex = (((char*)((colvalue + 13)->m_value))[0]) == 't' ? true : false;

    /* reltablespace 8 */
    sscanf((char*)((colvalue + 8)->m_value), "%u", &pgclass->reltablespace);
    if (0 == pgclass->reltablespace)
    {
        pgclass->reltablespace = PG_DFAULT_TABLESPACE;
    }

    /* Filter system-created temporary tables */
    if (8 <= strlen(pgclass->relname.data) && 0 == strncmp("pg_temp_", pgclass->relname.data, 8))
    {
        /* Memory release, no logging */
        rfree(pgclass);
        rfree(classvalue);
        rfree(catalogclass);
        return NULL;
    }

    /* reltoastrelid 12 */
    sscanf((char*)((colvalue + 12)->m_value), "%u", &pgclass->reltoastrelid);

    /* reltype 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%u", &pgclass->reltype);

    return catalogclass;
}

/* colvalue2class */
catalogdata* class_colvalue2class_nofilter(void* in_colvalue)
{
    catalogdata*                    catalogclass = NULL;
    pg_sysdict_Form_pg_class        pgclass = NULL;
    catalog_class_value*            classvalue = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* Value conversion */
    catalogclass = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if (NULL == catalogclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogclass, 0, '\0', sizeof(catalogdata));

    classvalue = (catalog_class_value*)rmalloc1(sizeof(catalog_class_value));
    if (NULL == classvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(classvalue, 0, '\0', sizeof(catalog_class_value));
    catalogclass->catalog = classvalue;
    catalogclass->type = CATALOG_TYPE_CLASS;

    pgclass = (pg_sysdict_Form_pg_class)rmalloc1(sizeof(pg_parser_sysdict_pgclass));
    if (NULL == pgclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgclass, 0, '\0', sizeof(pg_parser_sysdict_pgclass));
    classvalue->class = pgclass;

    /* oid  0*/
    sscanf((char*)(colvalue->m_value), "%u", &pgclass->oid);
    classvalue->oid = pgclass->oid;

    /* relfilenode 7 */
    sscanf((char*)((colvalue + 7)->m_value), "%u", &pgclass->relfilenode);

    /* relkind 16 */
    pgclass->relkind = ((char*)((colvalue + 16)->m_value))[0];

    /* relname 1*/
    rmemcpy1(pgclass->relname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* relnamespace 2*/
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgclass->relnamespace);

    /* relnatts 17 */
    sscanf((char*)((colvalue + 17)->m_value), "%hd", &pgclass->relnatts);

    /* relpersistence 15 */
    pgclass->relpersistence = ((char*)((colvalue + 15)->m_value))[0];

    /* relpersistence 25 */
    pgclass->relreplident = ((char*)((colvalue + 25)->m_value))[0];

    /* relhasindex 13 */
    pgclass->relhasindex = (((char*)((colvalue + 13)->m_value))[0]) == 't' ? true : false;

    /* reltablespace 8 */
    sscanf((char*)((colvalue + 8)->m_value), "%u", &pgclass->reltablespace);
    if (0 == pgclass->reltablespace)
    {
        pgclass->reltablespace = PG_DFAULT_TABLESPACE;
    }

    /* reltoastrelid 12 */
    sscanf((char*)((colvalue + 12)->m_value), "%u", &pgclass->reltoastrelid);

    /* reltype 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%u", &pgclass->reltype);

    return catalogclass;
}

bool bool_judgment(char* str)
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

/* catalogdata2transcache */
void class_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool                 found = false;
    RelFileNode          relfilenode = {0};
    catalog_class_value* newClass = NULL;
    catalog_class_value* classInHash = NULL;
    relfilenode2oid*     prelfilenode2oid = NULL;
    relfilenode2oid*     prelfilenode2oidOld = NULL;

    if (NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newClass = (catalog_class_value*)catalogdata->catalog;
    elog(RLOG_DEBUG,
         "op:%d, newclass, %s,type:%c, %u.%u",
         catalogdata->op,
         newClass->class->relname.data,
         newClass->class->relkind,
         newClass->class->oid,
         newClass->class->relfilenode);

    if (CATALOG_OP_INSERT == catalogdata->op)
    {
        classInHash = hash_search(sysdicts->by_class, &newClass->oid, HASH_ENTER, &found);
        if (true == found)
        {
            if (NULL != classInHash->class)
            {
                rfree(classInHash->class);
            }
        }
        classInHash->oid = newClass->oid;

        /* Allocate space and assign */
        classInHash->class = (pg_sysdict_Form_pg_class)rmalloc1(sizeof(pg_parser_sysdict_pgclass));
        if (NULL == classInHash->class)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(classInHash->class, 0, newClass->class, sizeof(pg_parser_sysdict_pgclass));

        if (NULL != sysdicts->by_relfilenode)
        {
            relfilenode.relNode = newClass->class->relfilenode;
            relfilenode.dbNode = misc_controldata_database_get(NULL);
            if (0 == newClass->class->reltablespace)
            {
                relfilenode.spcNode = PG_DFAULT_TABLESPACE;
                newClass->class->reltablespace = PG_DFAULT_TABLESPACE;
            }
            else
            {
                relfilenode.spcNode = newClass->class->reltablespace;
            }

            /* Add relfilenode to hash table */
            /* Insert new */
            prelfilenode2oid = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_ENTER, &found);
            if (NULL == prelfilenode2oid)
            {
                elog(RLOG_ERROR,
                     "put relfilenode2oid hash error, %u.%u.%u, oid:%u",
                     relfilenode.spcNode,
                     relfilenode.dbNode,
                     relfilenode.relNode,
                     newClass->oid);
            }
            prelfilenode2oid->oid = newClass->class->oid;
            rmemcpy1(&prelfilenode2oid->relfilenode, 0, &relfilenode, sizeof(RelFileNode));
        }
    }
    else if (CATALOG_OP_DELETE == catalogdata->op)
    {
        classInHash = hash_search(sysdicts->by_class, &newClass->oid, HASH_REMOVE, &found);

        if (NULL != classInHash)
        {
            if (NULL != classInHash->class)
            {
                relfilenode.relNode = classInHash->class->relfilenode;
                relfilenode.dbNode = misc_controldata_database_get(NULL);
                relfilenode.spcNode = classInHash->class->reltablespace;
                if (NULL != sysdicts->by_relfilenode)
                {
                    hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_REMOVE, NULL);
                }
                rfree(classInHash->class);
            }
        }
    }
    else if (CATALOG_OP_UPDATE == catalogdata->op)
    {
        classInHash = hash_search(sysdicts->by_class, &newClass->oid, HASH_FIND, &found);
        if (NULL == classInHash)
        {
            elog(RLOG_WARNING,
                 "relation %u,%s can not fond in class hash",
                 newClass->class->oid,
                 newClass->class->relname.data);
            return;
        }

        /* Check if relfilenode has changed */
        if (classInHash->class->relfilenode != newClass->class->relfilenode)
        {
            relfilenode.relNode = classInHash->class->relfilenode;
            relfilenode.dbNode = misc_controldata_database_get(NULL);
            relfilenode.spcNode = classInHash->class->reltablespace;

            /* When performing vacuum full on tables with toast fields, toast's relfilenode will be
             * modified */
            if (NULL != sysdicts->by_relfilenode)
            {
                prelfilenode2oidOld = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_FIND, NULL);
                if (prelfilenode2oidOld->oid == classInHash->class->oid)
                {
                    /* Replace relfilenode data */
                    hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_REMOVE, NULL);

                    /* Insert new */
                    relfilenode.relNode = newClass->class->relfilenode;
                    prelfilenode2oid = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_ENTER, &found);
                    if (!found)
                    {
                        rmemcpy1(&prelfilenode2oid->relfilenode, 0, &relfilenode, sizeof(RelFileNode));
                    }

                    /* Then directly replace */
                    prelfilenode2oid->oid = newClass->class->oid;
                }
            }
        }
        newClass->class->relhaspk = classInHash->class->relhaspk;
        newClass->class->relhasindex = classInHash->class->relhasindex;
        rmemcpy0(classInHash->class, 0, newClass->class, sizeof(pg_parser_sysdict_pgclass));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void classcache_write(HTAB* classcache, uint64* offset, sysdict_header_array* array)
{
    int                      fd;
    uint64                   page_num = 0;
    uint64                   page_offset = 0;
    HASH_SEQ_STATUS          status;
    char                     buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_class class;
    catalog_class_value*     entry;

    array[CATALOG_TYPE_CLASS - 1].type = CATALOG_TYPE_CLASS;
    array[CATALOG_TYPE_CLASS - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | O_EXCL | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status, classcache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        class = entry->class;

        if (page_offset + sizeof(pg_parser_sysdict_pgclass) > FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, class, sizeof(pg_parser_sysdict_pgclass));
        page_offset += sizeof(pg_parser_sysdict_pgclass);
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

    array[CATALOG_TYPE_CLASS - 1].len = page_num;

    return;
}

void class_catalogdatafree(catalogdata* catalogdata)
{
    catalog_class_value* catalog = NULL;
    if (NULL == catalogdata)
    {
        return;
    }

    /* Catalog memory release */
    if (NULL != catalogdata->catalog)
    {
        catalog = (catalog_class_value*)catalogdata->catalog;
        if (NULL != catalog->class)
        {
            rfree(catalog->class);
        }
        rfree(catalogdata->catalog);
    }
    rfree(catalogdata);
}

/* Get pg_class data by oid */
void* class_getbyoid(Oid oid, HTAB* by_class)
{
    bool                 found = false;
    catalog_class_value* classentry = NULL;
    classentry = hash_search(by_class, &oid, HASH_FIND, &found);
    if (false == found)
    {
        return NULL;
    }
    return (void*)classentry->class;
}
