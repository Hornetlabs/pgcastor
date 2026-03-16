#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/ripple_conn.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_class.h"
#include "catalog/ripple_attribute.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"


/* 生成系统字典 */
void ripple_class_attribute_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
    int i, j;
    bool found = false;
    PGresult    *res = NULL;
    ListCell*   cell = NULL;
    List*   classlist = NIL;
    xk_pg_sysdict_Form_pg_class ripple_class = NULL;
    xk_pg_sysdict_Form_pg_class classtoast = NULL;
    xk_pg_sysdict_Form_pg_attribute ripple_attribute = NULL;
    char sql_exec[RIPPLE_MAX_EXEC_SQL_LEN] = {'\0'};

    HASHCTL class_hash_ctl;
    HASHCTL attr_hash_ctl;
    ripple_catalog_class_value *class_entry = NULL;
    ripple_catalog_attribute_value *attr_entry = NULL;

    rmemset1(&class_hash_ctl, 0, '\0', sizeof(class_hash_ctl));
    class_hash_ctl.keysize = sizeof(Oid);
    class_hash_ctl.entrysize = sizeof(ripple_catalog_class_value);
    sysdicts->by_class = hash_create("ripple_catalog_sysdict_class", 2048, &class_hash_ctl,
                        HASH_ELEM | HASH_BLOBS);

    rmemset1(&attr_hash_ctl, 0, '\0', sizeof(attr_hash_ctl));
    attr_hash_ctl.keysize = sizeof(Oid);
    attr_hash_ctl.entrysize = sizeof(ripple_catalog_attribute_value);
    sysdicts->by_attribute = hash_create("ripple_catalog_sysdict_attribute", 2048, &attr_hash_ctl,
                        HASH_ELEM | HASH_BLOBS);

    sprintf(sql_exec, "SELECT rel.oid, \n"
                            "rel.relname, \n"
                            "rel.relnamespace, \n"
                            "rel.reltype, \n"
                            "case rel.relfilenode when  0 then pg_relation_filenode(rel.oid) else rel.relfilenode end relfilenode, \n"
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

    res = ripple_conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_class query failed");
    }

    // 打印行数据
    for (i = 0; i < PQntuples(res); i++)
    {
        ripple_class = (xk_pg_sysdict_Form_pg_class)rmalloc0(sizeof(xk_pg_parser_sysdict_pgclass));
        if(NULL == ripple_class)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(ripple_class, 0, '\0', sizeof(xk_pg_parser_sysdict_pgclass));
        j=0;
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->oid);
        strcpy(ripple_class->relname.data ,PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->relnamespace);
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->reltype);
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->relfilenode);
        sscanf(PQgetvalue(res, i, j++), "%c", &ripple_class->relkind);
        sscanf(PQgetvalue(res, i, j++), "%hd", &ripple_class->relnatts);
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->reltoastrelid);
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->reltablespace);
        if(0 == ripple_class->reltablespace)
        {
            ripple_class->reltablespace = RIPPLE_PG_DFAULT_TABLESPACE;
        }
        sscanf(PQgetvalue(res, i, j++), "%c", &ripple_class->relreplident);
        sscanf(PQgetvalue(res, i, j++), "%c", &ripple_class->relpersistence);
        if(strcmp(PQgetvalue(res, i, j++), "1") == 0)
        {
            ripple_class->relhaspk = true;
        }
        if(strcmp(PQgetvalue(res, i, j++), "t") == 0)
        {
            ripple_class->relhasindex = true;
        }
        sscanf(PQgetvalue(res, i, j++), "%u", &ripple_class->relowner);
        strcpy(ripple_class->nspname.data ,PQgetvalue(res, i, j++));

        classlist = lappend(classlist, ripple_class);
    }
    PQclear(res);

    foreach(cell, classlist)
    {
        classtoast = (xk_pg_sysdict_Form_pg_class) lfirst(cell);

        class_entry = hash_search(sysdicts->by_class, &classtoast->oid, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "class_oid:%u already exist in by_class", class_entry->ripple_class->oid);
        }
        class_entry->oid = classtoast->oid;
        class_entry->ripple_class = classtoast;

        rmemset1(sql_exec, 0, '\0', RIPPLE_MAX_EXEC_SQL_LEN);
        sprintf(sql_exec, "SELECT rel.attrelid, \n"
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
                                    "where rel.attrelid = '%u' and rel.attnum > 0;",classtoast->oid );
        res = ripple_conn_exec(conn, sql_exec);
        if (NULL == res)
        {
            elog(RLOG_ERROR, "pg_attribute query failed");
        }

        for (i = 0; i < PQntuples(res); i++) 
        {
            ripple_attribute = (xk_pg_sysdict_Form_pg_attribute)rmalloc0(sizeof(xk_pg_parser_sysdict_pgattributes));
            if(NULL == ripple_attribute)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(ripple_attribute, 0, '\0', sizeof(xk_pg_parser_sysdict_pgattributes));
            j=0;

            sscanf(PQgetvalue(res, i, j++), "%u", &ripple_attribute->attrelid);
            strcpy(ripple_attribute->attname.data ,PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%u", &ripple_attribute->atttypid);
            sscanf(PQgetvalue(res, i, j++), "%d", &ripple_attribute->attstattarget);
            sscanf(PQgetvalue(res, i, j++), "%hd", &ripple_attribute->attlen);
            sscanf(PQgetvalue(res, i, j++), "%hd", &ripple_attribute->attnum);
            sscanf(PQgetvalue(res, i, j++), "%d", &ripple_attribute->attndims);
            sscanf(PQgetvalue(res, i, j++), "%d", &ripple_attribute->attcacheoff);
            sscanf(PQgetvalue(res, i, j++), "%d", &ripple_attribute->atttypmod);
            ripple_attribute->attbyval = bool_judgment(PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%c", &ripple_attribute->attstorage);
            sscanf(PQgetvalue(res, i, j++), "%c", &ripple_attribute->attalign);
            ripple_attribute->attnotnull = bool_judgment(PQgetvalue(res, i, j++));
            ripple_attribute->atthasdef = bool_judgment(PQgetvalue(res, i, j++));
            ripple_attribute->atthasmissing = bool_judgment(PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%c", &ripple_attribute->attidentity);
            sscanf(PQgetvalue(res, i, j++), "%c", &ripple_attribute->attgenerated);
            ripple_attribute->attisdropped = bool_judgment(PQgetvalue(res, i, j++));
            ripple_attribute->attislocal = bool_judgment(PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%d", &ripple_attribute->attinhcount);
            sscanf(PQgetvalue(res, i, j++), "%u", &ripple_attribute->attcollation);

            attr_entry = (ripple_catalog_attribute_value *)hash_search(sysdicts->by_attribute, &ripple_attribute->attrelid, HASH_ENTER, &found);
            if (!found)
            {
                attr_entry->attrs = NIL;
            }
            attr_entry->attrelid = ripple_attribute->attrelid;
            attr_entry->attrs = lappend(attr_entry->attrs, ripple_attribute);
        }
        PQclear(res);
    }
    list_free(classlist);

    return;
}

void ripple_classdata_write(List* ripple_class, uint64 *offset, ripple_sysdict_header_array* array)
{
    int	 fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    ListCell*	cell = NULL;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_class class = NULL;
    
    array->type = RIPPLE_CATALOG_TYPE_CLASS;
    array->offset = *offset;
    page_num = *offset;
    
    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
    }

    foreach(cell, ripple_class)
    {
        class = (xk_pg_sysdict_Form_pg_class) lfirst(cell);
        if(page_offset + sizeof(xk_pg_parser_sysdict_pgclass) > RIPPLE_FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, class, sizeof(xk_pg_parser_sysdict_pgclass));
        page_offset += sizeof(xk_pg_parser_sysdict_pgclass);
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

HTAB* ripple_classcache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* classhtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;

    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_class class;
    ripple_catalog_class_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_class_value);
    classhtab = hash_create("ripple_catalog_class_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

	if (array[RIPPLE_CATALOG_TYPE_CLASS - 1].len == array[RIPPLE_CATALOG_TYPE_CLASS - 1].offset)
	{
		return classhtab;
	}

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_CLASS - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgclass) < RIPPLE_FILE_BLK_SIZE)
        {
            class = (xk_pg_sysdict_Form_pg_class)rmalloc1(sizeof(xk_pg_parser_sysdict_pgclass));
            if(NULL == class)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(class, 0, '\0', sizeof(xk_pg_parser_sysdict_pgclass));
            rmemcpy0(class, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgclass));
            entry = hash_search(classhtab, &class->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "class_oid:%u already exist in by_class", entry->ripple_class->oid);
            }
            entry->oid = class->oid;
            entry->ripple_class = class;
            offset += sizeof(xk_pg_parser_sysdict_pgclass);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_CLASS - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return classhtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }
    return classhtab;
}

/* colvalue2class */
ripple_catalogdata* ripple_class_colvalue2class(void* in_colvalue)
{
    ripple_catalogdata* catalogclass = NULL;
    xk_pg_sysdict_Form_pg_class pgclass = NULL;
    ripple_catalog_class_value* classvalue = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogclass = (ripple_catalogdata*)rmalloc0(sizeof(ripple_catalogdata));
    if(NULL == catalogclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogclass, 0, '\0', sizeof(ripple_catalogdata));

    classvalue = (ripple_catalog_class_value*)rmalloc0(sizeof(ripple_catalog_class_value));
    if(NULL == classvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(classvalue, 0, '\0', sizeof(ripple_catalog_class_value));
    catalogclass->catalog = classvalue;
    catalogclass->type = RIPPLE_CATALOG_TYPE_CLASS;

    pgclass = (xk_pg_sysdict_Form_pg_class)rmalloc0(sizeof(xk_pg_parser_sysdict_pgclass));
    if(NULL == pgclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgclass, 0, '\0', sizeof(xk_pg_parser_sysdict_pgclass));
    classvalue->ripple_class = pgclass;

    /* oid  0*/
    sscanf((char*)(colvalue->m_value), "%u", &pgclass->oid);
    classvalue->oid = pgclass->oid;

    /* relfilenode 7 */
    sscanf((char*)((colvalue + 7)->m_value), "%u", &pgclass->relfilenode);

    /* relkind 16 */
    pgclass->relkind = ((char*)((colvalue + 16)->m_value))[0];

    if(XK_PG_SYSDICT_RELKIND_VIEW == pgclass->relkind
        || XK_PG_SYSDICT_RELKIND_PARTITIONED_INDEX == pgclass->relkind)
    {
        /* 内存释放，不记录 */
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
    if(0 == pgclass->reltablespace)
    {
        pgclass->reltablespace = RIPPLE_PG_DFAULT_TABLESPACE;
    }

    /* 过滤系统自建临时表 */
    if(8 <= strlen(pgclass->relname.data)
        && 0 == strncmp("pg_temp_", pgclass->relname.data, 8))
    {
        /* 内存释放，不记录 */
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
ripple_catalogdata* ripple_class_colvalue2class_nofilter(void* in_colvalue)
{
    ripple_catalogdata* catalogclass = NULL;
    xk_pg_sysdict_Form_pg_class pgclass = NULL;
    ripple_catalog_class_value* classvalue = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogclass = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogclass, 0, '\0', sizeof(ripple_catalogdata));

    classvalue = (ripple_catalog_class_value*)rmalloc1(sizeof(ripple_catalog_class_value));
    if(NULL == classvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(classvalue, 0, '\0', sizeof(ripple_catalog_class_value));
    catalogclass->catalog = classvalue;
    catalogclass->type = RIPPLE_CATALOG_TYPE_CLASS;

    pgclass = (xk_pg_sysdict_Form_pg_class)rmalloc1(sizeof(xk_pg_parser_sysdict_pgclass));
    if(NULL == pgclass)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgclass, 0, '\0', sizeof(xk_pg_parser_sysdict_pgclass));
    classvalue->ripple_class = pgclass;

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
    if(0 == pgclass->reltablespace)
    {
        pgclass->reltablespace = RIPPLE_PG_DFAULT_TABLESPACE;
    }

    /* reltoastrelid 12 */
    sscanf((char*)((colvalue + 12)->m_value), "%u", &pgclass->reltoastrelid);

    /* reltype 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%u", &pgclass->reltype);

    return catalogclass;
}

bool bool_judgment(char * str)
{
    if (str[0] == 't' || str[0] == 'T') {
        return true;
    } else if (str[0] == 'f' || str[0] == 'F') {
        return false;
    }
    return -1;
}

/* catalogdata2transcache */
void ripple_class_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    RelFileNode relfilenode = { 0 };
    ripple_catalog_class_value* newClass = NULL;
    ripple_catalog_class_value* classInHash = NULL;
    ripple_relfilenode2oid* prelfilenode2oid = NULL;
    ripple_relfilenode2oid* prelfilenode2oidOld = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newClass = (ripple_catalog_class_value*)catalogdata->catalog;
    elog(RLOG_DEBUG, "op:%d, newclass, %s,type:%c, %u.%u",
                    catalogdata->op,
                    newClass->ripple_class->relname.data,
                    newClass->ripple_class->relkind,
                    newClass->ripple_class->oid,
                    newClass->ripple_class->relfilenode);

    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        classInHash = hash_search(sysdicts->by_class, &newClass->oid, HASH_ENTER, &found);
        if(true == found)
        {
            if(NULL != classInHash->ripple_class)
            {
                rfree(classInHash->ripple_class);
            }
        }
        classInHash->oid = newClass->oid;

        /* 申请空间，并赋值 */
        classInHash->ripple_class = (xk_pg_sysdict_Form_pg_class)rmalloc1(sizeof(xk_pg_parser_sysdict_pgclass));
        if(NULL == classInHash->ripple_class)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(classInHash->ripple_class, 0, newClass->ripple_class, sizeof(xk_pg_parser_sysdict_pgclass));

        if(NULL != sysdicts->by_relfilenode)
        {
            relfilenode.relNode = newClass->ripple_class->relfilenode;
            relfilenode.dbNode = ripple_misc_controldata_database_get(NULL);
            if(0 == newClass->ripple_class->reltablespace)
            {
                relfilenode.spcNode = RIPPLE_PG_DFAULT_TABLESPACE;
                newClass->ripple_class->reltablespace = RIPPLE_PG_DFAULT_TABLESPACE;
            }
            else
            {
                relfilenode.spcNode = newClass->ripple_class->reltablespace;
            }

            /* 将 relfilenode 添加到 hash 表中 */
            /* 插入新的 */
            prelfilenode2oid = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_ENTER, &found);
            if(NULL == prelfilenode2oid)
            {
                elog(RLOG_ERROR, "put relfilenode2oid hash error, %u.%u.%u, oid:%u",
                                    relfilenode.spcNode,
                                    relfilenode.dbNode,
                                    relfilenode.relNode,
                                    newClass->oid);
            }
            prelfilenode2oid->oid = newClass->ripple_class->oid;
            rmemcpy1(&prelfilenode2oid->relfilenode, 0, &relfilenode, sizeof(RelFileNode));
        }
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        classInHash = hash_search(sysdicts->by_class, &newClass->oid, HASH_REMOVE, &found);

        if(NULL != classInHash)
        {
            if(NULL != classInHash->ripple_class)
            {
                relfilenode.relNode = classInHash->ripple_class->relfilenode;
                relfilenode.dbNode = ripple_misc_controldata_database_get(NULL);
                relfilenode.spcNode = classInHash->ripple_class->reltablespace;
                if(NULL != sysdicts->by_relfilenode)
                {
                    hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_REMOVE, NULL);
                }
                rfree(classInHash->ripple_class);
            }
        }
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        classInHash = hash_search(sysdicts->by_class, &newClass->oid, HASH_FIND, &found);
        if(NULL == classInHash)
        {
            elog(RLOG_WARNING, "relation %u,%s can not fond in class hash",
                                newClass->ripple_class->oid, newClass->ripple_class->relname.data);
            return;
        }

        /* 查看 relfilenode 是否发生了变化 */
        if(classInHash->ripple_class->relfilenode != newClass->ripple_class->relfilenode)
        {
            relfilenode.relNode = classInHash->ripple_class->relfilenode;
            relfilenode.dbNode = ripple_misc_controldata_database_get(NULL);
            relfilenode.spcNode = classInHash->ripple_class->reltablespace;

            /* 在对含有 toast 字段的表做 vacuum full 操作时，会修改 toast 的 relfilenode */
            if(NULL != sysdicts->by_relfilenode)
            {
                prelfilenode2oidOld = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_FIND, NULL);
                if(prelfilenode2oidOld->oid == classInHash->ripple_class->oid)
                {
                    /* 替换 relfilenode 的数据信息 */
                    hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_REMOVE, NULL);

                    /* 插入新的 */
                    relfilenode.relNode = newClass->ripple_class->relfilenode;
                    prelfilenode2oid = hash_search(sysdicts->by_relfilenode, &relfilenode, HASH_ENTER, &found);
                    if(!found)
                    {
                        rmemcpy1(&prelfilenode2oid->relfilenode, 0, &relfilenode, sizeof(RelFileNode));
                    }

                    /* 那么直接替换掉 */
                    prelfilenode2oid->oid = newClass->ripple_class->oid;
                }
            }
        }
        newClass->ripple_class->relhaspk = classInHash->ripple_class->relhaspk;
        newClass->ripple_class->relhasindex = classInHash->ripple_class->relhasindex;
        rmemcpy0(classInHash->ripple_class, 0, newClass->ripple_class, sizeof(xk_pg_parser_sysdict_pgclass));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void ripple_classcache_write(HTAB* classcache, uint64 *offset, ripple_sysdict_header_array* array)
{
    int     fd;
    uint64 page_num = 0;
    uint64 page_offset = 0;
    HASH_SEQ_STATUS status;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_class class;
    ripple_catalog_class_value *entry;

    array[RIPPLE_CATALOG_TYPE_CLASS - 1].type = RIPPLE_CATALOG_TYPE_CLASS;
    array[RIPPLE_CATALOG_TYPE_CLASS - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
    fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | O_EXCL| RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    hash_seq_init(&status,classcache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        class = entry->ripple_class;

        if(page_offset + sizeof(xk_pg_parser_sysdict_pgclass) > RIPPLE_FILE_BLK_SIZE)
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
        rmemcpy1(buffer, page_offset, class, sizeof(xk_pg_parser_sysdict_pgclass));
        page_offset += sizeof(xk_pg_parser_sysdict_pgclass);
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

    array[RIPPLE_CATALOG_TYPE_CLASS - 1].len = page_num;

    return;
}

void ripple_class_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_class_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    /* catalog 内存释放 */
    if(NULL != catalogdata->catalog)
    {
        catalog = (ripple_catalog_class_value*)catalogdata->catalog;
        if(NULL != catalog->ripple_class)
        {
            rfree(catalog->ripple_class);
        }
        rfree(catalogdata->catalog);
    }
    rfree(catalogdata);
}

/* 根据oid获取pg_class 数据 */
void* ripple_class_getbyoid(Oid oid, HTAB* by_class)
{
    bool found = false;
    ripple_catalog_class_value *classentry = NULL;
    classentry = hash_search(by_class, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return (void*)classentry->ripple_class;
}
