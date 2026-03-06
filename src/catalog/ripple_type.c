#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/ripple_conn.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_type.h"

static bool bool_judgment(char * str)
{
	if (str[0] == 't' || str[0] == 'T') {
        return true;
    } else if (str[0] == 'f' || str[0] == 'F') {
        return false;
    }
	return -1;
}

void ripple_type_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
	int i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res  = NULL;
	ripple_catalog_type_value *entry = NULL;
	xk_pg_sysdict_Form_pg_type ripple_type = NULL;
	const char *query = "SELECT rel.oid,rel.typname, rel.typlen,rel.typbyval, rel.typtype, rel.typdelim, rel.typelem, rel.typoutput::oid, rel.typrelid, rel.typalign, rel.typstorage FROM pg_type rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(ripple_catalog_type_value);
	sysdicts->by_type = hash_create("ripple_catalog_sysdicts_type", 2048, &hash_ctl,
										HASH_ELEM | HASH_BLOBS);

	res = ripple_conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_type query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		ripple_type = (xk_pg_sysdict_Form_pg_type)rmalloc0(sizeof(xk_pg_parser_sysdict_pgtype));
		if(NULL == ripple_type)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(ripple_type, 0, '\0', sizeof(xk_pg_parser_sysdict_pgtype));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_type->oid);
		strcpy(ripple_type->typname.data ,PQgetvalue(res, i, j++));
		sscanf(PQgetvalue(res, i, j++), "%hd", &ripple_type->typlen);
		ripple_type->typbyval = bool_judgment(PQgetvalue(res, i, j++));
		sscanf(PQgetvalue(res, i, j++), "%c", &ripple_type->typtype);
		sscanf(PQgetvalue(res, i, j++), "%c", &ripple_type->typdelim);
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_type->typelem);
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_type->typoutput);
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_type->typrelid);
		sscanf(PQgetvalue(res, i, j++), "%c", &ripple_type->typalign);
		sscanf(PQgetvalue(res, i, j++), "%c", &ripple_type->typstorage);

		entry = hash_search(sysdicts->by_type, &ripple_type->oid, HASH_ENTER, &found);
		if(found)
		{
			elog(RLOG_ERROR, "type_oid:%u already exist in by_type", entry->ripple_type->oid);
		}
		entry->oid = ripple_type->oid;
		entry->ripple_type = ripple_type;
	}

	PQclear(res);
	return ;
}

void ripple_typedata_write(List* ripple_type, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_type type;

	array->type = RIPPLE_CATALOG_TYPE_TYPE;
	array->offset = *offset;
	page_num = *offset;
	
	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}

	foreach(cell, ripple_type)
	{
		type = (xk_pg_sysdict_Form_pg_type) lfirst(cell);
		if(page_offset + sizeof(xk_pg_parser_sysdict_pgtype) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, type, sizeof(xk_pg_parser_sysdict_pgtype));
		page_offset += sizeof(xk_pg_parser_sysdict_pgtype);
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

HTAB* ripple_typecache_load(ripple_sysdict_header_array* array)
{
    int r = 0;
    int fd = -1;
    HTAB* typehtab;
    HASHCTL hash_ctl;
    bool found = false;
    uint64 fileoffset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    xk_pg_sysdict_Form_pg_type type;
    ripple_catalog_type_value *entry = NULL;

    rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(ripple_catalog_type_value);
    typehtab = hash_create("ripple_catalog_type_value", 2048, &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS);

    if (array[RIPPLE_CATALOG_TYPE_TYPE - 1].len == array[RIPPLE_CATALOG_TYPE_TYPE - 1].offset)
    {
        return typehtab;
    }

    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }

    fileoffset = array[RIPPLE_CATALOG_TYPE_TYPE - 1].offset;
    while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
    {
        uint64 offset = 0;

        while (offset + sizeof(xk_pg_parser_sysdict_pgtype) < RIPPLE_FILE_BLK_SIZE)
        {
            type = (xk_pg_sysdict_Form_pg_type)rmalloc1(sizeof(xk_pg_parser_sysdict_pgtype));
            if(NULL == type)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(type, 0, '\0', sizeof(xk_pg_parser_sysdict_pgtype));
            rmemcpy0(type, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgtype));
            entry = hash_search(typehtab, &type->oid, HASH_ENTER, &found);
            if(found)
            {
                elog(RLOG_ERROR, "type_oid:%u already exist in by_type", entry->ripple_type->oid);
            }
            entry->oid = type->oid;
            entry->ripple_type = type;
            offset += sizeof(xk_pg_parser_sysdict_pgtype);
            if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_TYPE - 1].len)
            {
                if(FileClose(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
                }
                return typehtab;
            }
        }
        fileoffset += RIPPLE_FILE_BLK_SIZE;
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    return typehtab;
}

/* colvalue2type */
ripple_catalogdata* ripple_type_colvalue2type(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_type_value* typevalue = NULL;
    xk_pg_sysdict_Form_pg_type pgtype = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc0(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    typevalue = (ripple_catalog_type_value*)rmalloc0(sizeof(ripple_catalog_type_value));
    if(NULL == typevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(typevalue, 0, '\0', sizeof(ripple_catalog_type_value));
    catalogdata->catalog = typevalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_TYPE;

    pgtype = (xk_pg_sysdict_Form_pg_type)rmalloc1(sizeof(xk_pg_parser_sysdict_pgtype));
    if(NULL == pgtype)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgtype, 0, '\0', sizeof(xk_pg_parser_sysdict_pgtype));
    typevalue->ripple_type = pgtype;

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

    return catalogdata;
}

/* colvalue2type----pg14 */
ripple_catalogdata* ripple_type_colvalue2type_pg14(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_type_value* typevalue = NULL;
    xk_pg_sysdict_Form_pg_type pgtype = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc0(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    typevalue = (ripple_catalog_type_value*)rmalloc0(sizeof(ripple_catalog_type_value));
    if(NULL == typevalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(typevalue, 0, '\0', sizeof(ripple_catalog_type_value));
    catalogdata->catalog = typevalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_TYPE;

    pgtype = (xk_pg_sysdict_Form_pg_type)rmalloc1(sizeof(xk_pg_parser_sysdict_pgtype));
    if(NULL == pgtype)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgtype, 0, '\0', sizeof(xk_pg_parser_sysdict_pgtype));
    typevalue->ripple_type = pgtype;

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

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_type_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ripple_catalog_type_value* newcatalog = NULL;
    ripple_catalog_type_value* catalogInHash = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (ripple_catalog_type_value*)catalogdata->catalog;
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_type, &newcatalog->oid, HASH_ENTER, &found);
        if(true == found)
        {
            elog(RLOG_DEBUG, "by_type hash duplicate oid, %u, %s",
                                catalogInHash->ripple_type->oid, catalogInHash->ripple_type->typname.data);
            if(NULL != catalogInHash->ripple_type)
            {
                rfree(catalogInHash->ripple_type);
            }
        }
        catalogInHash->oid = newcatalog->oid;

		catalogInHash->ripple_type = (xk_pg_sysdict_Form_pg_type)rmalloc1(sizeof(xk_pg_parser_sysdict_pgtype));
		if(NULL == catalogInHash->ripple_type)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
        rmemcpy0(catalogInHash->ripple_type, 0, newcatalog->ripple_type, sizeof(xk_pg_parser_sysdict_pgtype));
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_type, &newcatalog->oid, HASH_REMOVE, &found);
        if(NULL != catalogInHash)
        {
            if(NULL != catalogInHash->ripple_type)
            {
                rfree(catalogInHash->ripple_type);
            }
        }
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash = hash_search(sysdicts->by_type, &newcatalog->oid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "type %s,%u can not fond in type hash",
                                newcatalog->ripple_type->oid, newcatalog->ripple_type->typname.data);
			return;
        }

        rfree(catalogInHash->ripple_type);

		catalogInHash->ripple_type = (xk_pg_sysdict_Form_pg_type)rmalloc1(sizeof(xk_pg_parser_sysdict_pgtype));
		if(NULL == catalogInHash->ripple_type)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
        rmemcpy0(catalogInHash->ripple_type, 0, newcatalog->ripple_type, sizeof(xk_pg_parser_sysdict_pgtype));
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}
void ripple_typecache_write(HTAB* typecache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	HASH_SEQ_STATUS status;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	ripple_catalog_type_value *entry = NULL;
	xk_pg_sysdict_Form_pg_type type = NULL;

	array[RIPPLE_CATALOG_TYPE_TYPE - 1].type = RIPPLE_CATALOG_TYPE_TYPE;
	array[RIPPLE_CATALOG_TYPE_TYPE - 1].offset = *offset;
	page_num = *offset;
	
	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}

	hash_seq_init(&status,typecache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		type = entry->ripple_type;

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgtype) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, type, sizeof(xk_pg_parser_sysdict_pgtype));
		page_offset += sizeof(xk_pg_parser_sysdict_pgtype);
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
	array[RIPPLE_CATALOG_TYPE_TYPE - 1].len = page_num;
	
}

/* 根据oid获取pg_type 数据 */
void* ripple_type_getbyoid(Oid oid, HTAB* by_type)
{
    bool found = false;
    ripple_catalog_type_value *typeentry = NULL;
    typeentry = hash_search(by_type, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return (void*)typeentry->ripple_type;
}

void ripple_type_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_type_value* catalog = NULL;
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
    catalog = (ripple_catalog_type_value*)catalogdata->catalog;
    if(NULL != catalog->ripple_type)
    {
        rfree(catalog->ripple_type);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
