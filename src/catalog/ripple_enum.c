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
#include "catalog/ripple_enum.h"

void ripple_enum_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts)
{
	int i, j;
	HASHCTL hash_ctl;
	bool found = false;
	PGresult *res = NULL;
	xk_pg_sysdict_Form_pg_enum ripple_enum;
	ripple_catalog_enum_value *entry = NULL;
	const char *query = "SELECT rel.oid,rel.enumtypid, rel.enumlabel FROM pg_enum rel;";

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(xk_pg_parser_sysdict_pgenum);
	sysdicts->by_enum = hash_create("ripple_catalog_enum_value", 2048, &hash_ctl,
									HASH_ELEM | HASH_BLOBS);

	res = ripple_conn_exec(conn, query);
	if (NULL == res)
	{
		elog(RLOG_ERROR, "pg_enum query failed");
	}

	// 打印行数据
	for (i = 0; i < PQntuples(res); i++) 
	{
		ripple_enum = (xk_pg_sysdict_Form_pg_enum)rmalloc0(sizeof(xk_pg_parser_sysdict_pgenum));
		if(NULL == ripple_enum)
		{
			elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
		}
		rmemset0(ripple_enum, 0, '\0', sizeof(xk_pg_parser_sysdict_pgenum));
		j=0;
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_enum->oid);
		sscanf(PQgetvalue(res, i, j++), "%u", &ripple_enum->enumtypid);
		strcpy(ripple_enum->enumlabel.data ,PQgetvalue(res, i, j++));

		entry = (ripple_catalog_enum_value *)hash_search(sysdicts->by_enum, &ripple_enum->enumtypid, HASH_ENTER, &found);
		if (!found)
		{
			entry->enums = NIL;
		}
		entry->enumtypid = ripple_enum->enumtypid;
		entry->enums = lappend(entry->enums, ripple_enum);
	}

	PQclear(res);

	return;
}

void ripple_enumdata_write(List* ripple_enum, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_enum rippleenum = NULL;
	
	array->type = RIPPLE_CATALOG_TYPE_ENUM;
	array->offset = *offset;
	page_num = *offset;

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}

	foreach(cell, ripple_enum)
	{
		rippleenum = (xk_pg_sysdict_Form_pg_enum) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgenum) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, rippleenum, sizeof(xk_pg_parser_sysdict_pgenum));
		page_offset += sizeof(xk_pg_parser_sysdict_pgenum);
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

HTAB* ripple_enumcache_load(ripple_sysdict_header_array* array)
{
	int r = 0;
	int fd = -1;
	HTAB* enumhtab;
	HASHCTL hash_ctl;
	bool found = false;
	uint64 fileoffset = 0;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_enum rippleenum;
	ripple_catalog_enum_value *entry = NULL;

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(uint32_t);
	hash_ctl.entrysize = sizeof(ripple_catalog_enum_value);
	enumhtab = hash_create("ripple_catalog_enum_value", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	if (array[RIPPLE_CATALOG_TYPE_ENUM - 1].len == array[RIPPLE_CATALOG_TYPE_ENUM - 1].offset)
	{
		return enumhtab;
	}

	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | RIPPLE_BINARY);
	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
	}

	fileoffset = array[RIPPLE_CATALOG_TYPE_ENUM - 1].offset;
	while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
	{
		uint64 offset = 0;

		while (offset + sizeof(xk_pg_parser_sysdict_pgclass) < RIPPLE_FILE_BLK_SIZE)
		{
			rippleenum = (xk_pg_sysdict_Form_pg_enum)rmalloc1(sizeof(xk_pg_parser_sysdict_pgenum));
			if(NULL == rippleenum)
			{
				elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
			}
			rmemset0(rippleenum, 0, '\0', sizeof(xk_pg_parser_sysdict_pgenum));
			rmemcpy0(rippleenum, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgenum));
			entry = (ripple_catalog_enum_value *)hash_search(enumhtab, &rippleenum->enumtypid, HASH_ENTER, &found);
			if (!found)
			{
				entry->enums = NIL;
			}
			entry->enumtypid = rippleenum->enumtypid;
			entry->enums = lappend(entry->enums, rippleenum);
			offset += sizeof(xk_pg_parser_sysdict_pgenum);
			if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_ENUM - 1].len)
			{
				if(FileClose(fd))
				{
					elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
				}
				return enumhtab;
			}
		}
		fileoffset += RIPPLE_FILE_BLK_SIZE;
	}

	if(FileClose(fd))
	{
		elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
	}

	return enumhtab;
}

/* colvalue2enum */
ripple_catalogdata* ripple_enum_colvalue2enum(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_enum_value* enumvalue = NULL;
    xk_pg_sysdict_Form_pg_enum pgenum = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;
    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;
    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));
    enumvalue = (ripple_catalog_enum_value*)rmalloc1(sizeof(ripple_catalog_enum_value));
    if(NULL == enumvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(enumvalue, 0, '\0', sizeof(ripple_catalog_enum_value));
    catalogdata->catalog = enumvalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_ENUM;
    pgenum = (xk_pg_sysdict_Form_pg_enum)rmalloc1(sizeof(xk_pg_parser_sysdict_pgenum));
    if(NULL == pgenum)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgenum, 0, '\0', sizeof(xk_pg_parser_sysdict_pgenum));
    enumvalue->enums = lappend(enumvalue->enums, pgenum);
    /* enumlabel 3 */
    rmemcpy1(pgenum->enumlabel.data, 0, (char*)((colvalue + 3)->m_value), (colvalue + 3)->m_valueLen);

    /* enumtypid 1 */
    sscanf((char*)((colvalue + 1)->m_value), "%u", &pgenum->enumtypid);
    enumvalue->enumtypid = pgenum->enumtypid;

    /* oid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgenum->oid);

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_enum_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ListCell* lc = NULL;
    List* newenumlist = NULL;
    ripple_catalog_enum_value* newcatalog = NULL;
    ripple_catalog_enum_value* catalogInHash = NULL;
    xk_pg_sysdict_Form_pg_enum pgenum = NULL;
    xk_pg_sysdict_Form_pg_enum pgenumInHash = NULL;
    xk_pg_sysdict_Form_pg_enum duppgenum = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    /* 获取his中的 attribute 结构 */
    newcatalog = (ripple_catalog_enum_value*)(catalogdata->catalog);
    pgenum = (xk_pg_sysdict_Form_pg_enum)lfirst(list_head(newcatalog->enums));

	elog(RLOG_DEBUG, "op:%d, %u, %s", catalogdata->op, pgenum->enumtypid, pgenum->enumlabel.data);
    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
    {
        /* 插入 */
        catalogInHash = hash_search(sysdicts->by_enum, &pgenum->enumtypid, HASH_ENTER, &found);
        if(false == found)
        {
            catalogInHash->enumtypid = pgenum->enumtypid;
            catalogInHash->enums = NIL;
        }

        duppgenum = (xk_pg_sysdict_Form_pg_enum)rmalloc1(sizeof(xk_pg_parser_sysdict_pgenum));
        if(NULL == duppgenum)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(duppgenum, 0, pgenum, sizeof(xk_pg_parser_sysdict_pgenum));

        catalogInHash->enums = lappend(catalogInHash->enums, duppgenum);
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
    {
        /* 更新 */
        catalogInHash = hash_search(sysdicts->by_enum, &pgenum->enumtypid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "pgenum %u, %s can not fond in enum hash",
                                pgenum->enumtypid,
                                pgenum->enumlabel.data);
			return;
        }

        /* 遍历更新 */
        foreach(lc, catalogInHash->enums)
        {
            pgenumInHash = (xk_pg_sysdict_Form_pg_enum)lfirst(lc);
            if(strlen(pgenumInHash->enumlabel.data) != strlen(pgenum->enumlabel.data)
                || 0 != strcmp(pgenumInHash->enumlabel.data, pgenum->enumlabel.data))
            {
                continue;
            }

            /* 删除原来的，设置新的 */
            rmemcpy0(pgenumInHash, 0, pgenum, sizeof(xk_pg_parser_sysdict_pgenum));
            return;
        }

        /* 没有匹配上,那么说明有问题，直接 append 上并WARNING */
        elog(RLOG_WARNING, "enum %u, %s can not fond in enum hash",
                            pgenum->enumtypid,
                            pgenum->enumlabel.data);
        
        duppgenum = (xk_pg_sysdict_Form_pg_enum)rmalloc1(sizeof(xk_pg_parser_sysdict_pgenum));
        if(NULL == duppgenum)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(duppgenum, 0, pgenum, sizeof(xk_pg_parser_sysdict_pgenum));

        catalogInHash->enums = lappend(catalogInHash->enums, duppgenum);
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
    {
        /* 删除 */
        catalogInHash = hash_search(sysdicts->by_enum, &pgenum->enumtypid, HASH_FIND, &found);
        if(NULL == catalogInHash)
        {
            elog(RLOG_WARNING, "enum %u, %s can not fond in enum hash",
                            pgenum->enumtypid,
                            pgenum->enumlabel.data);
            return;
        }

        foreach(lc, catalogInHash->enums)
        {
            pgenumInHash = (xk_pg_sysdict_Form_pg_enum)lfirst(lc);
            if(strlen(pgenumInHash->enumlabel.data) != strlen(pgenum->enumlabel.data)
                || 0 != strcmp(pgenumInHash->enumlabel.data, pgenum->enumlabel.data))
            {
                newenumlist = lappend(newenumlist, pgenumInHash);
                continue;
            }
            rfree(pgenumInHash);
        }

        list_free(catalogInHash->enums);
        catalogInHash->enums = newenumlist;

        if(NULL == catalogInHash->enums)
        {
            /* 说明为最后一项，那么直接删除掉hash项 */
            hash_search(sysdicts->by_enum, &catalogInHash->enumtypid, HASH_REMOVE, NULL);
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}
void ripple_enumcache_write(HTAB* enumscache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	List* ripple_enum = NULL;
	ListCell*	cell = NULL;
	HASH_SEQ_STATUS status;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_enum rippleenum;
	ripple_catalog_enum_value *entry = NULL;

	array[RIPPLE_CATALOG_TYPE_ENUM - 1].type = RIPPLE_CATALOG_TYPE_ENUM;
	array[RIPPLE_CATALOG_TYPE_ENUM - 1].offset = *offset;
	page_num = *offset;
	
	hash_seq_init(&status,enumscache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		foreach(cell, entry->enums)
		{
			ripple_enum = lappend(ripple_enum, lfirst(cell));
		}
	}
	
	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}

	foreach(cell, ripple_enum)
	{
		rippleenum = (xk_pg_sysdict_Form_pg_enum) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgenum) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, rippleenum, sizeof(xk_pg_parser_sysdict_pgenum));
		page_offset += sizeof(xk_pg_parser_sysdict_pgenum);
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
	array[RIPPLE_CATALOG_TYPE_ENUM - 1].len = page_num;

	list_free(ripple_enum);

}

void ripple_enum_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_enum_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    if(NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    catalog = (ripple_catalog_enum_value*)catalogdata->catalog;
    list_free_deep(catalog->enums);

    rfree(catalogdata->catalog);
    rfree(catalogdata);
}
