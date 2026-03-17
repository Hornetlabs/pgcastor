#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/conn.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/attribute.h"

void attributedata_write(List* attribute, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	char buffer[FILE_BLK_SIZE];
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	pg_sysdict_Form_pg_attribute attr_data = NULL;

	array->type = CATALOG_TYPE_ATTRIBUTE;
	array->offset = *offset;
	page_num = *offset;
	
	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
	}
	foreach(cell, attribute)
	{
		attr_data = (pg_sysdict_Form_pg_attribute) lfirst(cell);

		if(page_offset + sizeof(pg_parser_sysdict_pgattributes) > FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, attr_data, sizeof(pg_parser_sysdict_pgattributes));
		page_offset += sizeof(pg_parser_sysdict_pgattributes);

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

HTAB* attributecache_load(sysdict_header_array* array)
{
	int r = 0;
	int fd = -1;
	HTAB* attributehtab;
	HASHCTL hash_ctl;
	bool found = false;
	uint64 fileoffset = 0;
	char buffer[FILE_BLK_SIZE];
	pg_sysdict_Form_pg_attribute attr;
	catalog_attribute_value *entry = NULL;

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(uint32_t);
	hash_ctl.entrysize = sizeof(catalog_attribute_value);
	attributehtab = hash_create("catalog_attribute_value", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	if (array[CATALOG_TYPE_ATTRIBUTE - 1].len == array[CATALOG_TYPE_ATTRIBUTE - 1].offset)
	{
		return attributehtab;
	}

	fd = osal_basic_open_file(SYSDICTS_FILE,
						O_RDWR | BINARY);
	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
	}

	fileoffset = array[CATALOG_TYPE_ATTRIBUTE - 1].offset;
	while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0) 
	{
		uint64 offset = 0;

		while (offset + sizeof(pg_parser_sysdict_pgattributes) < FILE_BLK_SIZE)
		{
			attr = (pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(pg_parser_sysdict_pgattributes));
			if(NULL == attr)
			{
				elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
			}
			rmemset0(attr, 0, '\0', sizeof(pg_parser_sysdict_pgattributes));
			rmemcpy0(attr, 0, buffer + offset, sizeof(pg_parser_sysdict_pgattributes));
			entry = (catalog_attribute_value *)hash_search(attributehtab, &attr->attrelid, HASH_ENTER, &found);
			if (!found)
			{
				entry->attrs = NIL;
			}
			entry->attrelid = attr->attrelid;
			entry->attrs = lappend(entry->attrs, attr);
			offset += sizeof(pg_parser_sysdict_pgattributes);

			if (fileoffset + offset == array[CATALOG_TYPE_ATTRIBUTE - 1].len)
			{
				if(osal_file_close(fd))
				{
					elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
				}
				return attributehtab;
			}
		}
		fileoffset += FILE_BLK_SIZE;
	}

	if(osal_file_close(fd))
	{
		elog(RLOG_ERROR, "could not close file %s, %s", SYSDICTS_FILE, strerror(errno));
	}
	return attributehtab;
}

/* colvalue2attr */
catalogdata* class_colvalue2attribute(void* in_colvalue)
{
    catalogdata* catalog_data = NULL;
    catalog_attribute_value* attrvalue = NULL;
    pg_sysdict_Form_pg_attribute pgattribute = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if(NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    attrvalue = (catalog_attribute_value*)rmalloc1(sizeof(catalog_attribute_value));
    if(NULL == attrvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(attrvalue, 0, '\0', sizeof(catalog_attribute_value));
    catalog_data->catalog = attrvalue;
    catalog_data->type = CATALOG_TYPE_ATTRIBUTE;

    pgattribute = (pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(pg_parser_sysdict_pgattributes));
    if(NULL == pgattribute)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgattribute, 0, '\0', sizeof(pg_parser_sysdict_pgattributes));
    attrvalue->attrs = lappend(attrvalue->attrs, pgattribute);

    /* attalign 11 */
    pgattribute->attalign = ((char*)((colvalue + 11)->m_value))[0];

    /* attbyval 9 */
    pgattribute->attbyval = ((char*)((colvalue + 9)->m_value))[0] == 't' ? true : false;

    /* attcacheoff 7 */
    sscanf((char*)((colvalue + 7)->m_value), "%d", &pgattribute->attcacheoff);

    /* attcollation 20 */
    sscanf((char*)((colvalue + 20)->m_value), "%u", &pgattribute->attcollation);

    /* attgenerated 16 */
    pgattribute->attgenerated = ((char*)((colvalue + 16)->m_value))[0];

    /* atthasdef 13 */
    pgattribute->atthasdef = ((char*)((colvalue + 13)->m_value))[0] == 't' ? true : false;

    /* atthasmissing 14 */
    pgattribute->atthasmissing = ((char*)((colvalue + 14)->m_value))[0] == 't' ? true : false;

    /* attidentity 15 */
    pgattribute->attidentity = ((char*)((colvalue + 15)->m_value))[0] == 't' ? true : false;

    /* attinhcount 19 */
    sscanf((char*)((colvalue + 19)->m_value), "%d", &pgattribute->attinhcount);

    /* attisdropped 17 */
    pgattribute->attisdropped = ((char*)((colvalue + 17)->m_value))[0] == 't' ? true : false;

    /* attislocal 18 */
    pgattribute->attislocal = ((char*)((colvalue + 18)->m_value))[0] == 't' ? true : false;

    /* attlen 4 */
    sscanf((char*)((colvalue + 4)->m_value), "%hd", &pgattribute->attlen);

    /* attname 1 */
    rmemcpy1(pgattribute->attname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* attndims 6 */
    sscanf((char*)((colvalue + 6)->m_value), "%d", &pgattribute->attndims);

    /* attndims 12 */
    pgattribute->attnotnull = ((char*)((colvalue + 12)->m_value))[0] == 't' ? true : false;

    /* attnum 5 */
    sscanf((char*)((colvalue + 5)->m_value), "%hd", &pgattribute->attnum);

    /* attrelid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgattribute->attrelid);
    attrvalue->attrelid = pgattribute->attrelid;

    /* attstattarget 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%d", &pgattribute->attstattarget);

    /* attstattarget 10 */
    pgattribute->attstorage = ((char*)((colvalue + 10)->m_value))[0];

    /* atttypid 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgattribute->atttypid);

    /* atttypmod 8 */
    sscanf((char*)((colvalue + 8)->m_value), "%d", &pgattribute->atttypmod);

    return catalog_data;
}

catalogdata* class_colvalue2attribute_pg14(void* in_colvalue)
{
    catalogdata* catalog_data = NULL;
    catalog_attribute_value* attrvalue = NULL;
    pg_sysdict_Form_pg_attribute pgattribute = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalog_data = (catalogdata*)rmalloc1(sizeof(catalogdata));
    if(NULL == catalog_data)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalog_data, 0, '\0', sizeof(catalogdata));

    attrvalue = (catalog_attribute_value*)rmalloc1(sizeof(catalog_attribute_value));
    if(NULL == attrvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(attrvalue, 0, '\0', sizeof(catalog_attribute_value));
    catalog_data->catalog = attrvalue;
    catalog_data->type = CATALOG_TYPE_ATTRIBUTE;

    pgattribute = (pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(pg_parser_sysdict_pgattributes));
    if(NULL == pgattribute)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgattribute, 0, '\0', sizeof(pg_parser_sysdict_pgattributes));
    attrvalue->attrs = lappend(attrvalue->attrs, pgattribute);

    /* attalign 10, different from pg12 */
    pgattribute->attalign = ((char*)((colvalue + 10)->m_value))[0];

    /* attbyval 9 */
    pgattribute->attbyval = ((char*)((colvalue + 9)->m_value))[0] == 't' ? true : false;

    /* attcacheoff 7 */
    sscanf((char*)((colvalue + 7)->m_value), "%d", &pgattribute->attcacheoff);

    /* attcollation 22, different from pg12 */
    sscanf((char*)((colvalue + 22)->m_value), "%u", &pgattribute->attcollation);

    /* attgenerated 17, different from pg12 */
    pgattribute->attgenerated = ((char*)((colvalue + 17)->m_value))[0];

    /* atthasdef 14, different from pg12 */
    pgattribute->atthasdef = ((char*)((colvalue + 14)->m_value))[0] == 't' ? true : false;

    /* atthasmissing 15, different from pg12 */
    pgattribute->atthasmissing = ((char*)((colvalue + 15)->m_value))[0] == 't' ? true : false;

    /* attidentity 16, different from pg12 */
    pgattribute->attidentity = ((char*)((colvalue + 16)->m_value))[0] == 't' ? true : false;

    /* attinhcount 21, different from pg12 */
    sscanf((char*)((colvalue + 21)->m_value), "%d", &pgattribute->attinhcount);

    /* attisdropped 18, different from pg12 */
    pgattribute->attisdropped = ((char*)((colvalue + 18)->m_value))[0] == 't' ? true : false;

    /* attislocal 20, different from pg12 */
    pgattribute->attislocal = ((char*)((colvalue + 20)->m_value))[0] == 't' ? true : false;

    /* attlen 4 */
    sscanf((char*)((colvalue + 4)->m_value), "%hd", &pgattribute->attlen);

    /* attname 1 */
    rmemcpy1(pgattribute->attname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* attndims 6 */
    sscanf((char*)((colvalue + 6)->m_value), "%d", &pgattribute->attndims);

    /* attnotnull 13, different from pg12 */
    pgattribute->attnotnull = ((char*)((colvalue + 13)->m_value))[0] == 't' ? true : false;

    /* attnum 5 */
    sscanf((char*)((colvalue + 5)->m_value), "%hd", &pgattribute->attnum);

    /* attrelid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgattribute->attrelid);
    attrvalue->attrelid = pgattribute->attrelid;

    /* attstattarget 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%d", &pgattribute->attstattarget);

    /* attstorage 11, different from pg12 */
    pgattribute->attstorage = ((char*)((colvalue + 11)->m_value))[0];

    /* atttypid 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgattribute->atttypid);

    /* atttypmod 8 */
    sscanf((char*)((colvalue + 8)->m_value), "%d", &pgattribute->atttypmod);

    return catalog_data;
}

/* catalogdata2transcache */
void attribute_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool found = false;
    ListCell* lc = NULL;
    List* newattrlist = NULL;
    catalog_attribute_value* newAttr = NULL;
    catalog_attribute_value* attrInHash = NULL;
    pg_sysdict_Form_pg_attribute pgattr = NULL;
    pg_sysdict_Form_pg_attribute pgattrInHash = NULL;

    pg_sysdict_Form_pg_attribute dupattr = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    /* 获取his中的 attribute 结构 */
    newAttr = (catalog_attribute_value*)(catalogdata->catalog);
    pgattr = (pg_sysdict_Form_pg_attribute)lfirst(list_head(newAttr->attrs));

    if(CATALOG_OP_INSERT == catalogdata->op)
    {
        if(0 > pgattr->attnum)
        {
            return;
        }

        /* 插入 */
        attrInHash = hash_search(sysdicts->by_attribute, &pgattr->attrelid, HASH_ENTER, &found);
        if(false == found)
        {
            attrInHash->attrelid = pgattr->attrelid;
            attrInHash->attrs = NIL;
        }

        /* 在链表中查看是否存在，不存在则添加，存在则覆盖 */
        foreach(lc, attrInHash->attrs)
        {
            pgattrInHash = (pg_sysdict_Form_pg_attribute)lfirst(lc);
            if(pgattrInHash->attnum != pgattr->attnum)
            {
                continue;
            }

            /* 删除原来的，设置新的 */
            rmemcpy0(pgattrInHash, 0, pgattr, sizeof(pg_parser_sysdict_pgattributes));
            return;
        }

        /* 重新分配空间 */
        dupattr = (pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(pg_parser_sysdict_pgattributes));
        if(NULL == dupattr)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(dupattr, 0, pgattr, sizeof(pg_parser_sysdict_pgattributes));

        attrInHash->attrs = lappend(attrInHash->attrs, dupattr);
    }
    else if(CATALOG_OP_UPDATE == catalogdata->op)
    {
        /* 更新 */
        attrInHash = hash_search(sysdicts->by_attribute, &pgattr->attrelid, HASH_FIND, &found);
        if(NULL == attrInHash)
        {
            elog(RLOG_WARNING, "attribute %u, %s can not fond in attribute hash",
                                pgattr->attrelid,
                                pgattr->attname.data);
            return;
        }

        /* 遍历更新 */
        foreach(lc, attrInHash->attrs)
        {
            pgattrInHash = (pg_sysdict_Form_pg_attribute)lfirst(lc);
            if(pgattrInHash->attnum != pgattr->attnum)
            {
                continue;
            }

            /* 删除原来的，设置新的 */
            rmemcpy0(pgattrInHash, 0, pgattr, sizeof(pg_parser_sysdict_pgattributes));
            return;
        }

        /* 没有匹配上,那么说明有问题，直接 append 上并WARNING */
        elog(RLOG_WARNING, "attribute %u, %s can not fond in attribute hash",
                            pgattr->attrelid,
                            pgattr->attname.data);

        /* 重新分配空间 */
        dupattr = (pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(pg_parser_sysdict_pgattributes));
        if(NULL == dupattr)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(dupattr, 0, pgattr, sizeof(pg_parser_sysdict_pgattributes));

        attrInHash->attrs = lappend(attrInHash->attrs, dupattr);
    }
    else if(CATALOG_OP_DELETE == catalogdata->op)
    {
        /* 删除 */
        attrInHash = hash_search(sysdicts->by_attribute, &pgattr->attrelid, HASH_FIND, &found);
        if(NULL == attrInHash)
        {
            elog(RLOG_WARNING, "attribute %u, %s can not fond in attribute hash",
                                pgattr->attrelid,
                                pgattr->attname.data);
            return;
        }

        foreach(lc, attrInHash->attrs)
        {
            pgattrInHash = (pg_sysdict_Form_pg_attribute)lfirst(lc);
            if(pgattrInHash->attnum != pgattr->attnum)
            {
                newattrlist = lappend(newattrlist, pgattrInHash);
                continue;
            }

            rfree(pgattrInHash);
        }

        list_free(attrInHash->attrs);
        attrInHash->attrs = newattrlist;

        if(NULL == attrInHash->attrs)
        {
            /* 说明为最后一项，那么直接删除掉hash项 */
            hash_search(sysdicts->by_attribute, &attrInHash->attrelid, HASH_REMOVE, NULL);
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void attributecache_write(HTAB* attributecache, uint64 *offset, sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	HASH_SEQ_STATUS status;
	List* attribute = NULL;
	char buffer[FILE_BLK_SIZE];
	catalog_attribute_value *entry = NULL;
	pg_sysdict_Form_pg_attribute attr_data = NULL;

	array[CATALOG_TYPE_ATTRIBUTE - 1].type = CATALOG_TYPE_ATTRIBUTE;
	array[CATALOG_TYPE_ATTRIBUTE - 1].offset = *offset;
	page_num = *offset;
	
	hash_seq_init(&status,attributecache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		foreach(cell, entry->attrs)
		{
			attribute = lappend(attribute, lfirst(cell));
		}
	}

	rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
	fd = osal_basic_open_file(SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
	}
	foreach(cell, attribute)
	{
		attr_data = (pg_sysdict_Form_pg_attribute) lfirst(cell);

		if(page_offset + sizeof(pg_parser_sysdict_pgattributes) > FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, attr_data, sizeof(pg_parser_sysdict_pgattributes));
		page_offset += sizeof(pg_parser_sysdict_pgattributes);

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
	array[CATALOG_TYPE_ATTRIBUTE - 1].len = page_num;
	list_free(attribute);
}

void attribute_catalogdatafree(catalogdata* catalogdata)
{
    catalog_attribute_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    if(NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    catalog = (catalog_attribute_value*)catalogdata->catalog;
    list_free_deep(catalog->attrs);

    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

/* 根据oid获取pg_attribute 数据 */
void* attribute_getbyoid(Oid oid, HTAB* by_attribute)
{
    bool found = false;
    catalog_attribute_value *attrentry = NULL;
    attrentry = hash_search(by_attribute, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return (void*)attrentry->attrs;
}
