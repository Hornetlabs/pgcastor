#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/conn/ripple_conn.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_attribute.h"

void ripple_attributedata_write(List* ripple_attribute, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	xk_pg_sysdict_Form_pg_attribute attribute = NULL;

	array->type = RIPPLE_CATALOG_TYPE_ATTRIBUTE;
	array->offset = *offset;
	page_num = *offset;
	
	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_FILE);
	}
	foreach(cell, ripple_attribute)
	{
		attribute = (xk_pg_sysdict_Form_pg_attribute) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgattributes) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, attribute, sizeof(xk_pg_parser_sysdict_pgattributes));
		page_offset += sizeof(xk_pg_parser_sysdict_pgattributes);

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

HTAB* ripple_attributecache_load(ripple_sysdict_header_array* array)
{
	int r = 0;
	int fd = -1;
	HTAB* attributehtab;
	HASHCTL hash_ctl;
	bool found = false;
	uint64 fileoffset = 0;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	xk_pg_sysdict_Form_pg_attribute attr;
	ripple_catalog_attribute_value *entry = NULL;

	rmemset1(&hash_ctl, 0, '\0', sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(uint32_t);
	hash_ctl.entrysize = sizeof(ripple_catalog_attribute_value);
	attributehtab = hash_create("ripple_catalog_attribute_value", 2048, &hash_ctl,
								 HASH_ELEM | HASH_BLOBS);

	if (array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].len == array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].offset)
	{
		return attributehtab;
	}

	fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
						O_RDWR | RIPPLE_BINARY);
	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
	}

	fileoffset = array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].offset;
	while ((r = FilePRead(fd, buffer, RIPPLE_FILE_BLK_SIZE, fileoffset)) > 0) 
	{
		uint64 offset = 0;

		while (offset + sizeof(xk_pg_parser_sysdict_pgattributes) < RIPPLE_FILE_BLK_SIZE)
		{
			attr = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
			if(NULL == attr)
			{
				elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
			}
			rmemset0(attr, 0, '\0', sizeof(xk_pg_parser_sysdict_pgattributes));
			rmemcpy0(attr, 0, buffer + offset, sizeof(xk_pg_parser_sysdict_pgattributes));
			entry = (ripple_catalog_attribute_value *)hash_search(attributehtab, &attr->attrelid, HASH_ENTER, &found);
			if (!found)
			{
				entry->attrs = NIL;
			}
			entry->attrelid = attr->attrelid;
			entry->attrs = lappend(entry->attrs, attr);
			offset += sizeof(xk_pg_parser_sysdict_pgattributes);

			if (fileoffset + offset == array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].len)
			{
				if(FileClose(fd))
				{
					elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
				}
				return attributehtab;
			}
		}
		fileoffset += RIPPLE_FILE_BLK_SIZE;
	}

	if(FileClose(fd))
	{
		elog(RLOG_ERROR, "could not close file %s, %s", RIPPLE_SYSDICTS_FILE, strerror(errno));
	}
	return attributehtab;
}

/* colvalue2attr */
ripple_catalogdata* ripple_class_colvalue2attribute(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_attribute_value* attrvalue = NULL;
    xk_pg_sysdict_Form_pg_attribute pgattribute = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    attrvalue = (ripple_catalog_attribute_value*)rmalloc1(sizeof(ripple_catalog_attribute_value));
    if(NULL == attrvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(attrvalue, 0, '\0', sizeof(ripple_catalog_attribute_value));
    catalogdata->catalog = attrvalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_ATTRIBUTE;

    pgattribute = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
    if(NULL == pgattribute)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgattribute, 0, '\0', sizeof(xk_pg_parser_sysdict_pgattributes));
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

    return catalogdata;
}

ripple_catalogdata* ripple_class_colvalue2attribute_pg14(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_attribute_value* attrvalue = NULL;
    xk_pg_sysdict_Form_pg_attribute pgattribute = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    attrvalue = (ripple_catalog_attribute_value*)rmalloc1(sizeof(ripple_catalog_attribute_value));
    if(NULL == attrvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(attrvalue, 0, '\0', sizeof(ripple_catalog_attribute_value));
    catalogdata->catalog = attrvalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_ATTRIBUTE;

    pgattribute = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
    if(NULL == pgattribute)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgattribute, 0, '\0', sizeof(xk_pg_parser_sysdict_pgattributes));
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

    return catalogdata;
}

/* colvalue2attr */
ripple_catalogdata* ripple_class_colvalue2attribute_hg902(void* in_colvalue)
{
    ripple_catalogdata* catalogdata = NULL;
    ripple_catalog_attribute_value* attrvalue = NULL;
    xk_pg_sysdict_Form_pg_attribute pgattribute = NULL;
    xk_pg_parser_translog_tbcol_value* colvalue = NULL;

    colvalue = (xk_pg_parser_translog_tbcol_value*)in_colvalue;

    /* 值转换 */
    catalogdata = (ripple_catalogdata*)rmalloc1(sizeof(ripple_catalogdata));
    if(NULL == catalogdata)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogdata, 0, '\0', sizeof(ripple_catalogdata));

    attrvalue = (ripple_catalog_attribute_value*)rmalloc1(sizeof(ripple_catalog_attribute_value));
    if(NULL == attrvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(attrvalue, 0, '\0', sizeof(ripple_catalog_attribute_value));
    catalogdata->catalog = attrvalue;
    catalogdata->type = RIPPLE_CATALOG_TYPE_ATTRIBUTE;

    pgattribute = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
    if(NULL == pgattribute)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgattribute, 0, '\0', sizeof(xk_pg_parser_sysdict_pgattributes));
    attrvalue->attrs = lappend(attrvalue->attrs, pgattribute);

    /* attalign 11 */
    pgattribute->attalign = ((char*)((colvalue + 11)->m_value))[0];

    /* attbyval 9 */
    pgattribute->attbyval = ((char*)((colvalue + 9)->m_value))[0] == 't' ? true : false;

    /* attcacheoff 7 */
    sscanf((char*)((colvalue + 7)->m_value), "%d", &pgattribute->attcacheoff);

    /* attcollation 18 */
    sscanf((char*)((colvalue + 18)->m_value), "%u", &pgattribute->attcollation);

    /* attgenerated 14 */
    pgattribute->attgenerated = ((char*)((colvalue + 14)->m_value))[0];

    /* atthasdef 13 */
    pgattribute->atthasdef = ((char*)((colvalue + 13)->m_value))[0] == 't' ? true : false;

    /* attinhcount 17 */
    sscanf((char*)((colvalue + 17)->m_value), "%d", &pgattribute->attinhcount);

    /* attisdropped 15 */
    pgattribute->attisdropped = ((char*)((colvalue + 15)->m_value))[0] == 't' ? true : false;

    /* attislocal 16 */
    pgattribute->attislocal = ((char*)((colvalue + 16)->m_value))[0] == 't' ? true : false;

    /* attlen 4 */
    sscanf((char*)((colvalue + 4)->m_value), "%hd", &pgattribute->attlen);

    /* attname 1 */
    rmemcpy1(pgattribute->attname.data, 0, (char*)((colvalue + 1)->m_value), (colvalue + 1)->m_valueLen);

    /* attndims 6 */
    sscanf((char*)((colvalue + 6)->m_value), "%d", &pgattribute->attndims);

    /* attnotnull 12 */
    pgattribute->attnotnull = ((char*)((colvalue + 12)->m_value))[0] == 't' ? true : false;

    /* attnum 5 */
    sscanf((char*)((colvalue + 5)->m_value), "%hd", &pgattribute->attnum);

    /* attrelid 0 */
    sscanf((char*)((colvalue + 0)->m_value), "%u", &pgattribute->attrelid);
    attrvalue->attrelid = pgattribute->attrelid;

    /* attstattarget 3 */
    sscanf((char*)((colvalue + 3)->m_value), "%d", &pgattribute->attstattarget);

    /* attstorage 10 */
    pgattribute->attstorage = ((char*)((colvalue + 10)->m_value))[0];

    /* atttypid 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgattribute->atttypid);

    /* atttypmod 8 */
    sscanf((char*)((colvalue + 8)->m_value), "%d", &pgattribute->atttypmod);

    return catalogdata;
}

/* catalogdata2transcache */
void ripple_attribute_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata)
{
    bool found = false;
    ListCell* lc = NULL;
    List* newattrlist = NULL;
    ripple_catalog_attribute_value* newAttr = NULL;
    ripple_catalog_attribute_value* attrInHash = NULL;
    xk_pg_sysdict_Form_pg_attribute pgattr = NULL;
    xk_pg_sysdict_Form_pg_attribute pgattrInHash = NULL;

    xk_pg_sysdict_Form_pg_attribute dupattr = NULL;

    if(NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    /* 获取his中的 attribute 结构 */
    newAttr = (ripple_catalog_attribute_value*)(catalogdata->catalog);
    pgattr = (xk_pg_sysdict_Form_pg_attribute)lfirst(list_head(newAttr->attrs));

    if(RIPPLE_CATALOG_OP_INSERT == catalogdata->op)
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
            pgattrInHash = (xk_pg_sysdict_Form_pg_attribute)lfirst(lc);
            if(pgattrInHash->attnum != pgattr->attnum)
            {
                continue;
            }

            /* 删除原来的，设置新的 */
            rmemcpy0(pgattrInHash, 0, pgattr, sizeof(xk_pg_parser_sysdict_pgattributes));
            return;
        }

        /* 重新分配空间 */
        dupattr = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
        if(NULL == dupattr)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(dupattr, 0, pgattr, sizeof(xk_pg_parser_sysdict_pgattributes));

        attrInHash->attrs = lappend(attrInHash->attrs, dupattr);
    }
    else if(RIPPLE_CATALOG_OP_UPDATE == catalogdata->op)
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
            pgattrInHash = (xk_pg_sysdict_Form_pg_attribute)lfirst(lc);
            if(pgattrInHash->attnum != pgattr->attnum)
            {
                continue;
            }

            /* 删除原来的，设置新的 */
            rmemcpy0(pgattrInHash, 0, pgattr, sizeof(xk_pg_parser_sysdict_pgattributes));
            return;
        }

        /* 没有匹配上,那么说明有问题，直接 append 上并WARNING */
        elog(RLOG_WARNING, "attribute %u, %s can not fond in attribute hash",
                            pgattr->attrelid,
                            pgattr->attname.data);

        /* 重新分配空间 */
        dupattr = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
        if(NULL == dupattr)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemcpy0(dupattr, 0, pgattr, sizeof(xk_pg_parser_sysdict_pgattributes));

        attrInHash->attrs = lappend(attrInHash->attrs, dupattr);
    }
    else if(RIPPLE_CATALOG_OP_DELETE == catalogdata->op)
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
            pgattrInHash = (xk_pg_sysdict_Form_pg_attribute)lfirst(lc);
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

void ripple_attributecache_write(HTAB* attributecache, uint64 *offset, ripple_sysdict_header_array* array)
{
	int	 fd;
	uint64 page_num = 0;
	uint64 page_offset = 0;
	ListCell*	cell = NULL;
	HASH_SEQ_STATUS status;
	List* ripple_attribute = NULL;
	char buffer[RIPPLE_FILE_BLK_SIZE];
	ripple_catalog_attribute_value *entry = NULL;
	xk_pg_sysdict_Form_pg_attribute attribute = NULL;

	array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].type = RIPPLE_CATALOG_TYPE_ATTRIBUTE;
	array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].offset = *offset;
	page_num = *offset;
	
	hash_seq_init(&status,attributecache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		foreach(cell, entry->attrs)
		{
			ripple_attribute = lappend(ripple_attribute, lfirst(cell));
		}
	}

	rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);
	fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
						O_RDWR | O_CREAT | RIPPLE_BINARY);

	if (fd < 0)
	{
		elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}
	foreach(cell, ripple_attribute)
	{
		attribute = (xk_pg_sysdict_Form_pg_attribute) lfirst(cell);

		if(page_offset + sizeof(xk_pg_parser_sysdict_pgattributes) > RIPPLE_FILE_BLK_SIZE)
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
		rmemcpy1(buffer, page_offset, attribute, sizeof(xk_pg_parser_sysdict_pgattributes));
		page_offset += sizeof(xk_pg_parser_sysdict_pgattributes);

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
	array[RIPPLE_CATALOG_TYPE_ATTRIBUTE - 1].len = page_num;
	list_free(ripple_attribute);
}

void ripple_attribute_catalogdatafree(ripple_catalogdata* catalogdata)
{
    ripple_catalog_attribute_value* catalog = NULL;
    if(NULL == catalogdata)
    {
        return;
    }

    if(NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    catalog = (ripple_catalog_attribute_value*)catalogdata->catalog;
    list_free_deep(catalog->attrs);

    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

/* 根据oid获取pg_attribute 数据 */
void* ripple_attribute_getbyoid(Oid oid, HTAB* by_attribute)
{
    bool found = false;
    ripple_catalog_attribute_value *attrentry = NULL;
    attrentry = hash_search(by_attribute, &oid, HASH_FIND, &found);
    if(false == found)
    {
        return NULL;
    }
    return (void*)attrentry->attrs;
}
