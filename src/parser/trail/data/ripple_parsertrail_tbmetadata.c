#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/data/ripple_parsertrail_tbmetadata.h"


/*
 * 根据 column->length/precision /scale 构建typmod
 *
 * typmod = -1 表示没有 typmod
 */
static int32 ripple_parsertrail_tbmetadata_gettypmod(ripple_ff_column* column)
{
    Oid typid               = InvalidOid;
    int32 typmod            = -1;
    int32 length            = -1;
    int32 precision         = -1;
    int32 scale             = -1;

    typid = column->typid;
    length = column->length;
    precision = column->precision;
    scale = column->scale;

    if (typid == XK_PG_SYSDICT_BPCHAROID ||
        typid == XK_PG_SYSDICT_VARCHAROID)
    {
        /* varchar(n), bpchar(n) */
        if (length > 0)
        {
            typmod = length + (int32_t) sizeof(int32_t);
        }
        else
        {
            typmod = -1;
        }
    }
    else if (typid == XK_PG_SYSDICT_TIMEOID ||
             typid == XK_PG_SYSDICT_TIMETZOID ||
             typid == XK_PG_SYSDICT_TIMESTAMPOID ||
             typid == XK_PG_SYSDICT_TIMESTAMPTZOID)
    {
        /* time(p), timetz(p), timestamp(p), timestamptz(p) */
        if (precision >= 0)
        {
            typmod = precision;
        }
        else
        {
            typmod = -1;
        }
    }
    else if (typid == XK_PG_SYSDICT_NUMERICOID)
    {
        /* numeric(p,s) */
        if (precision > 0 && scale >= 0)
        {
            typmod = ((precision << 16) | (scale & 0xffff))
                     + (int32_t) sizeof(int32_t);
        }
        else
        {
            typmod = -1;
        }
    }
    else if (typid == XK_PG_SYSDICT_BITOID ||
             typid == XK_PG_SYSDICT_VARBITOID)
    {
        /* bit(n), varbit(n) */
        if (length > 0)
        {
            typmod = length;
        }
        else
        {
            typmod = -1;
        }
    }
    else
    {
        typmod = -1;
    }

    return typmod;
}

/* 将表数据加入到事务缓存中 */
static bool ripple_parsertrail_tbmetadata2hash(ripple_parsertrail* parsertrail,
                                                ripple_ff_tbmetadata* fftbmd)
{
    /*
     * 存在则不处理，不存在则增加
     */
    bool found = false;
    uint16 index = 0;
    ripple_catalog_type_value *typeentry = NULL;
    ripple_catalog_class_value *classentry = NULL;
    ripple_catalog_index_hash_entry *indexentry = NULL;
    ripple_catalog_attribute_value* attrentry = NULL;
    xk_pg_sysdict_Form_pg_attribute attribute = NULL;
    ripple_catalog_index_value* index_value = NULL;

    HASHCTL hctl = { 0 };
    ripple_txn *cur_txn = parsertrail->lasttxn;
    bool add_txn = false;
    ripple_txnstmt *stmt = NULL;
    ripple_txnstmt_metadata *metadata = NULL;
    ListCell *sys_begin = NULL;
    ListCell *index_cell = NULL;

    /*
     * 在 hash 表中查看是否含有，不含有则加入
     */
    if(NULL == parsertrail->transcache->sysdicts->by_class)
    {
        /* 创建 hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(ripple_catalog_class_value);
        parsertrail->transcache->sysdicts->by_class = hash_create("decodehclass",
                                                        1024,
                                                        &hctl,
                                                        HASH_ELEM | HASH_BLOBS);
    }

    classentry = hash_search(parsertrail->transcache->sysdicts->by_class, &fftbmd->oid, HASH_ENTER, &found);
    if(false == found)
    {
        /* 添加 */
        classentry->oid = fftbmd->oid;
        classentry->ripple_class = (xk_pg_sysdict_Form_pg_class)rmalloc0(sizeof(xk_pg_parser_sysdict_pgclass));
        if(NULL == classentry->ripple_class)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemset0(classentry->ripple_class, 0, '\0', sizeof(xk_pg_parser_sysdict_pgclass));
        classentry->ripple_class->oid = fftbmd->oid;
    }

    /* 加入到 entry 中 */
    rmemset1(classentry->ripple_class->nspname.data, 0, '\0', RIPPLE_NAMEDATALEN);
    rmemset1(classentry->ripple_class->relname.data, 0, '\0', RIPPLE_NAMEDATALEN);
    rmemcpy1(classentry->ripple_class->nspname.data, 0, fftbmd->schema, strlen(fftbmd->schema));
    rmemcpy1(classentry->ripple_class->relname.data, 0, fftbmd->table, strlen(fftbmd->table));

    /* 表信息 */
    if(NULL == parsertrail->transcache->sysdicts->by_attribute)
    {
        /* 创建 hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(ripple_catalog_attribute_value);
        parsertrail->transcache->sysdicts->by_attribute = hash_create("decodehclassattr",
                                                            2048,
                                                            &hctl,
                                                            HASH_ELEM | HASH_BLOBS);
    }

    /* 类型信息 */
    if(NULL == parsertrail->transcache->sysdicts->by_type)
    {
        /* 创建 hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(ripple_catalog_type_value);
        parsertrail->transcache->sysdicts->by_type = hash_create("decodehtype",
                                                                 2048,
                                                                 &hctl,
                                                                 HASH_ELEM | HASH_BLOBS);
    }

    /* 向 by_attribute 增加数据 */
    attrentry = hash_search(parsertrail->transcache->sysdicts->by_attribute, &fftbmd->oid, HASH_ENTER, &found);
    if(false == found)
    {
        attrentry->attrelid = fftbmd->oid;
    }
    else
    {
        /* 重新写入 */
        list_free_deep(attrentry->attrs);
    }
    attrentry->attrs = NULL;

    /* 加入到hash表中 */
    for(index = 0; index < fftbmd->colcnt; )
    {
        attribute = (xk_pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(xk_pg_parser_sysdict_pgattributes));
        if(NULL == attribute)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemset0(attribute, 0, '\0', sizeof(xk_pg_parser_sysdict_pgattributes));
        attribute->attrelid = fftbmd->oid;
        attribute->atttypid = fftbmd->columns[index].typid;
        rmemcpy1(attribute->attname.data, 0, fftbmd->columns[index].column, strlen(fftbmd->columns[index].column));
        /* 向 by_type 增加数据 */
        typeentry = hash_search(parsertrail->transcache->sysdicts->by_type, &attribute->atttypid, HASH_ENTER, &found);
        if (false == found)
        {
            typeentry->oid = attribute->atttypid;
            typeentry->ripple_type = (xk_pg_sysdict_Form_pg_type)rmalloc0(sizeof(xk_pg_parser_sysdict_pgtype));
            if(NULL == typeentry->ripple_type)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(typeentry->ripple_type, 0, '\0', sizeof(xk_pg_parser_sysdict_pgtype));
            typeentry->ripple_type->oid = attribute->atttypid;
            rmemcpy1(typeentry->ripple_type->typname.data, 0, fftbmd->columns[index].typename, strlen(fftbmd->columns[index].typename));
        }
        attribute->atttypmod = ripple_parsertrail_tbmetadata_gettypmod(&fftbmd->columns[index]);
        attribute->attnum = ++index;
        attrentry->attrs = lappend(attrentry->attrs, attribute);
    }

    /*
    * 在 hash 表中查看是否含有index, 不含有则加入
    */
    if(NULL == parsertrail->transcache->sysdicts->by_index)
    {
        /* 创建 hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(ripple_catalog_index_hash_entry);
        parsertrail->transcache->sysdicts->by_index = hash_create("decodehindex",
                                                        1024,
                                                        &hctl,
                                                        HASH_ELEM | HASH_BLOBS);
    }

    /* 向 by_index 增加数据 */
    indexentry = hash_search(parsertrail->transcache->sysdicts->by_index, &fftbmd->oid, HASH_ENTER, &found);
    if(false == found)
    {
        /* 添加 */
        indexentry->oid = fftbmd->oid;
        indexentry->ripple_index_list = NULL;
    }
    else
    {
        if(NULL != indexentry->ripple_index_list)
        {
            foreach(index_cell, indexentry->ripple_index_list)
            {
                index_value = (ripple_catalog_index_value*)lfirst(index_cell);
                if (index_value->ripple_index)
                {
                    if (index_value->ripple_index->indkey)
                    {
                        rfree(index_value->ripple_index->indkey);
                    }
                    rfree(index_value->ripple_index);
                }
                rfree(index_value);
            }
            list_free(indexentry->ripple_index_list);
        }
        index_cell = NULL;
        indexentry->oid = fftbmd->oid;
        indexentry->ripple_index_list = NULL;
    }

    if (fftbmd->index)
    {
        foreach(index_cell, fftbmd->index)
        {
            bool isprimary = false;
            ripple_ff_tbindex *metaindex = (ripple_ff_tbindex *) lfirst(index_cell);

            switch (metaindex->index_type)
            {
                case RIPPLE_FF_TBINDEX_TYPE_PKEY:
                {
                    isprimary = true;
                }
                case RIPPLE_FF_TBINDEX_TYPE_UNIQUE:
                {
                    ripple_catalog_index_value *indexvalue = NULL;
                    xk_pg_sysdict_Form_pg_index indexcatalog = NULL;

                    indexvalue = rmalloc0(sizeof(ripple_catalog_index_value));
                    if (!indexvalue)
                    {
                        elog(RLOG_WARNING, "oom");
                        return false;
                    }
                    rmemset0(indexvalue, 0, 0, sizeof(ripple_catalog_index_value));

                    indexcatalog = rmalloc0(sizeof(xk_pg_parser_sysdict_pgindex));
                    if (!indexcatalog)
                    {
                        elog(RLOG_WARNING, "oom");
                        return false;
                    }
                    rmemset0(indexcatalog, 0, 0, sizeof(xk_pg_parser_sysdict_pgindex));

                    indexcatalog->indkey = rmalloc0(sizeof(uint32) * metaindex->index_key_num);
                    if (!indexcatalog->indkey)
                    {
                        elog(RLOG_WARNING, "oom");
                        return false;
                    }
                    rmemset0(indexcatalog->indkey, 0, 0, sizeof(uint32) * metaindex->index_key_num);

                    indexcatalog->indrelid = fftbmd->oid;
                    indexcatalog->indexrelid = metaindex->index_oid;
                    indexcatalog->indisprimary = isprimary;
                    indexcatalog->indisreplident = metaindex->index_identify;
                    indexcatalog->indnatts = metaindex->index_key_num;
                    rmemcpy0(indexcatalog->indkey, 0, metaindex->index_key, sizeof(uint32) * metaindex->index_key_num);

                    indexvalue->oid = indexcatalog->indrelid;
                    indexvalue->ripple_index = indexcatalog;

                    indexentry->ripple_index_list = lappend(indexentry->ripple_index_list, indexvalue);
                    break;
                }
                default:
                {
                    elog(RLOG_WARNING, "unknown tbindex type: %d", metaindex->index_type);
                    break;
                }
            }
        }
    }

    /* 判断是否在一个事务内, 如果不在事务内, 为其分配一个新事物 */
    if (cur_txn == NULL)
    {
        /* 不在事务内, 创建一个新的txn */
        add_txn = true;
        cur_txn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
        cur_txn->type = RIPPLE_TXN_TYPE_METADATA;
    }

    /* 拼装sysdictHis */
    if (fftbmd)
    {
        int index_attrs = 0;
        /* pg_class */
        ripple_catalogdata *catalog_class = rmalloc0(sizeof(ripple_catalogdata));
        ripple_catalog_class_value *class_value = rmalloc0(sizeof(ripple_catalog_class_value));

        /* pg_attribute */
        rmemset0(catalog_class, 0, 0, sizeof(ripple_catalogdata));
        rmemset0(class_value, 0, 0, sizeof(ripple_catalog_class_value));

        /* class catalogdata赋值 */
        catalog_class->op = RIPPLE_CATALOG_OP_INSERT;
        catalog_class->type = RIPPLE_CATALOG_TYPE_CLASS;

        /* class_value赋值 */
        class_value->ripple_class = rmalloc0(sizeof(xk_pg_parser_sysdict_pgclass));
        rmemset0(class_value->ripple_class, 0, 0, sizeof(xk_pg_parser_sysdict_pgclass));
        class_value->oid = fftbmd->oid;

        /* 
         * class结构体赋值
         * 
         * oid
         * relnatts
         * relname
         * nspname
         */
        class_value->ripple_class->oid = fftbmd->oid;
        class_value->ripple_class->relnatts = fftbmd->colcnt;
        strcpy(class_value->ripple_class->relname.data, fftbmd->table);
        strcpy(class_value->ripple_class->nspname.data, fftbmd->schema);

        catalog_class->catalog = (void *) class_value;

        /* class 组装完毕, 附加到链表中 */
        cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_class);
        sys_begin = list_tail(cur_txn->sysdictHis);

        /* attribute_value 赋值 */

        /* 遍历所有列 */
        for (index_attrs = 0; index_attrs < fftbmd->colcnt; index_attrs++)
        {
            ripple_catalogdata *catalog_type = NULL;
            ripple_catalog_type_value *type_value = NULL;
            xk_pg_sysdict_Form_pg_attribute attr = NULL;
            ripple_catalogdata *catalog_attribute = NULL;
            ripple_catalog_attribute_value *attribute_value = NULL;

            catalog_attribute = rmalloc0(sizeof(ripple_catalogdata));
            if(NULL == catalog_attribute)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(catalog_attribute, 0, 0, sizeof(ripple_catalogdata));

            /* attribute catalogdata赋值 */
            catalog_attribute->op = RIPPLE_CATALOG_OP_INSERT;
            catalog_attribute->type = RIPPLE_CATALOG_TYPE_ATTRIBUTE;

            attribute_value = rmalloc0(sizeof(ripple_catalog_attribute_value));
            if(NULL == attribute_value)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(attribute_value, 0, 0, sizeof(ripple_catalog_attribute_value));
            attribute_value->attrelid = fftbmd->oid;

            attr = rmalloc0(sizeof(xk_pg_parser_sysdict_pgattributes));
            if(NULL == attr)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(attr, 0, 0, sizeof(xk_pg_parser_sysdict_pgattributes));

            /* 
             * attribute结构体赋值
             * 
             * attrelid
             * attnum
             * atttypid
             * attname
             */
            attr->attrelid = fftbmd->oid;
            attr->attnum = fftbmd->columns[index_attrs].num;
            attr->atttypid = fftbmd->columns[index_attrs].typid;
            strcpy(attr->attname.data, fftbmd->columns[index_attrs].column);
            attr->atttypmod = ripple_parsertrail_tbmetadata_gettypmod(&fftbmd->columns[index_attrs]);

            /* attribute_value赋值 */
            attribute_value->attrs = lappend(attribute_value->attrs, attr);
            catalog_attribute->catalog = (void *) attribute_value;

            /* attribute组装完毕, 附加到链表中 */
            cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_attribute);

            /* 组装type */
            catalog_type = rmalloc0(sizeof(ripple_catalogdata));
            if(NULL == catalog_type)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(catalog_type, 0, 0, sizeof(ripple_catalogdata));

            /* type catalogdata赋值 */
            catalog_type->op = RIPPLE_CATALOG_OP_INSERT;
            catalog_type->type = RIPPLE_CATALOG_TYPE_TYPE;

            type_value = rmalloc0(sizeof(ripple_catalog_type_value));
            if(NULL == type_value)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(type_value, 0, 0, sizeof(ripple_catalog_type_value));
            type_value->oid = fftbmd->columns[index_attrs].typid;

            type_value->ripple_type = rmalloc0(sizeof(xk_pg_parser_sysdict_pgtype));
            if(NULL == type_value->ripple_type)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(type_value->ripple_type, 0, 0, sizeof(xk_pg_parser_sysdict_pgtype));
            type_value->ripple_type->oid = fftbmd->columns[index_attrs].typid;
            rmemcpy1(type_value->ripple_type->typname.data, 0, fftbmd->columns[index_attrs].typename, strlen(fftbmd->columns[index_attrs].typename));
            catalog_type->catalog = (void *) type_value;

            /* type 组装完毕, 附加到链表中 */
            cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_type);
        }

        if (fftbmd->index)
        {
            foreach(index_cell, fftbmd->index)
            {
                bool isprimary = false;
                ripple_ff_tbindex *metaindex = (ripple_ff_tbindex *) lfirst(index_cell);
                ripple_catalogdata *catalogdata_index = NULL;

                catalogdata_index = rmalloc0(sizeof(ripple_catalogdata));
                if(NULL == catalogdata_index)
                {
                    elog(RLOG_WARNING, "out of memory");
                    return false;
                }
                rmemset0(catalogdata_index, 0, 0, sizeof(ripple_catalogdata));

                /* index catalogdata赋值 */
                catalogdata_index->op = RIPPLE_CATALOG_OP_INSERT;
                catalogdata_index->type = RIPPLE_CATALOG_TYPE_INDEX;

                switch (metaindex->index_type)
                {
                    case RIPPLE_FF_TBINDEX_TYPE_PKEY:
                    {
                        isprimary = true;
                    }
                    case RIPPLE_FF_TBINDEX_TYPE_UNIQUE:
                    {
                        ripple_catalog_index_value *indexvalue = NULL;
                        xk_pg_sysdict_Form_pg_index indexcatalog = NULL;

                        indexvalue = rmalloc0(sizeof(ripple_catalog_index_value));
                        if (!indexvalue)
                        {
                            elog(RLOG_WARNING, "oom");
                            return false;
                        }
                        rmemset0(indexvalue, 0, 0, sizeof(ripple_catalog_index_value));

                        indexcatalog = rmalloc0(sizeof(xk_pg_parser_sysdict_pgindex));
                        if (!indexcatalog)
                        {
                            elog(RLOG_WARNING, "oom");
                            return false;
                        }
                        rmemset0(indexcatalog, 0, 0, sizeof(xk_pg_parser_sysdict_pgindex));

                        indexcatalog->indkey = rmalloc0(sizeof(uint32) * metaindex->index_key_num);
                        if (!indexcatalog->indkey)
                        {
                            elog(RLOG_WARNING, "oom");
                            return false;
                        }
                        rmemset0(indexcatalog->indkey, 0, 0, sizeof(uint32) * metaindex->index_key_num);

                        indexcatalog->indrelid = fftbmd->oid;
                        indexcatalog->indexrelid = metaindex->index_oid;
                        indexcatalog->indisprimary = isprimary;
                        indexcatalog->indisreplident = metaindex->index_identify;
                        indexcatalog->indnatts = metaindex->index_key_num;
                        rmemcpy0(indexcatalog->indkey, 0, metaindex->index_key, sizeof(uint32) * metaindex->index_key_num);

                        indexvalue->oid = indexcatalog->indrelid;
                        indexvalue->ripple_index = indexcatalog;

                        catalogdata_index->catalog = indexvalue;

                        cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalogdata_index);
                        break;
                    }
                    default:
                    {
                        elog(RLOG_WARNING, "unknown tbindex type: %d", metaindex->index_type);
                        break;
                    }
                }
            }
        }
    }

    /* 创建METADATA stmt */
    /* 添加stmt, 作为系统表段标识 */
    stmt = rmalloc0(sizeof(ripple_txnstmt));
    metadata = rmalloc0(sizeof(ripple_txnstmt_metadata));

    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    rmemset0(metadata, 0, 0, sizeof(ripple_txnstmt_metadata));

    stmt->type = RIPPLE_TXNSTMT_TYPE_METADATA;

    metadata->begin = sys_begin;
    metadata->end = list_tail(cur_txn->sysdictHis);
    stmt->stmt = (void *)metadata;
    cur_txn->stmts = lappend(cur_txn->stmts, stmt);

    /* 如果是创建的新事物, 在这里将事务添加到缓存中 */
    if (add_txn)
    {
        parsertrail->dtxns = dlist_put(parsertrail->dtxns, cur_txn);
        if(NULL == parsertrail->dtxns)
        {
            elog(RLOG_WARNING, "parser trail table metadata add txn to dlist error");
            return false;
        }
        parsertrail->lasttxn = NULL;
    }
    return true;
}

/*
 * 数据库信息应用
 *  此处为独立的处理逻辑，函数流程到此说明前期的处理已经完成，在这只需要应用即可
*/
bool ripple_parsertrail_tbmetadataapply(ripple_parsertrail* parsertrail, void* data)
{
    bool found = false;
    uint16 index = 0;
    ripple_fftrail_table_deserialkey tbkey = { 0 };
    ripple_ff_tbmetadata* fftbmd = NULL;
    ripple_fftrail_privdata* privdata = NULL;
    ripple_fftrail_table_deserialentry* deserialentry = NULL;

    if(NULL == data)
    {
        return true;
    }

    fftbmd = (ripple_ff_tbmetadata*)data;
    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    /* 像缓存中插入数据 */
    if(false == ripple_parsertrail_tbmetadata2hash(parsertrail, fftbmd))
    {
        elog(RLOG_WARNING, "parser trail table metadata error");
        return false;
    }

    /* 将表放入到 hash 中 */
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;
    tbkey.tbnum = fftbmd->tbmdno;
    deserialentry = hash_search(privdata->tables, &tbkey, HASH_ENTER, &found);
    if(false == found)
    {
        deserialentry->key.tbnum = tbkey.tbnum;
        deserialentry->oid = fftbmd->oid;

        privdata->tbnum++;
        privdata->tbentrys = lappend(privdata->tbentrys, deserialentry);
    }
    else
    {
        /* 找到了，那么替换掉 */
        if(NULL != deserialentry->columns)
        {
            rfree(deserialentry->columns);
        }
    }

    deserialentry->dbno = fftbmd->header.dbmdno;
    rmemset1(deserialentry->schema, 0, '\0', sizeof(deserialentry->schema));
    rmemcpy1(deserialentry->schema, 0, fftbmd->schema, strlen(fftbmd->schema));
    rmemset1(deserialentry->table, 0, '\0', sizeof(deserialentry->table));
    rmemcpy1(deserialentry->table, 0, fftbmd->table, strlen(fftbmd->table));
    deserialentry->colcnt = fftbmd->colcnt;
    deserialentry->columns = fftbmd->columns;
    fftbmd->columns = NULL;

    /* 打印内容 */
    /* 日志级别为 debug */
    if(RLOG_DEBUG == g_loglevel)
    {
        /* 输出调试日志 */
        elog(RLOG_DEBUG, "----------Trail TableMeta Begin----------------");
        elog(RLOG_DEBUG, "dbmdno:           %u",    fftbmd->header.dbmdno);
        elog(RLOG_DEBUG, "tbmdno:           %u",    fftbmd->tbmdno);
        elog(RLOG_DEBUG, "oid:              %u",    fftbmd->oid);
        elog(RLOG_DEBUG, "schema:           %s",    fftbmd->schema);
        elog(RLOG_DEBUG, "table:            %s",    fftbmd->table);
        elog(RLOG_DEBUG, "flag:             %u",    fftbmd->flag);
        elog(RLOG_DEBUG, "colcnt:           %u",    fftbmd->colcnt);
        elog(RLOG_DEBUG, "indexcnt:         %u",    fftbmd->index ? fftbmd->index->length : 0);
        elog(RLOG_DEBUG, "----table columns begin----");
        for(index = 0; index < fftbmd->colcnt; index++)
        {
            elog(RLOG_DEBUG, "  colid:      %u",    deserialentry->columns[index].num);
            elog(RLOG_DEBUG, "  flag:       %u",    deserialentry->columns[index].flag);
            elog(RLOG_DEBUG, "  type:       %u",    deserialentry->columns[index].typid);
            elog(RLOG_DEBUG, "  column:     %s",    deserialentry->columns[index].column);
        }
        elog(RLOG_DEBUG, "----table columns   end----");

        elog(RLOG_DEBUG, "----------Trail TableMeta   end----------------");
    }
    return true;
}

/*
 * 数据库信息清理
 */
void ripple_parsertrail_tbmetadataclean(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_tbmetadata* fftbmd = NULL;

    if(NULL == data)
    {
        return;
    }

    fftbmd = (ripple_ff_tbmetadata*)data;

    /* 内存释放 */
    ripple_ff_tbmetadata_free(fftbmd);
}
