#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "stmts/txnstmt.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_tbmetadata.h"

/*
 * Build typmod based on column->length/precision/scale
 *
 * typmod = -1 means no typmod
 */
static int32 parsertrail_tbmetadata_gettypmod(ff_column* column)
{
    Oid   typid = INVALIDOID;
    int32 typmod = -1;
    int32 length = -1;
    int32 precision = -1;
    int32 scale = -1;

    typid = column->typid;
    length = column->length;
    precision = column->precision;
    scale = column->scale;

    if (typid == PG_SYSDICT_BPCHAROID || typid == PG_SYSDICT_VARCHAROID)
    {
        /* varchar(n), bpchar(n) */
        if (length > 0)
        {
            typmod = length + (int32_t)sizeof(int32_t);
        }
        else
        {
            typmod = -1;
        }
    }
    else if (typid == PG_SYSDICT_TIMEOID || typid == PG_SYSDICT_TIMETZOID ||
             typid == PG_SYSDICT_TIMESTAMPOID || typid == PG_SYSDICT_TIMESTAMPTZOID)
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
    else if (typid == PG_SYSDICT_NUMERICOID)
    {
        /* numeric(p,s) */
        if (precision > 0 && scale >= 0)
        {
            typmod = ((precision << 16) | (scale & 0xffff)) + (int32_t)sizeof(int32_t);
        }
        else
        {
            typmod = -1;
        }
    }
    else if (typid == PG_SYSDICT_BITOID || typid == PG_SYSDICT_VARBITOID)
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

/* Add table data to transaction cache */
static bool parsertrail_tbmetadata2hash(parsertrail* parsertrail, ff_tbmetadata* fftbmd)
{
    /*
     * Skip if exists, add if not found
     */
    bool                         found = false;
    uint16                       index = 0;
    catalog_type_value*          typeentry = NULL;
    catalog_class_value*         classentry = NULL;
    catalog_index_hash_entry*    indexentry = NULL;
    catalog_attribute_value*     attrentry = NULL;
    pg_sysdict_Form_pg_attribute attribute = NULL;
    catalog_index_value*         index_value = NULL;

    HASHCTL                      hctl = {0};
    txn*                         cur_txn = parsertrail->lasttxn;
    bool                         add_txn = false;
    txnstmt*                     stmt = NULL;
    txnstmt_metadata*            metadata = NULL;
    ListCell*                    sys_begin = NULL;
    ListCell*                    index_cell = NULL;

    /*
     * Check if exists in hash table, add if not found
     */
    if (NULL == parsertrail->transcache->sysdicts->by_class)
    {
        /* Create hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(catalog_class_value);
        parsertrail->transcache->sysdicts->by_class =
            hash_create("decodehclass", 1024, &hctl, HASH_ELEM | HASH_BLOBS);
    }

    classentry =
        hash_search(parsertrail->transcache->sysdicts->by_class, &fftbmd->oid, HASH_ENTER, &found);
    if (false == found)
    {
        /* Add */
        classentry->oid = fftbmd->oid;
        classentry->class = (pg_sysdict_Form_pg_class)rmalloc0(sizeof(pg_parser_sysdict_pgclass));
        if (NULL == classentry->class)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemset0(classentry->class, 0, '\0', sizeof(pg_parser_sysdict_pgclass));
        classentry->class->oid = fftbmd->oid;
    }

    /* Add to entry */
    rmemset1(classentry->class->nspname.data, 0, '\0', NAMEDATALEN);
    rmemset1(classentry->class->relname.data, 0, '\0', NAMEDATALEN);
    rmemcpy1(classentry->class->nspname.data, 0, fftbmd->schema, strlen(fftbmd->schema));
    rmemcpy1(classentry->class->relname.data, 0, fftbmd->table, strlen(fftbmd->table));

    /* Table info */
    if (NULL == parsertrail->transcache->sysdicts->by_attribute)
    {
        /* Create hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(catalog_attribute_value);
        parsertrail->transcache->sysdicts->by_attribute =
            hash_create("decodehclassattr", 2048, &hctl, HASH_ELEM | HASH_BLOBS);
    }

    /* Type info */
    if (NULL == parsertrail->transcache->sysdicts->by_type)
    {
        /* Create hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(catalog_type_value);
        parsertrail->transcache->sysdicts->by_type =
            hash_create("decodehtype", 2048, &hctl, HASH_ELEM | HASH_BLOBS);
    }

    /* Add data to by_attribute */
    attrentry = hash_search(
        parsertrail->transcache->sysdicts->by_attribute, &fftbmd->oid, HASH_ENTER, &found);
    if (false == found)
    {
        attrentry->attrelid = fftbmd->oid;
    }
    else
    {
        /* Overwrite */
        list_free_deep(attrentry->attrs);
    }
    attrentry->attrs = NULL;

    /* Add to hash table */
    for (index = 0; index < fftbmd->colcnt;)
    {
        attribute = (pg_sysdict_Form_pg_attribute)rmalloc1(sizeof(pg_parser_sysdict_pgattributes));
        if (NULL == attribute)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemset0(attribute, 0, '\0', sizeof(pg_parser_sysdict_pgattributes));
        attribute->attrelid = fftbmd->oid;
        attribute->atttypid = fftbmd->columns[index].typid;
        rmemcpy1(attribute->attname.data,
                 0,
                 fftbmd->columns[index].column,
                 strlen(fftbmd->columns[index].column));
        /* Add data to by_type */
        typeentry = hash_search(
            parsertrail->transcache->sysdicts->by_type, &attribute->atttypid, HASH_ENTER, &found);
        if (false == found)
        {
            typeentry->oid = attribute->atttypid;
            typeentry->type = (pg_sysdict_Form_pg_type)rmalloc0(sizeof(pg_parser_sysdict_pgtype));
            if (NULL == typeentry->type)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(typeentry->type, 0, '\0', sizeof(pg_parser_sysdict_pgtype));
            typeentry->type->oid = attribute->atttypid;
            rmemcpy1(typeentry->type->typname.data,
                     0,
                     fftbmd->columns[index].typename,
                     strlen(fftbmd->columns[index].typename));
        }
        attribute->atttypmod = parsertrail_tbmetadata_gettypmod(&fftbmd->columns[index]);
        attribute->attnum = ++index;
        attrentry->attrs = lappend(attrentry->attrs, attribute);
    }

    /*
     * Check if index exists in hash table, add if not found
     */
    if (NULL == parsertrail->transcache->sysdicts->by_index)
    {
        /* Create hash */
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(catalog_index_hash_entry);
        parsertrail->transcache->sysdicts->by_index =
            hash_create("decodehindex", 1024, &hctl, HASH_ELEM | HASH_BLOBS);
    }

    /* Add data to by_index */
    indexentry =
        hash_search(parsertrail->transcache->sysdicts->by_index, &fftbmd->oid, HASH_ENTER, &found);
    if (false == found)
    {
        /* Add */
        indexentry->oid = fftbmd->oid;
        indexentry->index_list = NULL;
    }
    else
    {
        if (NULL != indexentry->index_list)
        {
            foreach (index_cell, indexentry->index_list)
            {
                index_value = (catalog_index_value*)lfirst(index_cell);
                if (index_value->index)
                {
                    if (index_value->index->indkey)
                    {
                        rfree(index_value->index->indkey);
                    }
                    rfree(index_value->index);
                }
                rfree(index_value);
            }
            list_free(indexentry->index_list);
        }
        index_cell = NULL;
        indexentry->oid = fftbmd->oid;
        indexentry->index_list = NULL;
    }

    if (fftbmd->index)
    {
        foreach (index_cell, fftbmd->index)
        {
            bool        isprimary = false;
            ff_tbindex* metaindex = (ff_tbindex*)lfirst(index_cell);

            switch (metaindex->index_type)
            {
                case FF_TBINDEX_TYPE_PKEY:
                {
                    isprimary = true;
                }
                case FF_TBINDEX_TYPE_UNIQUE:
                {
                    catalog_index_value*     indexvalue = NULL;
                    pg_sysdict_Form_pg_index indexcatalog = NULL;

                    indexvalue = rmalloc0(sizeof(catalog_index_value));
                    if (!indexvalue)
                    {
                        elog(RLOG_WARNING, "oom");
                        return false;
                    }
                    rmemset0(indexvalue, 0, 0, sizeof(catalog_index_value));

                    indexcatalog = rmalloc0(sizeof(pg_parser_sysdict_pgindex));
                    if (!indexcatalog)
                    {
                        elog(RLOG_WARNING, "oom");
                        return false;
                    }
                    rmemset0(indexcatalog, 0, 0, sizeof(pg_parser_sysdict_pgindex));

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
                    rmemcpy0(indexcatalog->indkey,
                             0,
                             metaindex->index_key,
                             sizeof(uint32) * metaindex->index_key_num);

                    indexvalue->oid = indexcatalog->indrelid;
                    indexvalue->index = indexcatalog;

                    indexentry->index_list = lappend(indexentry->index_list, indexvalue);
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

    /* Check if within a transaction, if not allocate a new transaction */
    if (cur_txn == NULL)
    {
        /* Not in transaction, create a new txn */
        add_txn = true;
        cur_txn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
        cur_txn->type = TXN_TYPE_METADATA;
    }

    /* Assemble sysdictHis */
    if (fftbmd)
    {
        int                  index_attrs = 0;
        /* pg_class */
        catalogdata*         catalog_class = rmalloc0(sizeof(catalogdata));
        catalog_class_value* class_value = rmalloc0(sizeof(catalog_class_value));

        /* pg_attribute */
        rmemset0(catalog_class, 0, 0, sizeof(catalogdata));
        rmemset0(class_value, 0, 0, sizeof(catalog_class_value));

        /* class catalogdata assignment */
        catalog_class->op = CATALOG_OP_INSERT;
        catalog_class->type = CATALOG_TYPE_CLASS;

        /* class_value assignment */
        class_value->class = rmalloc0(sizeof(pg_parser_sysdict_pgclass));
        rmemset0(class_value->class, 0, 0, sizeof(pg_parser_sysdict_pgclass));
        class_value->oid = fftbmd->oid;

        /*
         * class structure assignment
         *
         * oid
         * relnatts
         * relname
         * nspname
         */
        class_value->class->oid = fftbmd->oid;
        class_value->class->relnatts = fftbmd->colcnt;
        strcpy(class_value->class->relname.data, fftbmd->table);
        strcpy(class_value->class->nspname.data, fftbmd->schema);

        catalog_class->catalog = (void*)class_value;

        /* class assembly complete, append to list */
        cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_class);
        sys_begin = list_tail(cur_txn->sysdictHis);

        /* attribute_value assignment */

        /* Iterate all columns */
        for (index_attrs = 0; index_attrs < fftbmd->colcnt; index_attrs++)
        {
            catalogdata*                 catalog_type = NULL;
            catalog_type_value*          type_value = NULL;
            pg_sysdict_Form_pg_attribute attr = NULL;
            catalogdata*                 catalog_attribute = NULL;
            catalog_attribute_value*     attribute_value = NULL;

            catalog_attribute = rmalloc0(sizeof(catalogdata));
            if (NULL == catalog_attribute)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(catalog_attribute, 0, 0, sizeof(catalogdata));

            /* attribute catalogdata assignment */
            catalog_attribute->op = CATALOG_OP_INSERT;
            catalog_attribute->type = CATALOG_TYPE_ATTRIBUTE;

            attribute_value = rmalloc0(sizeof(catalog_attribute_value));
            if (NULL == attribute_value)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(attribute_value, 0, 0, sizeof(catalog_attribute_value));
            attribute_value->attrelid = fftbmd->oid;

            attr = rmalloc0(sizeof(pg_parser_sysdict_pgattributes));
            if (NULL == attr)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(attr, 0, 0, sizeof(pg_parser_sysdict_pgattributes));

            /*
             * attribute structure assignment
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
            attr->atttypmod = parsertrail_tbmetadata_gettypmod(&fftbmd->columns[index_attrs]);

            /* attribute_value assignment */
            attribute_value->attrs = lappend(attribute_value->attrs, attr);
            catalog_attribute->catalog = (void*)attribute_value;

            /* attribute assembly complete, append to list */
            cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_attribute);

            /* Assemble type */
            catalog_type = rmalloc0(sizeof(catalogdata));
            if (NULL == catalog_type)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(catalog_type, 0, 0, sizeof(catalogdata));

            /* type catalogdata assignment */
            catalog_type->op = CATALOG_OP_INSERT;
            catalog_type->type = CATALOG_TYPE_TYPE;

            type_value = rmalloc0(sizeof(catalog_type_value));
            if (NULL == type_value)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(type_value, 0, 0, sizeof(catalog_type_value));
            type_value->oid = fftbmd->columns[index_attrs].typid;

            type_value->type = rmalloc0(sizeof(pg_parser_sysdict_pgtype));
            if (NULL == type_value->type)
            {
                elog(RLOG_WARNING, "out of memory");
                return false;
            }
            rmemset0(type_value->type, 0, 0, sizeof(pg_parser_sysdict_pgtype));
            type_value->type->oid = fftbmd->columns[index_attrs].typid;
            rmemcpy1(type_value->type->typname.data,
                     0,
                     fftbmd->columns[index_attrs].typename,
                     strlen(fftbmd->columns[index_attrs].typename));
            catalog_type->catalog = (void*)type_value;

            /* type assembly complete, append to list */
            cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_type);
        }

        if (fftbmd->index)
        {
            foreach (index_cell, fftbmd->index)
            {
                bool         isprimary = false;
                ff_tbindex*  metaindex = (ff_tbindex*)lfirst(index_cell);
                catalogdata* catalogdata_index = NULL;

                catalogdata_index = rmalloc0(sizeof(catalogdata));
                if (NULL == catalogdata_index)
                {
                    elog(RLOG_WARNING, "out of memory");
                    return false;
                }
                rmemset0(catalogdata_index, 0, 0, sizeof(catalogdata));

                /* index catalogdata assignment */
                catalogdata_index->op = CATALOG_OP_INSERT;
                catalogdata_index->type = CATALOG_TYPE_INDEX;

                switch (metaindex->index_type)
                {
                    case FF_TBINDEX_TYPE_PKEY:
                    {
                        isprimary = true;
                    }
                    case FF_TBINDEX_TYPE_UNIQUE:
                    {
                        catalog_index_value*     indexvalue = NULL;
                        pg_sysdict_Form_pg_index indexcatalog = NULL;

                        indexvalue = rmalloc0(sizeof(catalog_index_value));
                        if (!indexvalue)
                        {
                            elog(RLOG_WARNING, "oom");
                            return false;
                        }
                        rmemset0(indexvalue, 0, 0, sizeof(catalog_index_value));

                        indexcatalog = rmalloc0(sizeof(pg_parser_sysdict_pgindex));
                        if (!indexcatalog)
                        {
                            elog(RLOG_WARNING, "oom");
                            return false;
                        }
                        rmemset0(indexcatalog, 0, 0, sizeof(pg_parser_sysdict_pgindex));

                        indexcatalog->indkey = rmalloc0(sizeof(uint32) * metaindex->index_key_num);
                        if (!indexcatalog->indkey)
                        {
                            elog(RLOG_WARNING, "oom");
                            return false;
                        }
                        rmemset0(
                            indexcatalog->indkey, 0, 0, sizeof(uint32) * metaindex->index_key_num);

                        indexcatalog->indrelid = fftbmd->oid;
                        indexcatalog->indexrelid = metaindex->index_oid;
                        indexcatalog->indisprimary = isprimary;
                        indexcatalog->indisreplident = metaindex->index_identify;
                        indexcatalog->indnatts = metaindex->index_key_num;
                        rmemcpy0(indexcatalog->indkey,
                                 0,
                                 metaindex->index_key,
                                 sizeof(uint32) * metaindex->index_key_num);

                        indexvalue->oid = indexcatalog->indrelid;
                        indexvalue->index = indexcatalog;

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

    /* Create METADATA stmt */
    /* Add stmt as system table segment identifier */
    stmt = rmalloc0(sizeof(txnstmt));
    metadata = rmalloc0(sizeof(txnstmt_metadata));

    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    rmemset0(metadata, 0, 0, sizeof(txnstmt_metadata));

    stmt->type = TXNSTMT_TYPE_METADATA;

    metadata->begin = sys_begin;
    metadata->end = list_tail(cur_txn->sysdictHis);
    stmt->stmt = (void*)metadata;
    cur_txn->stmts = lappend(cur_txn->stmts, stmt);

    /* If created new transaction, add transaction to cache here */
    if (add_txn)
    {
        parsertrail->dtxns = dlist_put(parsertrail->dtxns, cur_txn);
        if (NULL == parsertrail->dtxns)
        {
            elog(RLOG_WARNING, "parser trail table metadata add txn to dlist error");
            return false;
        }
        parsertrail->lasttxn = NULL;
    }
    return true;
}

/*
 * Database info apply
 * This is independent processing logic, flow reaching here means preprocessing is complete, just
 * need to apply
 */
bool parsertrail_tbmetadataapply(parsertrail* parsertrail, void* data)
{
    bool                         found = false;
    uint16                       index = 0;
    fftrail_table_deserialkey    tbkey = {0};
    ff_tbmetadata*               fftbmd = NULL;
    fftrail_privdata*            privdata = NULL;
    fftrail_table_deserialentry* deserialentry = NULL;

    if (NULL == data)
    {
        return true;
    }

    fftbmd = (ff_tbmetadata*)data;
    /* Check if file switch occurred, if so need to clean up cache */
    if (FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* Swap */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    /* Insert data into cache */
    if (false == parsertrail_tbmetadata2hash(parsertrail, fftbmd))
    {
        elog(RLOG_WARNING, "parser trail table metadata error");
        return false;
    }

    /* Put table into hash */
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;
    tbkey.tbnum = fftbmd->tbmdno;
    deserialentry = hash_search(privdata->tables, &tbkey, HASH_ENTER, &found);
    if (false == found)
    {
        deserialentry->key.tbnum = tbkey.tbnum;
        deserialentry->oid = fftbmd->oid;

        privdata->tbnum++;
        privdata->tbentrys = lappend(privdata->tbentrys, deserialentry);
    }
    else
    {
        /* Found, then replace */
        if (NULL != deserialentry->columns)
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

    /* Print content */
    /* Log level is debug */
    if (RLOG_DEBUG == g_loglevel)
    {
        /* Output debug log */
        elog(RLOG_DEBUG, "----------Trail TableMeta Begin----------------");
        elog(RLOG_DEBUG, "dbmdno:           %u", fftbmd->header.dbmdno);
        elog(RLOG_DEBUG, "tbmdno:           %u", fftbmd->tbmdno);
        elog(RLOG_DEBUG, "oid:              %u", fftbmd->oid);
        elog(RLOG_DEBUG, "schema:           %s", fftbmd->schema);
        elog(RLOG_DEBUG, "table:            %s", fftbmd->table);
        elog(RLOG_DEBUG, "flag:             %u", fftbmd->flag);
        elog(RLOG_DEBUG, "colcnt:           %u", fftbmd->colcnt);
        elog(RLOG_DEBUG, "indexcnt:         %u", fftbmd->index ? fftbmd->index->length : 0);
        elog(RLOG_DEBUG, "----table columns begin----");
        for (index = 0; index < fftbmd->colcnt; index++)
        {
            elog(RLOG_DEBUG, "  colid:      %u", deserialentry->columns[index].num);
            elog(RLOG_DEBUG, "  flag:       %u", deserialentry->columns[index].flag);
            elog(RLOG_DEBUG, "  type:       %u", deserialentry->columns[index].typid);
            elog(RLOG_DEBUG, "  column:     %s", deserialentry->columns[index].column);
        }
        elog(RLOG_DEBUG, "----table columns   end----");

        elog(RLOG_DEBUG, "----------Trail TableMeta   end----------------");
    }
    return true;
}

/*
 * Database info cleanup
 */
void parsertrail_tbmetadataclean(parsertrail* parsertrail, void* data)
{
    ff_tbmetadata* fftbmd = NULL;

    if (NULL == data)
    {
        return;
    }

    fftbmd = (ff_tbmetadata*)data;

    /* Memory release */
    ff_tbmetadata_free(fftbmd);
}
