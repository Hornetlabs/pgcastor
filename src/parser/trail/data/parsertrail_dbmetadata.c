#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_metadata.h"
#include "catalog/catalog.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_dbmetadata.h"

/* Add data to database hash */
static bool parsertrail_dbmetadata2hash(parsertrail* parsertrail, ff_dbmetadata* ffdbmd)
{
    /*
     * Skip if exists, add if not found
     */
    bool                       nadd = false;
    bool                       found = false;
    catalog_database_value*    dbentry = NULL;
    catalog_datname2oid_value* dbnameentry = NULL;
    pg_parser_NameData         dbname = {{'\0'}};
    HASHCTL                    hctl = {0};
    txn*                       cur_txn = parsertrail->lasttxn;
    bool                       add_txn = false;
    txnstmt*                   stmt = NULL;
    txnstmt_metadata*          metadata = NULL;
    ListCell*                  sys_begin = NULL;

    /*
     * Check if exists in hash table, add if not found
     */
    if (NULL == parsertrail->transcache->sysdicts->by_database)
    {
        /* Create hash */
        nadd = true;
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(Oid);
        hctl.entrysize = sizeof(catalog_database_value);
        parsertrail->transcache->sysdicts->by_database =
            hash_create("decodehdatabase", 256, &hctl, HASH_ELEM | HASH_BLOBS);
    }

    dbentry = hash_search(parsertrail->transcache->sysdicts->by_database, &ffdbmd->oid, HASH_ENTER, &found);
    if (false == found)
    {
        /* Add */
        dbentry->oid = ffdbmd->oid;
        dbentry->database = (pg_sysdict_Form_pg_database)rmalloc1(sizeof(pg_parser_sysdict_pgdatabase));
        if (NULL == dbentry->database)
        {
            elog(RLOG_WARNING, "out of memory");
            return false;
        }
        rmemset0(dbentry->database, 0, '\0', sizeof(pg_parser_sysdict_pgdatabase));
        dbentry->database->oid = ffdbmd->oid;
        rmemcpy1(dbentry->database->datname.data, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
    }

    if (false == nadd)
    {
        return true;
    }

    if (NULL == parsertrail->transcache->sysdicts->by_datname2oid)
    {
        rmemset1(&hctl, 0, 0, sizeof(hctl));
        hctl.keysize = sizeof(pg_parser_NameData);
        hctl.entrysize = sizeof(catalog_datname2oid_value);
        parsertrail->transcache->sysdicts->by_datname2oid =
            hash_create("decodehdbname2oid", 256, &hctl, HASH_ELEM | HASH_BLOBS);
    }

    rmemcpy1(dbname.data, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
    dbnameentry = hash_search(parsertrail->transcache->sysdicts->by_datname2oid, dbname.data, HASH_ENTER, &found);
    if (false == found)
    {
        /* Add */
        dbnameentry->oid = ffdbmd->oid;
        rmemcpy1(dbnameentry->datname.data, 0, ffdbmd->dbname, strlen(ffdbmd->dbname));
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
    if (ffdbmd)
    {
        /* pg_database */
        catalogdata*            catalog_db = rmalloc0(sizeof(catalogdata));
        catalog_database_value* db_value = rmalloc0(sizeof(catalog_database_value));

        rmemset0(catalog_db, 0, 0, sizeof(catalogdata));
        rmemset0(db_value, 0, 0, sizeof(catalog_database_value));

        /* database catalogdata assignment */
        catalog_db->op = CATALOG_OP_INSERT;
        catalog_db->type = CATALOG_TYPE_DATABASE;

        /* db_value assignment */
        db_value->database = rmalloc0(sizeof(pg_parser_sysdict_pgdatabase));
        rmemset0(db_value->database, 0, 0, sizeof(pg_parser_sysdict_pgdatabase));
        db_value->oid = ffdbmd->oid;

        /*
         * database structure assignment
         *
         * oid
         * datname
         */
        db_value->database->oid = ffdbmd->oid;
        strcpy(db_value->database->datname.data, ffdbmd->dbname);

        catalog_db->catalog = (void*)db_value;

        /* database assembly complete, append to list */
        cur_txn->sysdictHis = lappend(cur_txn->sysdictHis, catalog_db);
        sys_begin = list_tail(cur_txn->sysdictHis);
    }

    /* Create METADATA stmt */
    /* Add stmt as system table segment identifier */
    stmt = txnstmt_init();
    if (NULL == stmt)
    {
        elog(RLOG_WARNING, "init txnstmt error");
        return false;
    }
    stmt->type = TXNSTMT_TYPE_METADATA;

    metadata = txnstmt_metadata_init();
    if (NULL == metadata)
    {
        elog(RLOG_WARNING, "init metastmt error");
        return false;
    }
    stmt->stmt = (void*)metadata;
    metadata->begin = sys_begin;
    metadata->end = list_tail(cur_txn->sysdictHis);
    cur_txn->stmts = lappend(cur_txn->stmts, stmt);

    /* If created new transaction, add transaction to cache here */
    if (add_txn)
    {
        parsertrail->dtxns = dlist_put(parsertrail->dtxns, cur_txn);
        if (NULL == parsertrail->dtxns)
        {
            elog(RLOG_WARNING, "parser trail database metadata add txn to dlist error");
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
bool parsertrail_dbmetadataapply(parsertrail* parsertrail, void* data)
{
    bool                            found = false;
    ff_dbmetadata*                  dbmetadata = NULL;
    fftrail_privdata*               privdata = NULL;
    fftrail_database_deserialentry* deserialentry = NULL;

    dbmetadata = (ff_dbmetadata*)data;

    /* Apply database info */
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;

    deserialentry = hash_search(privdata->databases, &dbmetadata->dbmdno, HASH_ENTER, &found);
    if (true == found)
    {
        elog(RLOG_ERROR, "in version 1.0, privdata->databases shoulde be empty");
    }

    /* Add */
    deserialentry->no = dbmetadata->dbmdno;
    deserialentry->oid = dbmetadata->oid;
    rmemcpy1(deserialentry->database, 0, dbmetadata->dbname, strlen(dbmetadata->dbname));

    /* Append to list */
    privdata->dbentrys = lappend(privdata->dbentrys, deserialentry);

    if (false == parsertrail_dbmetadata2hash(parsertrail, dbmetadata))
    {
        elog(RLOG_WARNING, "parser trail dbmetadata error");
        return false;
    }

    /* Log level is debug */
    if (RLOG_DEBUG == g_loglevel)
    {
        /* Output debug log */
        elog(RLOG_DEBUG, "----------Trail MetaDB Begin----------------");
        elog(RLOG_DEBUG, "dbmdno:           %u", dbmetadata->header.dbmdno);
        elog(RLOG_DEBUG, "tbmdno:           %u", dbmetadata->header.tbmdno);
        elog(RLOG_DEBUG, "transid:          %lu", dbmetadata->header.transid);
        elog(RLOG_DEBUG, "transind:         %u", dbmetadata->header.transind);
        elog(RLOG_DEBUG, "totallength:      %lu", dbmetadata->header.totallength);
        elog(RLOG_DEBUG, "reclength:        %u", dbmetadata->header.reclength);
        elog(RLOG_DEBUG, "reccount:         %u", dbmetadata->header.reccount);
        elog(RLOG_DEBUG, "formattype:       %u", dbmetadata->header.formattype);
        elog(RLOG_DEBUG, "subtype:          %u", dbmetadata->header.subtype);
        elog(RLOG_DEBUG, "dbmdno:           %u", dbmetadata->dbmdno);
        elog(RLOG_DEBUG, "oid:              %u", dbmetadata->oid);
        elog(RLOG_DEBUG, "dbname:           %s", dbmetadata->dbname);
        elog(RLOG_DEBUG, "charset:          %s", dbmetadata->charset);
        elog(RLOG_DEBUG, "timezone:         %s", dbmetadata->timezone);
        elog(RLOG_DEBUG, "money:            %s", dbmetadata->money);
        elog(RLOG_DEBUG, "----------Trail MetaDB   End----------------");
    }
    return true;
}

/* Cleanup work */
void parsertrail_dbmetadataclean(parsertrail* parsertrail, void* data)
{
    ff_dbmetadata* dbmetadata = NULL;

    if (!data)
    {
        return;
    }

    dbmetadata = (ff_dbmetadata*)data;

    UNUSED(parsertrail);

    /* Memory release */
    if (NULL != dbmetadata->dbname)
    {
        rfree(dbmetadata->dbname);
    }

    if (NULL != dbmetadata->charset)
    {
        rfree(dbmetadata->charset);
    }

    if (NULL != dbmetadata->timezone)
    {
        rfree(dbmetadata->timezone);
    }

    if (NULL != dbmetadata->money)
    {
        rfree(dbmetadata->money);
    }
    rfree(dbmetadata);
}
