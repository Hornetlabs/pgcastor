#ifndef _CACHE_SYSDICTS_H
#define _CACHE_SYSDICTS_H

typedef struct CACHE_SYSDICTS
{
    /* system dictionary table caches */
    HTAB* by_class;     /* pg_class table info,             key: oid */
    HTAB* by_attribute; /* pg_attribute, hash+list, key: atttrelid, list of attribute info */
    HTAB* by_type;      /* pg_type table info, key: oid */
    HTAB* by_proc;      /* pg_proc table info, key: oid */

    /* pg_tablespace table info */
    HTAB* by_tablespace; /* pg_tablespace hash table, key: id */

    /* pg_namespace table info */
    HTAB* by_namespace; /* pg_namespace hash table, key: id */

    /* pg_range info */
    HTAB* by_range; /* pg_range hash table, key: rangetypid */

    /* pg_enum info */
    HTAB* by_enum; /* pg_enum, hash+list, key: enumtypid */

    /* pg_operator info */
    HTAB* by_operator; /* pg_operator table info, key: oid */

    /* pg_authid info */
    HTAB* by_authid; /* pg_authid table info, key: oid */

    /* pg_database info */
    HTAB* by_database; /* pg_database table info, key: oid */

    /* pg_constraint info */
    HTAB* by_constraint; /* pg_constraint table info, key: oid */

    /* database name to oid mapping */
    HTAB* by_datname2oid; /* pg_database table info, key: datname */

    /* relfilenode to oid mapping, key: relfilenode, entry: relfilenode2oid */
    HTAB* by_relfilenode;

    /* pg_index info */
    HTAB* by_index; /* pg_index, index info, key: oid */
} cache_sysdicts;

/* load data into cache */
void cache_sysdictsload(void** ref_sysdicts);

cache_sysdicts* cache_sysdicts_integrate_init(void);

void cache_sysdicts_free(void* args);

/* flush modified system dictionary cache to disk */
void sysdictscache_write(cache_sysdicts* sysdicts, XLogRecPtr redolsn);

/* build hash table keyed by relfilenode for fast lookups during WAL parsing */
HTAB* cache_sysdicts_buildrelfilenode2oid(Oid dbid, void* data);

/* free historical system dictionary data on transaction commit or rollback */
void cache_sysdicts_txnsysdicthisfree(List* sysdicthis);

/* free system dictionary data structure */
void cache_sysdicts_catalogdatafreevoid(void* args);

/* apply historical system dictionary changes to cache on transaction commit */
void cache_sysdicts_txnsysdicthis2cache(cache_sysdicts* sysdicts, List* sysdicthis);

/*
 * clear system table data by class
 */
void cache_sysdicts_clearsysdicthisbyclass(cache_sysdicts* sysdicts, ListCell* lc);

/*
 * apply a single system table change to cache
 */
void cache_sysdicts_txnsysdicthisitem2cache(cache_sysdicts* sysdicts, ListCell* lc);

#endif
