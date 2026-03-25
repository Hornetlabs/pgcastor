#ifndef _DATABASE_H
#define _DATABASE_H

void database_getfromdb(PGconn* conn, cache_sysdicts* sysdicts);

void databasedata_write(List* database_list, uint64* offset, sysdict_header_array* array);

HTAB* databasecache_load(sysdict_header_array* array);

HTAB* datname2oid_cache_load(sysdict_header_array* array);

void databasecache_write(HTAB* databasecache, uint64* offset, sysdict_header_array* array);

/* colvalue2database */
catalogdata* database_colvalue2database(void* in_colvalue);
#define database_colvalue2database_pg14 database_colvalue2database

/* catalogdata2transcache */
void database_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

/* Release */
void database_catalogdatafree(catalogdata* catalogdata);

/* Get database name */
char* database_getdbname(Oid dbid, HTAB* by_database);

/* Get dboid */
Oid database_getdbid(HTAB* by_database);

#endif
