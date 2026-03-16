#ifndef _RIPPLE_DATABASE_H
#define _RIPPLE_DATABASE_H

void ripple_database_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_databasedata_write(List* database_list, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_databasecache_load(ripple_sysdict_header_array* array);

HTAB* ripple_datname2oid_cache_load(ripple_sysdict_header_array* array);

void ripple_databasecache_write(HTAB* databasecache, uint64 *offset, ripple_sysdict_header_array* array);


/* colvalue2database */
ripple_catalogdata* ripple_database_colvalue2database(void* in_colvalue);
#define ripple_database_colvalue2database_pg14 ripple_database_colvalue2database

/* catalogdata2transcache */
void ripple_database_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

/* 释放 */
void ripple_database_catalogdatafree(ripple_catalogdata* catalogdata);

/* 获取数据库的名称 */
char* ripple_database_getdbname(Oid dbid, HTAB* by_database);

/* 获取dboid */
Oid ripple_database_getdbid(HTAB* by_database);

#endif
