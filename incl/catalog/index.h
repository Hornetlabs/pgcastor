#ifndef INDEX_H
#define INDEX_H

extern void index_getfromdb(PGconn* conn, cache_sysdicts* sysdicts);
extern catalogdata* index_colvalue2index(void* in_colvalue);
extern void index_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);
extern void index_catalogdatafree(catalogdata* catalogdata);
extern void* index_getbyoid(Oid oid, HTAB* by_index);
extern void indexcache_write(HTAB* indexcache, uint64* offset, sysdict_header_array* array);
extern HTAB* indexcache_load(sysdict_header_array* array);

#endif
