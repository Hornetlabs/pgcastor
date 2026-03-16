#ifndef RIPPLE_INDEX_H
#define RIPPLE_INDEX_H

extern void ripple_index_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);
extern ripple_catalogdata* ripple_index_colvalue2index(void* in_colvalue);
extern void ripple_index_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);
extern void ripple_index_catalogdatafree(ripple_catalogdata* catalogdata);
extern void* ripple_index_getbyoid(Oid oid, HTAB* by_index);
extern void ripple_indexcache_write(HTAB* indexcache, uint64 *offset, ripple_sysdict_header_array* array);
extern HTAB* ripple_indexcache_load(ripple_sysdict_header_array* array);


#endif
