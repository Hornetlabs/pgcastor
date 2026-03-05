#ifndef _RIPPLE_NAMESPACE_H
#define _RIPPLE_NAMESPACE_H

void ripple_namespace_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_namespacedata_write(List* ripple_namespace, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_namespacecache_load(ripple_sysdict_header_array* array);

void ripple_namespacecache_write(HTAB* namespacecache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2namespace */
ripple_catalogdata* ripple_namespace_colvalue2namespace(void* in_colvalue);
ripple_catalogdata* ripple_namespace_colvalue2namespace_hg902(void* in_colvalue);

#define ripple_namespace_colvalue2namespace_hg458 ripple_namespace_colvalue2namespace
#define ripple_namespace_colvalue2namespace_hg457 ripple_namespace_colvalue2namespace
#define ripple_namespace_colvalue2namespace_hg901 ripple_namespace_colvalue2namespace

/* catalogdata2transcache */
void ripple_namespace_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void ripple_namespace_catalogdatafree(ripple_catalogdata* catalogdata);

/* 根据oid获取namespace数据 */
void* ripple_namespace_getbyoid(Oid oid, HTAB* by_namespace);

#endif