#ifndef _NAMESPACE_H
#define _NAMESPACE_H

void namespace_getfromdb(PGconn* conn, cache_sysdicts* sysdicts);

void namespacedata_write(List* namespace, uint64* offset, sysdict_header_array* array);

HTAB* namespacecache_load(sysdict_header_array* array);

void namespacecache_write(HTAB* namespacecache, uint64* offset, sysdict_header_array* array);

/* colvalue2namespace */
catalogdata* namespace_colvalue2namespace(void* in_colvalue);

/* catalogdata2transcache */
void namespace_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void namespace_catalogdatafree(catalogdata* catalogdata);

/* Get namespace data by oid */
void* namespace_getbyoid(Oid oid, HTAB* by_namespace);

#endif