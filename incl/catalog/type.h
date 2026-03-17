#ifndef _TYPE_H
#define _TYPE_H

void type_getfromdb(PGconn *conn, cache_sysdicts* sysdicts);

void typedata_write(List* type, uint64 *offset, sysdict_header_array* array);

HTAB* typecache_load(sysdict_header_array* array);

void typecache_write(HTAB* typecache, uint64 *offset, sysdict_header_array* array);

/* colvalue2type */
catalogdata* type_colvalue2type(void* in_colvalue);
catalogdata* type_colvalue2type_pg14(void* in_colvalue);

/* catalogdata2transcache */
void type_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void* type_getbyoid(Oid oid, HTAB* by_type);

void type_catalogdatafree(catalogdata* catalogdata);

#endif