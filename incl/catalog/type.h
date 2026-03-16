#ifndef _RIPPLE_TYPE_H
#define _RIPPLE_TYPE_H

void ripple_type_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_typedata_write(List* ripple_type, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_typecache_load(ripple_sysdict_header_array* array);

void ripple_typecache_write(HTAB* typecache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2type */
ripple_catalogdata* ripple_type_colvalue2type(void* in_colvalue);
ripple_catalogdata* ripple_type_colvalue2type_pg14(void* in_colvalue);

/* catalogdata2transcache */
void ripple_type_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void* ripple_type_getbyoid(Oid oid, HTAB* by_type);

void ripple_type_catalogdatafree(ripple_catalogdata* catalogdata);

#endif