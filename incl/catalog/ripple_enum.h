#ifndef _RIPPLE_ENUM_H
#define _RIPPLE_ENUM_H


void ripple_enum_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_enumdata_write(List* ripple_enum, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_enumcache_load(ripple_sysdict_header_array* array);

void ripple_enumcache_write(HTAB* enumscache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2enum */
ripple_catalogdata* ripple_enum_colvalue2enum(void* in_colvalue);

/* catalogdata2transcache */
void ripple_enum_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void ripple_enum_catalogdatafree(ripple_catalogdata* catalogdata);

#endif