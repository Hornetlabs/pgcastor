#ifndef _ENUM_H
#define _ENUM_H


void enum_getfromdb(PGconn *conn, cache_sysdicts* sysdicts);

void enumdata_write(List* enum_list, uint64 *offset, sysdict_header_array* array);

HTAB* enumcache_load(sysdict_header_array* array);

void enumcache_write(HTAB* enumscache, uint64 *offset, sysdict_header_array* array);

/* colvalue2enum */
catalogdata* enum_colvalue2enum(void* in_colvalue);

/* catalogdata2transcache */
void enum_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void enum_catalogdatafree(catalogdata* catalogdata);

#endif