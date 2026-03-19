#ifndef _AUTHID_H
#define _AUTHID_H

void authid_getfromdb(PGconn* conn, cache_sysdicts* sysdicts);

void authiddata_write(List* authid_list, uint64* offset, sysdict_header_array* array);

HTAB* authidcache_load(sysdict_header_array* array);

void authidcache_write(HTAB* authidcache, uint64* offset, sysdict_header_array* array);

/* colvalue2authid */
catalogdata* authid_colvalue2authid(void* in_colvalue);

/* catalogdata2transcache */
void authid_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

/* free */
void authid_catalogdatafree(catalogdata* catalogdata);

#endif
