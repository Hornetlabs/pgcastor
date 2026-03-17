#ifndef _RANGE_H
#define _RANGE_H

void range_getfromdb(PGconn *conn, cache_sysdicts* sysdicts);

void rangedata_write(List* range, uint64 *offset, sysdict_header_array* array);

HTAB* rangecache_load(sysdict_header_array* array);

void rangecache_write(HTAB* rangecache, uint64 *offset, sysdict_header_array* array);

/* colvalue2range */
catalogdata* range_colvalue2range(void* in_colvalue);

/* catalogdata2transcache */
void range_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void range_catalogdatafree(catalogdata* catalogdata);

#endif