#ifndef _RIPPLE_RANGE_H
#define _RIPPLE_RANGE_H

void ripple_range_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_rangedata_write(List* ripple_range, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_rangecache_load(ripple_sysdict_header_array* array);

void ripple_rangecache_write(HTAB* rangecache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2range */
ripple_catalogdata* ripple_range_colvalue2range(void* in_colvalue);

#define ripple_range_colvalue2range_hg458 ripple_range_colvalue2range
#define ripple_range_colvalue2range_hg457 ripple_range_colvalue2range
#define ripple_range_colvalue2range_hg901 ripple_range_colvalue2range
#define ripple_range_colvalue2range_hg902 ripple_range_colvalue2range

/* catalogdata2transcache */
void ripple_range_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void ripple_range_catalogdatafree(ripple_catalogdata* catalogdata);

#endif