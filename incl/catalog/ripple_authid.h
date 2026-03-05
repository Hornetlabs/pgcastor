#ifndef _RIPPLE_AUTHID_H
#define _RIPPLE_AUTHID_H

void ripple_authid_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_authiddata_write(List* authid_list, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_authidcache_load(ripple_sysdict_header_array* array);

void ripple_authidcache_write(HTAB* authidcache, uint64 *offset, ripple_sysdict_header_array* array);


/* colvalue2authid */
ripple_catalogdata* ripple_authid_colvalue2authid(void* in_colvalue);
ripple_catalogdata* ripple_authid_colvalue2authid_hg902(void* in_colvalue);

#define ripple_authid_colvalue2authid_hg458 ripple_authid_colvalue2authid
#define ripple_authid_colvalue2authid_hg457 ripple_authid_colvalue2authid
#define ripple_authid_colvalue2authid_hg901 ripple_authid_colvalue2authid

/* catalogdata2transcache */
void ripple_authid_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

/* 释放 */
void ripple_authid_catalogdatafree(ripple_catalogdata* catalogdata);

#endif
