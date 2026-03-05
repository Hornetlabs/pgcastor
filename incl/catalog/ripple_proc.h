#ifndef _RIPPLE_PROC_H
#define _RIPPLE_PROC_H


void ripple_proc_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_procdata_write(List* ripple_proc, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_proccache_load(ripple_sysdict_header_array* array);

void ripple_proccache_write(HTAB* proccache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2proc */
ripple_catalogdata* ripple_proc_colvalue2proc(void* in_colvalue);
ripple_catalogdata* ripple_proc_colvalue2proc_hg902(void* in_colvalue);

#define ripple_proc_colvalue2proc_hg458 ripple_proc_colvalue2proc
#define ripple_proc_colvalue2proc_hg457 ripple_proc_colvalue2proc
#define ripple_proc_colvalue2proc_hg901 ripple_proc_colvalue2proc

/* catalogdata2transcache */
void ripple_proc_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void ripple_proc_catalogdatafree(ripple_catalogdata* catalogdata);

#endif
