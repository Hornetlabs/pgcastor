#ifndef _PROC_H
#define _PROC_H


void proc_getfromdb(PGconn *conn, cache_sysdicts* sysdicts);

void procdata_write(List* proc, uint64 *offset, sysdict_header_array* array);

HTAB* proccache_load(sysdict_header_array* array);

void proccache_write(HTAB* proccache, uint64 *offset, sysdict_header_array* array);

/* colvalue2proc */
catalogdata* proc_colvalue2proc(void* in_colvalue);

/* catalogdata2transcache */
void proc_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void proc_catalogdatafree(catalogdata* catalogdata);

#endif
