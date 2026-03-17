#ifndef _CONSTRAINT_H
#define _CONSTRAINT_H

/* colvalue2constraint */
catalogdata* constraint_colvalue2constraint(void* in_colvalue);

/* catalogdata2transcache */
void constraint_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void constraint_catalogdatafree(catalogdata* catalogdata);

void constraint_getfromdb(PGconn *conn, cache_sysdicts* sysdicts);

void constraintdata_write(List* constraint_list, uint64 *offset, sysdict_header_array* array);

HTAB* constraintcache_load(sysdict_header_array* array);

void constraintcache_write(HTAB* constraintcache, uint64 *offset, sysdict_header_array* array);

#endif
