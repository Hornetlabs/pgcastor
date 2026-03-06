#ifndef _RIPPLE_CONSTRAINT_H
#define _RIPPLE_CONSTRAINT_H

/* colvalue2constraint */
ripple_catalogdata* ripple_constraint_colvalue2constraint(void* in_colvalue);

/* catalogdata2transcache */
void ripple_constraint_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void ripple_constraint_catalogdatafree(ripple_catalogdata* catalogdata);

void ripple_constraint_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_constraintdata_write(List* constraint_list, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_constraintcache_load(ripple_sysdict_header_array* array);

void ripple_constraintcache_write(HTAB* constraintcache, uint64 *offset, ripple_sysdict_header_array* array);

#endif
