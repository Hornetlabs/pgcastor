#ifndef _RIPPLE_OPERATOR_H
#define _RIPPLE_OPERATOR_H

void ripple_operator_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

void ripple_operatordata_init(PGconn *conn, uint64 *offset, ripple_sysdict_header_array* array);

void ripple_operatordata_write(List* operator_list, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_operatorcache_load(ripple_sysdict_header_array* array);

void ripple_operatorcache_write(HTAB* operatorcache, uint64 *offset, ripple_sysdict_header_array* array);

#endif
