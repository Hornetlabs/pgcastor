#ifndef _OPERATOR_H
#define _OPERATOR_H

void operator_getfromdb(PGconn *conn, cache_sysdicts* sysdicts);

void operatordata_init(PGconn *conn, uint64 *offset, sysdict_header_array* array);

void operatordata_write(List* operator_list, uint64 *offset, sysdict_header_array* array);

HTAB* operatorcache_load(sysdict_header_array* array);

void operatorcache_write(HTAB* operatorcache, uint64 *offset, sysdict_header_array* array);

#endif
