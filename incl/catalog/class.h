#ifndef _CLASS_H
#define _CLASS_H

void class_attribute_getfromdb(PGconn* conn, cache_sysdicts* sysdicts);

bool bool_judgment(char* str);

void classdata_write(List* class, uint64* offset, sysdict_header_array* array);

HTAB* classcache_load(sysdict_header_array* array);

void classcache_write(HTAB* classcache, uint64* offset, sysdict_header_array* array);

/* colvalue2class */
catalogdata* class_colvalue2class(void* in_colvalue);

catalogdata* class_colvalue2class_nofilter(void* in_colvalue);

/* catalog2his */
void class_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

/* catalog->class Memory release */
void class_catalogdatafree(catalogdata* catalogdata);

/* Get class data by oid */
void* class_getbyoid(Oid oid, HTAB* by_class);

#endif