#ifndef _ATTRIBUTE_H
#define _ATTRIBUTE_H



void attributedata_write(List* attributes, uint64 *offset, sysdict_header_array* array);

HTAB* attributecache_load(sysdict_header_array* array);

void attributecache_write(HTAB* attributecache, uint64 *offset, sysdict_header_array* array);

/* colvalue2attr */
catalogdata* class_colvalue2attribute(void* in_colvalue);
catalogdata* class_colvalue2attribute_pg14(void* in_colvalue);

void attribute_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

void attribute_catalogdatafree(catalogdata* catalogdata);

/* 根据oid获取attribute数据 */
void* attribute_getbyoid(Oid oid, HTAB* by_attribute);


#endif