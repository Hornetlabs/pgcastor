#ifndef _RIPPLE_ATTRIBUTE_H
#define _RIPPLE_ATTRIBUTE_H



void ripple_attributedata_write(List* ripple_attributes, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_attributecache_load(ripple_sysdict_header_array* array);

void ripple_attributecache_write(HTAB* attributecache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2attr */
ripple_catalogdata* ripple_class_colvalue2attribute(void* in_colvalue);
ripple_catalogdata* ripple_class_colvalue2attribute_pg14(void* in_colvalue);
ripple_catalogdata* ripple_class_colvalue2attribute_hg902(void* in_colvalue);

void ripple_attribute_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

void ripple_attribute_catalogdatafree(ripple_catalogdata* catalogdata);

/* 根据oid获取attribute数据 */
void* ripple_attribute_getbyoid(Oid oid, HTAB* by_attribute);

#define ripple_class_colvalue2attribute_hg458 ripple_class_colvalue2attribute
#define ripple_class_colvalue2attribute_hg457 ripple_class_colvalue2attribute
#define ripple_class_colvalue2attribute_hg901 ripple_class_colvalue2attribute_pg14

#endif