#ifndef _RIPPLE_CLASS_H
#define _RIPPLE_CLASS_H

void ripple_class_attribute_getfromdb(PGconn *conn, ripple_cache_sysdicts* sysdicts);

bool bool_judgment(char * str);

// void ripple_classdata_write(List* ripple_class);
void ripple_classdata_write(List* ripple_class, uint64 *offset, ripple_sysdict_header_array* array);

HTAB* ripple_classcache_load(ripple_sysdict_header_array* array);

void ripple_classcache_write(HTAB* classcache, uint64 *offset, ripple_sysdict_header_array* array);

/* colvalue2class */
ripple_catalogdata* ripple_class_colvalue2class(void* in_colvalue);
ripple_catalogdata* ripple_class_colvalue2class_hg902(void* in_colvalue);

#define ripple_class_colvalue2class_hg458 ripple_class_colvalue2class
#define ripple_class_colvalue2class_hg457 ripple_class_colvalue2class
#define ripple_class_colvalue2class_hg901 ripple_class_colvalue2class

ripple_catalogdata* ripple_class_colvalue2class_nofilter(void* in_colvalue);

ripple_catalogdata* ripple_class_colvalue2class_nofilter_hg902(void* in_colvalue);

/* catalog2his */
void ripple_class_catalogdata2transcache(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);

/* catalog->class 内存释放 */
void ripple_class_catalogdatafree(ripple_catalogdata* catalogdata);

/* 根据oid获取class数据 */
void* ripple_class_getbyoid(Oid oid, HTAB* by_class);

#endif