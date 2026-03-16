#ifndef _RIPPLE_FFTRAIL_DBMETADATA_H
#define _RIPPLE_FFTRAIL_DBMETADATA_H

/* 数据库信息序列化 */
bool ripple_fftrail_dbmetadata_serial(void* data, void* state);

/* 数据库信息反序列化 */
bool ripple_fftrail_dbmetadata_deserial(void** data, void* state);

#endif
