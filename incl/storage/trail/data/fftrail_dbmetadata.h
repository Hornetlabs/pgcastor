#ifndef _FFTRAIL_DBMETADATA_H
#define _FFTRAIL_DBMETADATA_H

/* 数据库信息序列化 */
bool fftrail_dbmetadata_serial(void* data, void* state);

/* 数据库信息反序列化 */
bool fftrail_dbmetadata_deserial(void** data, void* state);

#endif
