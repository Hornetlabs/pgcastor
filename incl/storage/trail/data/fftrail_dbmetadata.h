#ifndef _FFTRAIL_DBMETADATA_H
#define _FFTRAIL_DBMETADATA_H

/* Database information serialization */
bool fftrail_dbmetadata_serial(void* data, void* state);

/* Database information deserialization */
bool fftrail_dbmetadata_deserial(void** data, void* state);

#endif
