#ifndef _FFTRAIL_TXDDL_H
#define _FFTRAIL_TXDDL_H

/* ddl 语句序列化 */
bool fftrail_txnddl_serial(void* data, void* state);

/* ddl 语句反序列化 */
bool fftrail_txnddl_deserial(void** data, void* state);

#endif

