#ifndef _FFTRAIL_TXREFRESH_H
#define _FFTRAIL_TXREFRESH_H

/* ddl 语句序列化 */
bool fftrail_txnrefresh_serial(void* data, void* state);

/* ddl 语句反序列化 */
bool fftrail_txnrefresh_deserial(void** data, void* state);

#endif
