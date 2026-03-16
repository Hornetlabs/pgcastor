#ifndef _RIPPLE_FFTRAIL_TXNBIGTXN_BEGIN_H
#define _RIPPLE_FFTRAIL_TXNBIGTXN_BEGIN_H

/* ddl 语句序列化 */
bool ripple_fftrail_txnbigtxn_begin_serial(void* data, void* state);

/* ddl 语句反序列化 */
bool ripple_fftrail_txnbigtxn_begin_deserial(void** data, void* state);

#endif