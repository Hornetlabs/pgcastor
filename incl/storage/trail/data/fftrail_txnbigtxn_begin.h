#ifndef _FFTRAIL_TXNBIGTXN_BEGIN_H
#define _FFTRAIL_TXNBIGTXN_BEGIN_H

/* ddl statement serialization */
bool fftrail_txnbigtxn_begin_serial(void* data, void* state);

/* ddl statement deserialization */
bool fftrail_txnbigtxn_begin_deserial(void** data, void* state);

#endif