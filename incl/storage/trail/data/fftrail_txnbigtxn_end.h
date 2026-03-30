#ifndef _FFTRAIL_TXNBIGTXN_END_H
#define _FFTRAIL_TXNBIGTXN_END_H

/* ddl statement serialization */
bool fftrail_txnbigtxn_end_serial(void* data, void* state);

/* ddl statement deserialization */
bool fftrail_txnbigtxn_end_deserial(void** data, void* state);

#endif