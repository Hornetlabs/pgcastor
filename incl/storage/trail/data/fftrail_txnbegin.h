#ifndef _FFTRAIL_TXNBEGIN_H
#define _FFTRAIL_TXNBEGIN_H

/*
 * Serialize transaction begin flag
 * Currently no parsing for this type
 */
bool fftrail_txnbegin_serial(void* data, void* state);

/* Transaction begin flag deserialization */
bool fftrail_txnbegin_deserial(void** data, void* state);

#endif
