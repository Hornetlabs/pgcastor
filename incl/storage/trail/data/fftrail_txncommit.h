#ifndef _FFTRAIL_TXNCOMMIT_H
#define _FFTRAIL_TXNCOMMIT_H

/*
 * Serialize transaction end flag
 *
 * Not all transactions have this flag, only when transaction end is metadata, this flag exists in
 * Trail file
 *
 */
bool fftrail_txncommit_serial(void* data, void* state);

/* Transaction end flag deserialization */
bool fftrail_txncommit_deserial(void** data, void* state);

#endif
