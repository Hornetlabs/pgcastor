#ifndef _FFTRAIL_TXREFRESH_H
#define _FFTRAIL_TXREFRESH_H

/* ddl statement serialization */
bool fftrail_txnrefresh_serial(void* data, void* state);

/* ddl statement deserialization */
bool fftrail_txnrefresh_deserial(void** data, void* state);

#endif
