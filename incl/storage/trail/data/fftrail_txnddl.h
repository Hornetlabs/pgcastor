#ifndef _FFTRAIL_TXDDL_H
#define _FFTRAIL_TXDDL_H

/* ddl statement serialization */
bool fftrail_txnddl_serial(void* data, void* state);

/* ddl statement deserialization */
bool fftrail_txnddl_deserial(void** data, void* state);

#endif
