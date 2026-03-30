#ifndef _FFTRAIL_TXNONLINEREFRESH_BEGIN_H
#define _FFTRAIL_TXNONLINEREFRESH_BEGIN_H

/* online refresh statement serialization */
bool fftrail_txnonlinerefresh_begin_serial(void* data, void* state);

/* online refresh statement deserialization */
bool fftrail_txnonlinerefresh_begin_deserial(void** data, void* state);

#endif
