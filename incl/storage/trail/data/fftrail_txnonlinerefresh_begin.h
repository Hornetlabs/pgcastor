#ifndef _FFTRAIL_TXNONLINEREFRESH_BEGIN_H
#define _FFTRAIL_TXNONLINEREFRESH_BEGIN_H

/* online refresh 语句序列化 */
bool fftrail_txnonlinerefresh_begin_serial(void* data, void* state);

/* online refresh 语句反序列化 */
bool fftrail_txnonlinerefresh_begin_deserial(void** data, void* state);

#endif
