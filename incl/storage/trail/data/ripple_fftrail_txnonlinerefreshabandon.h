#ifndef _RIPPLE_FFTRAIL_TXNONLINEREFRESHABANDON_H_
#define _RIPPLE_FFTRAIL_TXNONLINEREFRESHABANDON_H_

/* 序列化onlinerefreshabandon */
extern bool ripple_fftrail_txnonlinerefreshabandon_serial(void* data, void* state);

/* 反序列化onlinerefreshabandon */
extern bool ripple_fftrail_txnonlinerefreshabandon_deserial(void** data, void* state);

#endif
