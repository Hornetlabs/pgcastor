#ifndef _RIPPLE_FFTRAIL_TXNCOMMIT_H
#define _RIPPLE_FFTRAIL_TXNCOMMIT_H

/*
 * 序列化事务结束标识
 * 
 * 并不是所有的事务都会有此标识，只有在事务的结束为 metadata 时，在 Trail 文件中含有此标识
 * 
 */
bool ripple_fftrail_txncommit_serial(void* data, void* state);


/* 事务结束标识反序列化 */
bool ripple_fftrail_txncommit_deserial(void** data, void* state);

#endif
