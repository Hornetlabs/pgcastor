#ifndef _FFTRAIL_TXNBEGIN_H
#define _FFTRAIL_TXNBEGIN_H

/*
 * 序列化事务开始标识
 * 暂时没有该类型解析
 */
bool fftrail_txnbegin_serial(void* data, void* state);


/* 事务开始标识反序列化 */
bool fftrail_txnbegin_deserial(void** data, void* state);

#endif
