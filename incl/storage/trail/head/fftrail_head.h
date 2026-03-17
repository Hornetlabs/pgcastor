#ifndef _FFTRAIL_HEAD_H
#define _FFTRAIL_HEAD_H

/* 序列化头信息 */
bool fftrail_head_serail(void* data, void* state);

/* 反序列化信息 */
bool fftrail_head_deserail(void** data, void* state);

#endif
