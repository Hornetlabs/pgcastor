#ifndef _RIPPLE_FFTRAIL_HEAD_H
#define _RIPPLE_FFTRAIL_HEAD_H

/* 序列化头信息 */
bool ripple_fftrail_head_serail(void* data, void* state);

/* 反序列化信息 */
bool ripple_fftrail_head_deserail(void** data, void* state);

#endif
