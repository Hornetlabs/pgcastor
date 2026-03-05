#ifndef _RIPPLE_FFTRAIL_TAIL_H
#define _RIPPLE_FFTRAIL_TAIL_H

/* 序列化尾部信息 */
bool ripple_fftrail_tail_serail(void* data, void* state);

/* 反序列化尾部信息 */
bool ripple_fftrail_tail_deserail(void** data, void* state);

#endif
