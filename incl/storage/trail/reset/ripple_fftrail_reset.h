#ifndef _RIPPLE_FFTRAIL_RESET_H
#define _RIPPLE_FFTRAIL_RESET_H

/* 序列化reset信息 */
bool ripple_fftrail_reset_serail(void* data, void* state);

/* 反序列化reset信息 */
bool ripple_fftrail_reset_deserail(void** data, void* state);

#endif
