#ifndef _FFTRAIL_RESET_H
#define _FFTRAIL_RESET_H

/* 序列化reset信息 */
bool fftrail_reset_serail(void* data, void* state);

/* 反序列化reset信息 */
bool fftrail_reset_deserail(void** data, void* state);

#endif
