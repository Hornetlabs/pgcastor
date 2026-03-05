/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
*/

#ifndef _RIPPLE_PROCESS_H
#define _RIPPLE_PROCESS_H

/* 关闭标准输入/输出 */
void ripple_closestd(void);

/* 设置为后台执行 */
void ripple_makedaemon(void);

/* 执行后台命令 */
bool ripple_execcommand(char* cmd, void* args, void (*childdestroy)(void* args));

#endif

