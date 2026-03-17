/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
*/

#ifndef _PROCESS_H
#define _PROCESS_H

/* 关闭标准输入/输出 */
void closestd(void);

/* 设置为后台执行 */
void makedaemon(void);

/* 执行后台命令 */
bool execcommand(char* cmd, void* args, void (*childdestroy)(void* args));

#endif

