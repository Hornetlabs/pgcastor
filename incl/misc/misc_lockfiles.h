#ifndef _MISC_LOCKFILES_H
#define _MISC_LOCKFILES_H

/* 创建锁文件 */
void misc_lockfiles_create(const char *filename);

/* 在lock文件中获取 pid */
long misc_lockfiles_getpid(void);

void misc_lockfiles_unlink(int status, void* arg);

#endif
