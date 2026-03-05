#ifndef _RIPPLE_MISC_LOCKFILES_H
#define _RIPPLE_MISC_LOCKFILES_H

/* 创建锁文件 */
void ripple_misc_lockfiles_create(const char *filename);

/* 在lock文件中获取 pid */
long ripple_misc_lockfiles_getpid(void);

void ripple_misc_lockfiles_unlink(int status, void* arg);

#endif
