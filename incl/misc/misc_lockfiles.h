#ifndef _MISC_LOCKFILES_H
#define _MISC_LOCKFILES_H

/* Create lock file */
void misc_lockfiles_create(const char* filename);

/* Get pid from lock file */
long misc_lockfiles_getpid(void);

void misc_lockfiles_unlink(int status, void* arg);

#endif
