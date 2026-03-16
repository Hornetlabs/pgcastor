#ifndef _RIPPLE_THREAD_H
#define _RIPPLE_THREAD_H


typedef void* (*thrworkfunc)(void *);

int ripple_thread_mutex_init(pthread_mutex_t* mutex, const pthread_mutexattr_t* attr);

int ripple_thread_mutex_destroy(pthread_mutex_t* mutex);

int ripple_thread_cond_init(pthread_cond_t* cond, const pthread_condattr_t* attr);

int ripple_thread_cond_destroy(pthread_cond_t* cond);

int ripple_thread_lock(pthread_mutex_t* mutex);

int ripple_thread_unlock(pthread_mutex_t* mutex);

int ripple_thread_cond_signal(pthread_cond_t *cond);

int ripple_thread_cond_timewait(pthread_cond_t *cond, pthread_mutex_t* mutex, const struct timespec* abstime);

int ripple_thread_create(pthread_t* thread, const pthread_attr_t *attr, thrworkfunc func, void *arg);

/* 回收线程 */
int ripple_thread_join(pthread_t thread, void **retval);

int ripple_thread_tryjoin_np(pthread_t thread, void **retval);

/* 设置线程的名称 */
int ripple_thread_setname_np(pthread_t thread, const char* name);

/* 线程退出 */
void ripple_pthread_exit(void* retval);

#endif
