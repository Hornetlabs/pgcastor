#ifndef _THREAD_H
#define _THREAD_H

typedef void* (*thrworkfunc)(void*);

int osal_thread_mutex_init(pthread_mutex_t* mutex, const pthread_mutexattr_t* attr);

int osal_thread_mutex_destroy(pthread_mutex_t* mutex);

int osal_thread_cond_init(pthread_cond_t* cond, const pthread_condattr_t* attr);

int osal_thread_cond_destroy(pthread_cond_t* cond);

int osal_thread_lock(pthread_mutex_t* mutex);

int osal_thread_unlock(pthread_mutex_t* mutex);

int osal_thread_cond_signal(pthread_cond_t* cond);

int osal_thread_cond_timewait(pthread_cond_t* cond, pthread_mutex_t* mutex, const struct timespec* abstime);

int osal_thread_create(pthread_t* thread, const pthread_attr_t* attr, thrworkfunc func, void* arg);

/* Reclaim thread */
int osal_thread_join(pthread_t thread, void** retval);

int osal_thread_tryjoin_np(pthread_t thread, void** retval);

/* Set thread name */
int osal_thread_setname_np(pthread_t thread, const char* name);

/* Thread exit */
void osal_thread_exit(void* retval);

#endif
