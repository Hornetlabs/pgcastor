#include "app_incl.h"
#include "port/thread/thread.h"

/* Initialize mutex */
int osal_thread_mutex_init(pthread_mutex_t* mutex, const pthread_mutexattr_t* attr)
{
    return pthread_mutex_init(mutex, attr);
}

/* Destroy mutex */
int osal_thread_mutex_destroy(pthread_mutex_t* mutex)
{
    return pthread_mutex_destroy(mutex);
}

/* Initialize condition variable */
int osal_thread_cond_init(pthread_cond_t* cond, const pthread_condattr_t* attr)
{
    return pthread_cond_init(cond, attr);
}

/* Destroy condition variable */
int osal_thread_cond_destroy(pthread_cond_t* cond)
{
    return pthread_cond_destroy(cond);
}

/* Lock */
int osal_thread_lock(pthread_mutex_t* mutex)
{
    return pthread_mutex_lock(mutex);
}

/* Unlock */
int osal_thread_unlock(pthread_mutex_t* mutex)
{
    return pthread_mutex_unlock(mutex);
}

/* Signal */
int osal_thread_cond_signal(pthread_cond_t* cond)
{
    return pthread_cond_signal(cond);
}

/* Wait for condition */
int osal_thread_cond_timewait(pthread_cond_t*        cond,
                              pthread_mutex_t*       mutex,
                              const struct timespec* abstime)
{
    return pthread_cond_timedwait(cond, mutex, abstime);
}

/* Create thread */
int osal_thread_create(pthread_t* thread, const pthread_attr_t* attr, thrworkfunc func, void* arg)
{
    return pthread_create(thread, NULL, func, arg);
}

/* Join thread */
int osal_thread_tryjoin_np(pthread_t thread, void** retval)
{
    return pthread_tryjoin_np(thread, retval);
}

/* Join thread */
int osal_thread_join(pthread_t thread, void** retval)
{
    return pthread_join(thread, retval);
}

/* Set thread name */
int osal_thread_setname_np(pthread_t thread, const char* name)
{
    return pthread_setname_np(thread, name);
}

/* Exit thread */
void osal_thread_exit(void* retval)
{
    pthread_exit(retval);
}
