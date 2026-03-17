#include "app_incl.h"
#include "port/thread/thread.h"

/* 初始化锁信息 */
int osal_thread_mutex_init(pthread_mutex_t* mutex, const pthread_mutexattr_t* attr)
{
    return pthread_mutex_init(mutex, attr);
}

/* 销毁锁信息 */
int osal_thread_mutex_destroy(pthread_mutex_t* mutex)
{
    return pthread_mutex_destroy(mutex);
}

/* 初始化信号锁信息 */
int osal_thread_cond_init(pthread_cond_t* cond, const pthread_condattr_t* attr)
{
    return pthread_cond_init(cond, attr);
}

/* 销毁信号锁信息 */
int osal_thread_cond_destroy(pthread_cond_t* cond)
{
    return pthread_cond_destroy(cond);
}

/* 加锁 */
int osal_thread_lock(pthread_mutex_t* mutex)
{
    return pthread_mutex_lock(mutex);
}

/* 解锁 */
int osal_thread_unlock(pthread_mutex_t* mutex)
{
    return pthread_mutex_unlock(mutex);
}

/* 通知 */
int osal_thread_cond_signal(pthread_cond_t *cond)
{
    return pthread_cond_signal(cond);
}

/* 等待条件成立 */
int osal_thread_cond_timewait(pthread_cond_t *cond, pthread_mutex_t* mutex, const struct timespec* abstime)
{
    return pthread_cond_timedwait(cond, mutex, abstime);
}

/* 创建线程 */
int osal_thread_create(pthread_t* thread, const pthread_attr_t *attr, thrworkfunc func, void *arg)
{
    return pthread_create(thread, NULL, func, arg);
}

/* 回收线程 */
int osal_thread_tryjoin_np(pthread_t thread, void **retval)
{
    return pthread_tryjoin_np(thread, retval);
}

/* 回收线程 */
int osal_thread_join(pthread_t thread, void **retval)
{
    return pthread_join(thread, retval);
}

/* 设置线程的名称 */
int osal_thread_setname_np(pthread_t thread, const char* name)
{
    return pthread_setname_np(thread, name);
}

/* 线程退出 */
void osal_thread_exit(void* retval)
{
    pthread_exit(retval);
}
