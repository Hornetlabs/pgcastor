#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "storage/ripple_file_buffer.h"


static void ripple_file_buffer_reset(ripple_file_buffer* rfbuffer)
{
    if(NULL == rfbuffer)
    {
        return;
    }

    rfbuffer->flag = RIPPLE_FILE_BUFFER_FLAG_NOP;
    rfbuffer->next = NULL;
    rfbuffer->tail = NULL;
    rfbuffer->start = 0;
    rfbuffer->used = false;
    rfbuffer->extra.chkpoint.sysdicts = NULL;
    rfbuffer->extra.chkpoint.redolsn.wal.lsn = InvalidXLogRecPtr;
    rfbuffer->extra.chkpoint.orgaddr.trail.fileid = 0;
    rfbuffer->extra.chkpoint.orgaddr.trail.offset = 0;
    rfbuffer->extra.chkpoint.segno.trail.fileid = 0;
    rfbuffer->extra.chkpoint.segno.trail.offset = 0;
    rfbuffer->extra.rewind.confirmlsn.wal.lsn = InvalidXLogRecPtr;
    rfbuffer->extra.rewind.restartlsn.wal.lsn = InvalidXLogRecPtr;
    rfbuffer->extra.rewind.fileaddr.trail.fileid = 0;
    rfbuffer->extra.rewind.fileaddr.trail.offset = 0;
    rmemset1(rfbuffer->data, 0, '\0', rfbuffer->maxsize);
}

/* 初始化 */
ripple_file_buffers* ripple_file_buffer_init(void)
{
    int index = 0;
    int mbytes = 0;
    ripple_file_buffers* filebuffers = NULL;

    if(NULL != filebuffers)
    {
        return filebuffers;
    }

    /* 获取缓存大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    if(mbytes < RIPPLE_FILE_BUFFER_MINSIZE)
    {
        mbytes = RIPPLE_FILE_BUFFER_MINSIZE;
    }
    else if(mbytes > RIPPLE_FILE_BUFFER_MAXSIZE)
    {
        mbytes = RIPPLE_FILE_BUFFER_MAXSIZE;
    }
    mbytes = RIPPLE_MB2BYTE(mbytes);

    /* 空间申请，并设置初始值 */
    filebuffers = (ripple_file_buffers*)rmalloc1(sizeof(ripple_file_buffers));
    if(NULL == filebuffers)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(filebuffers, 0, '\0', sizeof(ripple_file_buffers));

    /* 锁和信号初始化 */
    ripple_thread_mutex_init(&filebuffers->fllock, NULL);
    ripple_thread_mutex_init(&filebuffers->wfllock, NULL);
    filebuffers->flwsignal = false;
    filebuffers->wflwsignal = false;
    if(0 != ripple_thread_cond_init(&filebuffers->flcond, NULL))
    {
        elog(RLOG_ERROR, "buffer init, can not init pthread flcond, %s", strerror(errno));
    }

    if(0 != ripple_thread_cond_init(&filebuffers->wflcond, NULL))
    {
        elog(RLOG_ERROR, "buffer init, can not init pthread wflcond, %s", strerror(errno));
    }

    filebuffers->maxbufid = (mbytes / RIPPLE_FILE_BUFFER_SIZE);

    /* 缓存初始化 */
    filebuffers->buffers = (ripple_file_buffer*)rmalloc0(filebuffers->maxbufid*sizeof(ripple_file_buffer));
    if(NULL == filebuffers->buffers)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(filebuffers->buffers, 0, '\0', (filebuffers->maxbufid*sizeof(ripple_file_buffer)));
    for(index = 0; index < filebuffers->maxbufid; index++)
    {
        ripple_file_buffer_reset(&filebuffers->buffers[index]);
        filebuffers->buffers[index].bufid = (index+1);
        filebuffers->buffers[index].data = NULL;
        filebuffers->buffers[index].maxsize = 0;
        filebuffers->buffers[index].privdata = NULL;

        if(NULL != filebuffers->freelist)
        {
            filebuffers->buffers[index].next = filebuffers->freelist;
        }
        filebuffers->freelist = &filebuffers->buffers[index];
    }
    filebuffers->wflushlist = NULL;

    return filebuffers;
}

/* 根据 bufid 获取 buffer */
ripple_file_buffer* ripple_file_buffer_getbybufid(ripple_file_buffers* filebuffers, int bufid)
{
    return &filebuffers->buffers[bufid - 1];
}

/* 获取可用的 buffer */
int ripple_file_buffer_get(ripple_file_buffers* filebuffers, int* timeout)
{
    int iret = 0;
    ripple_file_buffer* rfbuffer = NULL;

    /* 加锁 */
    /* 加入到链表中 */
    *timeout = RIPPLE_ERROR_SUCCESS;
    while(1)
    {
        iret = ripple_thread_lock(&filebuffers->fllock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "get buffer from freelist, lock error:%s", strerror(errno));
            return RIPPLE_INVALID_BUFFERID;
        }

        /* 是否含有数据，不含有数据，那么等待数据 */
        if(NULL == filebuffers->freelist)
        {
            /* 设置超时时间 */
            struct timespec ts = { 0 };
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            filebuffers->flwsignal = true;
            iret = ripple_thread_cond_timewait(&filebuffers->flcond, &filebuffers->fllock, &ts);
            if(0 != iret)
            {
                if(iret == ETIMEDOUT)
                {
                    filebuffers->flwsignal = false;
                    *timeout = RIPPLE_ERROR_TIMEOUT;
                    ripple_thread_unlock(&(filebuffers->fllock));
                    return RIPPLE_INVALID_BUFFERID;
                }

                ripple_thread_unlock(&(filebuffers->fllock));
                elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
                return RIPPLE_INVALID_BUFFERID;
            }
            filebuffers->flwsignal = false;
            ripple_thread_unlock(&(filebuffers->fllock));
            usleep(50000);
            continue;
        }

        rfbuffer = filebuffers->freelist;
        filebuffers->freelist = filebuffers->freelist->next;

        /* 解锁 */
        iret = ripple_thread_unlock(&filebuffers->fllock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "unlock error:%s", strerror(errno));
            return RIPPLE_INVALID_BUFFERID;
        }

        rfbuffer->tail = NULL;
        rfbuffer->next = NULL;
        rfbuffer->used = true;
        if(NULL == rfbuffer->data)
        {
            /* 申请空间 */
            rfbuffer->maxsize = RIPPLE_FILE_BUFFER_SIZE;
            rfbuffer->data = rmalloc1(rfbuffer->maxsize);
            if(NULL == rfbuffer->data)
            {
                elog(RLOG_WARNING, "out of memory");
                return RIPPLE_INVALID_BUFFERID;
            }
            rmemset0(rfbuffer->data, 0, '\0', rfbuffer->maxsize);
        }
        rfbuffer->flag = RIPPLE_FILE_BUFFER_FLAG_DATA;
        return rfbuffer->bufid;
    }

    return RIPPLE_INVALID_BUFFERID;
}

/* 将 buffer 放入空闲队列 */
void ripple_file_buffer_free(ripple_file_buffers* filebuffers, ripple_file_buffer* rfbuffer)
{
    /*
     * 1、在 wflushlist 表中移除
     * 2、加入到 freelist 队列中
     */
    int iret = 0;

    ripple_file_buffer_reset(rfbuffer);

    /* 加入到空闲链表中 */
    /* 加锁 */
    iret = ripple_thread_lock(&filebuffers->fllock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "wait flush buffer set, get fllock error:%s", strerror(errno));
    }

    /* 加入到 freelist 中 */
    if(NULL != filebuffers->freelist)
    {
        rfbuffer->next = filebuffers->freelist;
    }
    filebuffers->freelist = rfbuffer;

    /* 查看是否有线程在等待 */
    if(true == filebuffers->flwsignal)
    {
        filebuffers->flwsignal = false;
        ripple_thread_cond_signal(&filebuffers->flcond);
    }

    /* 解锁 */
    ripple_thread_unlock(&filebuffers->fllock);
}

/* 将 buffer 放入待刷新队列 */
void ripple_file_buffer_waitflush_add(ripple_file_buffers* filebuffers, ripple_file_buffer* fbuffer)
{
    int iret = 0;
    /* 加锁 */
    iret = ripple_thread_lock(&filebuffers->wfllock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "add buffer 2 waitflush, lock error:%s", strerror(errno));
    }

    if(NULL == filebuffers->wflushlist)
    {
        filebuffers->wflushlist = fbuffer;
        filebuffers->wflushlist->tail = fbuffer;
    }
    else
    {
        filebuffers->wflushlist->tail->next = fbuffer;
        filebuffers->wflushlist->tail = fbuffer;
    }

    if(true == filebuffers->wflwsignal)
    {
        ripple_thread_cond_signal(&filebuffers->wflcond);
    }

    ripple_thread_unlock(&filebuffers->wfllock);
}

/* 
 * 做 copy
 *  需要关注的时 src->privdata 会设置为空
*/
void ripple_file_buffer_copy(ripple_file_buffer* src, ripple_file_buffer* dst)
{
    if(NULL == src || NULL == dst)
    {
        return;
    }

    dst->bufid = src->bufid;
    rmemcpy0(dst->data, 0, src->data, src->maxsize);
    rmemcpy1(&dst->extra, 0, &src->extra, sizeof(ripple_file_buffer_extra));
    dst->flag = src->flag;
    dst->maxsize = src->maxsize;
    dst->next = NULL;
    if(NULL != dst->privdata)
    {
        rfree(dst->privdata);
        dst->privdata = NULL;
    }
    dst->privdata = src->privdata;
    src->privdata = NULL;
    dst->start = src->start;
    dst->tail = NULL;
    dst->used = src->used;
}

/* 
 * 在待刷新缓存中获取buffer
 *     extra           0               退出
 *     extra          1               超时
 */
ripple_file_buffer* ripple_file_buffer_waitflush_get(ripple_file_buffers* filebuffers, int* timeout)
{
    int iret = 0;
    ripple_file_buffer* rfbuffer = NULL;

    /* 加锁 */
    /* 加入到链表中 */
    if(NULL != timeout)
    {
        *timeout = RIPPLE_ERROR_SUCCESS;
    }
    while(1)
    {
        iret = ripple_thread_lock(&filebuffers->wfllock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "get waitflush buffer from wbuffer, lock error:%s", strerror(errno));
            return NULL;
        }

ripple_file_buffer_waitflush_get_recheck:
        /* 是否含有数据，不含有数据，那么等待数据 */
        if(NULL == filebuffers->wflushlist)
        {
            /* 设置超时时间 */
            struct timespec ts = { 0 };
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            filebuffers->wflwsignal = true;
            iret = ripple_thread_cond_timewait(&filebuffers->wflcond, &filebuffers->wfllock, &ts);
            if(0 != iret)
            {
                if(iret == ETIMEDOUT)
                {
                    filebuffers->wflwsignal = false;
                    ripple_thread_unlock(&(filebuffers->wfllock));
                    if(NULL != timeout)
                    {
                        *timeout = RIPPLE_ERROR_TIMEOUT;
                        return NULL;
                    }
                    continue;
                }

                elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
                return NULL;
            }
            filebuffers->wflwsignal = false;
            goto ripple_file_buffer_waitflush_get_recheck;
        }

        /* 在 wflushlist 表中移除 */
        if(NULL != filebuffers->wflushlist->next)
        {
            filebuffers->wflushlist->next->tail = filebuffers->wflushlist->tail;
        }
        rfbuffer = filebuffers->wflushlist;
        filebuffers->wflushlist = filebuffers->wflushlist->next;

        /* 解锁 */
        iret = ripple_thread_unlock(&filebuffers->wfllock);
        if(0 != iret)
        {
            elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
        }
        rfbuffer->next = NULL;
        rfbuffer->tail = NULL;
        return rfbuffer;
    }

    return NULL;
}

/* 清理buffer */
void ripple_file_buffer_clean(ripple_file_buffers* filebuffers)
{
    int iret = 0;
    int index = 0;

    if(NULL == filebuffers)
    {
        return;
    }

    /* 上锁 */
    iret = ripple_thread_lock(&filebuffers->wfllock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get waitflush buffer from wbuffer, lock error:%s", strerror(errno));
    }

    filebuffers->flwsignal = false;
    filebuffers->wflwsignal = false;
    filebuffers->freelist = NULL;

    for(index = 0; index < filebuffers->maxbufid; index++)
    {
        ripple_file_buffer_reset(&filebuffers->buffers[index]);
        filebuffers->buffers[index].bufid = (index+1);
        if (NULL != filebuffers->buffers[index].data)
        {
            rfree(filebuffers->buffers[index].data);
        }
        filebuffers->buffers[index].data = NULL;
        filebuffers->buffers[index].maxsize = 0;

        if(NULL != filebuffers->freelist)
        {
            filebuffers->buffers[index].next = filebuffers->freelist;
        }
        filebuffers->freelist = &filebuffers->buffers[index];
    }
    filebuffers->wflushlist = NULL;

    /* 解锁 */
    iret = ripple_thread_unlock(&filebuffers->wfllock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }
}


/* 清理 waitflush */
void riple_file_buffer_clean_waitflush(ripple_file_buffers* filebuffers)
{
    int iret = 0;
    ripple_file_buffer* rfbuffer = NULL;
    
    iret = ripple_thread_lock(&filebuffers->wfllock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get waitflush buffer from wbuffer, lock error:%s", strerror(errno));
    }

    if(NULL == filebuffers->wflushlist)
    {
        /* 解锁 */
        iret = ripple_thread_unlock(&filebuffers->wfllock);
        if(0 != iret)
        {
            elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
        }
        return;
    }

    while(NULL != filebuffers->wflushlist)
    {
        rfbuffer = NULL;

         /* 在 wflushlist 表中移除 */
        if(NULL != filebuffers->wflushlist->next)
        {
            filebuffers->wflushlist->next->tail = filebuffers->wflushlist->tail;
        }
        rfbuffer = filebuffers->wflushlist;
        filebuffers->wflushlist = filebuffers->wflushlist->next;
        rfbuffer->next = NULL;
        rfbuffer->tail = NULL;
        ripple_file_buffer_free(filebuffers, rfbuffer);
    }

    /* 解锁 */
    iret = ripple_thread_unlock(&filebuffers->wfllock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }
    return;
}


/* 内存释放 */
void ripple_file_buffer_destroy(ripple_file_buffers* filebuffers)
{
    int index = 0;
    if(NULL == filebuffers)
    {
        return;
    }

    for(index = 0; index < filebuffers->maxbufid; index++)
    {
        if(NULL != filebuffers->buffers[index].privdata)
        {
            rfree(filebuffers->buffers[index].privdata);
            filebuffers->buffers[index].privdata = NULL;
        }

        if(NULL != filebuffers->buffers[index].data)
        {
            rfree(filebuffers->buffers[index].data);
            filebuffers->buffers[index].data = NULL;
        }
    }
    rfree(filebuffers->buffers);
    rfree(filebuffers);
    filebuffers = NULL;
}
