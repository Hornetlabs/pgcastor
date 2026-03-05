#include "ripple_app_incl.h"
#include "works/ripple_workthreadmgr.h"
#include "works/dyworks/ripple_dyworks.h"

static ripple_workthreadmgrs* m_pthrworks = NULL;

/* 增加工作线程 */
void ripple_workthreadmgr_add(int thrtype, const pthread_attr_t *attr, thrworkfunc func, void* privdata)
{
    ripple_workmgr_node* worknode = NULL;

    worknode = (ripple_workmgr_node*)rmalloc1(sizeof(ripple_workmgr_node));
    if(NULL == worknode)
    {
        elog(RLOG_ERROR, "out of memory,%s", strerror(errno));
    }
    rmemset0(worknode, 0, '\0', sizeof(ripple_workmgr_node));

    worknode->id = InvalidTHRID;
    worknode->next = NULL;
    worknode->status = RIPPLE_WORK_STATUS_INIT;
    worknode->type = thrtype;
    worknode->privdata = privdata;

    *m_pthrworks->tail = worknode;
    m_pthrworks->tail = &(worknode->next);
    m_pthrworks->thrnum++;

    /* 创建工作线程 */
    ripple_thread_create(&worknode->id, NULL, func, worknode);
}

/* 设置线程退出 */
void ripple_workthreadmgr_setstatus_term(void)
{
    ripple_workmgr_node* worknode = NULL;
    for(worknode = m_pthrworks->head; NULL != worknode; worknode = worknode->next)
    {
        if(RIPPLE_WORK_STATUS_WORK != worknode->status)
        {
            continue;
        }

        worknode->status = RIPPLE_WORK_STATUS_TERM;
        elog(RLOG_DEBUG, "set thrid:%lu term", worknode->id);
    }
}

void ripple_workthreadmgr_waitstatus(ripple_workmgr_node* worknode, ripple_work_status status)
{
    while(1)
    {
        if(status == worknode->status)
        {
            break;
        }
        usleep(50000);
    }
}

/* 
 * ripple_cache_txn_destroy检测是否有线程退出 
 * 返回值说明:
 *  false       不需要退出
 *  true        需要退出
*/
bool ripple_workthreadmgr_trydestroy(void)
{
    int iret = 0;
    ripple_workmgr_node* worknode = NULL;
    for(worknode = m_pthrworks->head; NULL != worknode; worknode = worknode->next)
    {
        if(RIPPLE_WORK_STATUS_DEAD == worknode->status)
        {
            continue;
        }

        iret = ripple_thread_tryjoin_np(worknode->id, NULL);
        if(EBUSY == iret || EINTR == iret)
        {
            /* 正常运行 */
            continue;
        }
        else if(0 == iret)
        {
            if(RIPPLE_WORK_STATUS_EXIT != worknode->status)
            {
                elog(RLOG_WARNING, "pthread:%lu abnormal, ripple exit", worknode->id);
                g_gotsigterm = true;
            }
            worknode->status = RIPPLE_WORK_STATUS_DEAD;
            m_pthrworks->threxitnum++;
        }
        else
        {
            /* 线程异常退出, ripple进程退出 */
            elog(RLOG_ERROR, "never come here, pthread:%lu, %s", worknode->id, strerror(errno));
        }
    }

    if(m_pthrworks->threxitnum == m_pthrworks->thrnum
        && true == ripple_dyworks_canexit())
    {
        return true;
    }

    return false;
}


/* 初始化线程 */
void ripple_workthreadmgr_init(void)
{
    /* 创建多个子线程工作 */
    m_pthrworks = (ripple_workthreadmgrs*)rmalloc0(sizeof(ripple_workthreadmgrs));
    if(NULL == m_pthrworks)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(m_pthrworks, 0, '\0', sizeof(ripple_workthreadmgrs));

    /* 设置初始值 */
    m_pthrworks->thrnum = 0;
    m_pthrworks->threxitnum = 0;
    m_pthrworks->head = NULL;
    m_pthrworks->tail = &(m_pthrworks->head);
}

void ripple_workthreadmgr_destroy(int status, void* arg)
{
    ripple_workmgr_node* node = NULL;

    if(NULL == m_pthrworks)
    {
        return;
    }

    for(node = m_pthrworks->head; NULL != node; node = m_pthrworks->head)
    {
        m_pthrworks->head = node->next;
        rfree(node);
    }

    rfree(m_pthrworks);
    m_pthrworks = NULL;
}

