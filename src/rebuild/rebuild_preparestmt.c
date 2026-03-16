#include "ripple_app_incl.h"
#include "rebuild/ripple_rebuild_preparestmt.h"

/* 预解析初始化节点 */
ripple_rebuild_preparestmt* ripple_rebuild_preparestmt_init(void)
{
    ripple_rebuild_preparestmt* preparestmt = NULL;

    preparestmt = rmalloc0(sizeof(ripple_rebuild_preparestmt));
    if(NULL == preparestmt)
    {
        elog(RLOG_WARNING, "rebuild preparestmt init error");
        return NULL;
    }
    rmemset0(preparestmt, 0, '\0', sizeof(ripple_rebuild_preparestmt));
    rmemset0(preparestmt->stmtname, 0, '\0', RIPPLE_NAMEDATALEN);
    return preparestmt;
}

/* 预解析比较 */
int ripple_rebuild_preparestmt_cmp(void* v1, void* v2)
{
    int iret = 0;
    ripple_rebuild_preparestmt* t1 = NULL;
    ripple_rebuild_preparestmt* t2 = NULL;

    t1 = (ripple_rebuild_preparestmt*)v1;
    t2 = (ripple_rebuild_preparestmt*)v2;

    iret = strcmp(t1->preparesql, t2->preparesql);
    if(0 == iret)
    {
        return 0;
    }
    else if(0 > iret)
    {
        return -1;
    }
    else
    {
        return 1;
    }

    return 0;
}

/* 预解析释放 */
void ripple_rebuild_preparestmt_free(void* argv)
{
    ripple_rebuild_preparestmt* preparestmt = NULL;

    if(NULL == argv)
    {
        return;
    }
    preparestmt = (ripple_rebuild_preparestmt*)argv;

    if (preparestmt->preparesql)
    {
        rfree(preparestmt->preparesql);
    }

    rfree(preparestmt);
}

/* 预解析调试 */
void ripple_rebuild_preparestmt_debug(void* v1)
{
    ripple_rebuild_preparestmt* preparestmt = NULL;

    preparestmt = (ripple_rebuild_preparestmt*)v1;

    elog(RLOG_DEBUG, "preparesql:%s", preparestmt->preparesql);
    elog(RLOG_DEBUG, "     |---%lu", preparestmt->number);
    elog(RLOG_DEBUG, "     |---%s", preparestmt->stmtname);
}