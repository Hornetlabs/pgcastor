#include "app_incl.h"
#include "rebuild/rebuild_preparestmt.h"

/* Prepared statement initialization node */
rebuild_preparestmt* rebuild_preparestmt_init(void)
{
    rebuild_preparestmt* preparestmt = NULL;

    preparestmt = rmalloc0(sizeof(rebuild_preparestmt));
    if (NULL == preparestmt)
    {
        elog(RLOG_WARNING, "rebuild preparestmt init error");
        return NULL;
    }
    rmemset0(preparestmt, 0, '\0', sizeof(rebuild_preparestmt));
    rmemset0(preparestmt->stmtname, 0, '\0', NAMEDATALEN);
    return preparestmt;
}

/* Prepared statement comparison */
int rebuild_preparestmt_cmp(void* v1, void* v2)
{
    int                  iret = 0;
    rebuild_preparestmt* t1 = NULL;
    rebuild_preparestmt* t2 = NULL;

    t1 = (rebuild_preparestmt*)v1;
    t2 = (rebuild_preparestmt*)v2;

    iret = strcmp(t1->preparesql, t2->preparesql);
    if (0 == iret)
    {
        return 0;
    }
    else if (0 > iret)
    {
        return -1;
    }
    else
    {
        return 1;
    }

    return 0;
}

/* Prepared statement free */
void rebuild_preparestmt_free(void* argv)
{
    rebuild_preparestmt* preparestmt = NULL;

    if (NULL == argv)
    {
        return;
    }
    preparestmt = (rebuild_preparestmt*)argv;

    if (preparestmt->preparesql)
    {
        rfree(preparestmt->preparesql);
    }

    rfree(preparestmt);
}

/* Prepared statement debug */
void rebuild_preparestmt_debug(void* v1)
{
    rebuild_preparestmt* preparestmt = NULL;

    preparestmt = (rebuild_preparestmt*)v1;

    elog(RLOG_DEBUG, "preparesql:%s", preparestmt->preparesql);
    elog(RLOG_DEBUG, "     |---%lu", preparestmt->number);
    elog(RLOG_DEBUG, "     |---%s", preparestmt->stmtname);
}