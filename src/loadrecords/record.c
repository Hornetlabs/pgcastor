#include "ripple_app_incl.h"
#include "loadrecords/ripple_record.h"

/* record 初始化 */
ripple_record* ripple_record_init(void)
{
    ripple_record* rec = NULL;

    rec = rmalloc0(sizeof(ripple_record));
    if(NULL == rec)
    {
        return NULL;
    }
    rmemset0(rec, 0, '\0', sizeof(ripple_record));
    rec->type = RIPPLE_RECORD_TYPE_NOP;
    return rec;
}

/* record 释放 */
void ripple_record_free(ripple_record* rec)
{
    if(NULL == rec)
    {
        return;
    }

    if(NULL != rec->data)
    {
        rfree(rec->data);
    }
    rfree(rec);
}

/* record 释放 */
void ripple_record_freevoid(void* args)
{
    ripple_record* rec = NULL;
    if(NULL == args)
    {
        return;
    }

    rec = (ripple_record*)args;

    if(NULL != rec->data)
    {
        rfree(rec->data);
    }
    rfree(rec);
}

ripple_recordcross* ripple_recordcross_init(void)
{
    ripple_recordcross* result = NULL;

    result = rmalloc0(sizeof(ripple_recordcross));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_recordcross));

    return result;
}

void ripple_recordcross_free(ripple_recordcross* rec_cross)
{
    if (!rec_cross)
    {
        return;
    }

    if (rec_cross->record)
    {
        ripple_record_free(rec_cross->record);
    }

    rfree(rec_cross);
}
