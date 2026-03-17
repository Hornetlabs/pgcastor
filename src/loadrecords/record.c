#include "app_incl.h"
#include "loadrecords/record.h"

/* record 初始化 */
record* record_init(void)
{
    record* rec = NULL;

    rec = rmalloc0(sizeof(record));
    if(NULL == rec)
    {
        return NULL;
    }
    rmemset0(rec, 0, '\0', sizeof(record));
    rec->type = RECORD_TYPE_NOP;
    return rec;
}

/* record 释放 */
void record_free(record* rec)
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
void record_freevoid(void* args)
{
    record* rec = NULL;
    if(NULL == args)
    {
        return;
    }

    rec = (record*)args;

    if(NULL != rec->data)
    {
        rfree(rec->data);
    }
    rfree(rec);
}

recordcross* recordcross_init(void)
{
    recordcross* result = NULL;

    result = rmalloc0(sizeof(recordcross));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(recordcross));

    return result;
}

void recordcross_free(recordcross* rec_cross)
{
    if (!rec_cross)
    {
        return;
    }

    if (rec_cross->record)
    {
        record_free(rec_cross->record);
    }

    rfree(rec_cross);
}
