#include "app_incl.h"
#include "utils/list/list_func.h"
#include "storage/ff_detail.h"

/* primarykey/uniquekey 初始化函数 */
ff_tbindex* ff_tbindex_init(int type, uint32_t keynum)
{
    ff_tbindex* result = NULL;

    result = rmalloc0(sizeof(ff_tbindex));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ff_tbindex));

    result->index_type = type;
    result->index_key_num = keynum;
    result->index_key = NULL;

    return result;
}

/* ff_tbindex 释放函数 */
void ff_tbindex_free(ff_tbindex* index)
{

    if (index)
    {
        if (index->index_key)
        {
            rfree(index->index_key);
        }
        rfree(index);
    }
}

static void ff_tbindex_list_free(List* index_list)
{
    ListCell *cell = NULL;
    foreach(cell, index_list)
    {
        ff_tbindex* index = (ff_tbindex*)lfirst(cell);
        if (index)
        {
            ff_tbindex_free(index);
        }
    }
    list_free(index_list);
}

/* ff_tbmetadata 释放函数 */
void ff_tbmetadata_free(ff_tbmetadata *data)
{
    /* 内存释放 */
    if(NULL != data->table)
    {
        rfree(data->table);
    }

    if(NULL != data->schema)
    {
        rfree(data->schema);
    }

    if (NULL != data->index)
    {
        ff_tbindex_list_free(data->index);
    }

    rfree(data);
}
