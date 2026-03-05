#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "net/netmsg/ripple_netmsg.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_datacmpresultitem.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"

ripple_fastcompare_datacmpresult *ripple_fastcompare_datacmpresult_init(void)
{
    ripple_fastcompare_datacmpresult *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_datacmpresult));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result , 0, 0, sizeof(ripple_fastcompare_datacmpresult));

    result->base.type = RIPPLE_NETMSG_TYPE_FASTCOMPARE_DATACMPRESULT;

    return result;
}

/* 内存释放 */
void ripple_fastcompare_datacmpresult_free(ripple_fastcompare_datacmpresult *cmpresult)
{
    ListCell *cell = NULL;
    ripple_fastcompare_datacmpresultitem *resultitem = NULL;
    if (NULL == cmpresult)
    {
        return;
    }

    if (cmpresult->checkresult)
    {
        foreach(cell, cmpresult->checkresult)
        {
            resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(cell);
            ripple_fastcompare_datacmpresultitem_free(resultitem);
        }
        list_free(cmpresult->checkresult);
    }

    if (cmpresult->corrresult)
    {
        foreach(cell, cmpresult->corrresult)
        {
            resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(cell);
            ripple_fastcompare_datacmpresultitem_free(resultitem);
        }
        list_free(cmpresult->corrresult);
    }
    
    rfree(cmpresult);
}

/* 是否还存在比较结果 */
bool ripple_fastcompare_datacmpresult_hashisnull(HTAB *result)
{
    HASH_SEQ_STATUS status;
    ripple_fastcompare_compareresulthashentry *entry = NULL;

    hash_seq_init(&status, result);

    entry = (ripple_fastcompare_compareresulthashentry *) hash_seq_search(&status);
    if (entry != NULL)
    {
        return false;
    }
    else
    {
        return true;
    }
}

/* cmpresult hash资源释放 */
void ripple_fastcompare_datacmpresult_hash_free(HTAB *result)
{
    hash_destroy(result);
}

