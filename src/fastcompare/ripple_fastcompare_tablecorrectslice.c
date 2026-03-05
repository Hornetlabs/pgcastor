#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "net/netmsg/ripple_netmsg.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"

ripple_fastcompare_tablecorrectslice *ripple_fastcompare_tablecorrectslice_init(void)
{
    ripple_fastcompare_tablecorrectslice* correctslice = NULL;

    correctslice = (ripple_fastcompare_tablecorrectslice*)rmalloc0(sizeof(ripple_fastcompare_tablecorrectslice));
    if(NULL == correctslice)
    {
        return false;
    }
    rmemset0(correctslice, 0, '\0', sizeof(ripple_fastcompare_tablecorrectslice));

    return correctslice;
}

void ripple_fastcompare_tablecorrectslice_set_dstchunk(ripple_fastcompare_tablecorrectslice* correctslice, ripple_fastcompare_simpledatachunk* dstchunk)
{
    if (correctslice->dstchunk)
    {
        ripple_fastcompare_simpledatachunk_clean(correctslice->dstchunk);
        correctslice->dstchunk = NULL;
    }
    
    correctslice->dstchunk = dstchunk;
}

void ripple_fastcompare_tablecorrectslice_free(ripple_fastcompare_tablecorrectslice* correctslice)
{
    if(NULL == correctslice)
    {
        return;
    }

    if (correctslice->beginslice)
    {
        if (correctslice->beginslice->columns)
        {
            ripple_fastcompare_columndefine_list_clean(correctslice->beginslice->columns);
            correctslice->beginslice->columns = NULL;
        }
        ripple_fastcompare_beginslice_clean(correctslice->beginslice);
    }

    if (correctslice->compresult)
    {
        ripple_fastcompare_datacmpresult_hash_free(correctslice->compresult);
    }
    
    if (correctslice->condition)
    {
        rfree(correctslice->condition);
    }
    
    if (correctslice->dstchunk)
    {
        ripple_fastcompare_simpledatachunk_clean(correctslice->dstchunk);
    }

    rfree(correctslice);
}
