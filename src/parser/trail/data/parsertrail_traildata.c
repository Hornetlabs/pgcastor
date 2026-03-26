#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_traildata.h"
#include "parser/trail/data/parsertrail_dbmetadata.h"
#include "parser/trail/data/parsertrail_tbmetadata.h"
#include "parser/trail/data/parsertrail_txninsert.h"
#include "parser/trail/data/parsertrail_txndelete.h"
#include "parser/trail/data/parsertrail_txnupdate.h"
#include "parser/trail/data/parsertrail_txnmultiinsert.h"
#include "parser/trail/data/parsertrail_txnddl.h"
#include "parser/trail/data/parsertrail_txncommit.h"
#include "parser/trail/data/parsertrail_txnrefresh.h"
#include "parser/trail/data/parsertrail_txnonlinerefreshend.h"
#include "parser/trail/data/parsertrail_txnonlinerefreshbegin.h"
#include "parser/trail/data/parsertrail_txnonlinerefresh_incrementend.h"
#include "parser/trail/data/parsertrail_txnbigtxnend.h"
#include "parser/trail/data/parsertrail_txnbigtxnbegin.h"
#include "parser/trail/data/parsertrail_txnonlinerefreshabandon.h"

static void parsertrail_txnsimpleclean(parsertrail* parsertrail, void* data);

typedef struct PARSERTRAIL_TRAILDATAMGR
{
    int                       type;
    char*                     desc;
    parsertrailtokenapplyfunc func;
    parsertrailtokenclean     clean;
} parsertrail_traildatamgr;

static parsertrail_traildatamgr m_typmgr[] = {
    {FF_DATA_TYPE_NOP, "NOP", NULL, NULL},
    {FF_DATA_TYPE_DBMETADATA,
     "DatabaseMetaData",
     parsertrail_dbmetadataapply,
     parsertrail_dbmetadataclean},
    {FF_DATA_TYPE_TBMETADATA,
     "TableMetaData",
     parsertrail_tbmetadataapply,
     parsertrail_tbmetadataclean},
    {FF_DATA_TYPE_TXN, "Transaction Nop", NULL, NULL},
    {FF_DATA_TYPE_DML_INSERT,
     "Transaction Insert",
     parsertrail_txninsertapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_DML_UPDATE,
     "Transaction Update",
     parsertrail_txnupdateapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_DML_DELETE,
     "Transaction Delete",
     parsertrail_txndeleteapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_DDL_STMT,
     "Transaction DDL STMT",
     parsertrail_txnddlapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_DDL_STRUCT, "Transaction DDL Struct", NULL, NULL},
    {FF_DATA_TYPE_REC_CONTRECORD, "Record Cross", NULL, NULL},
    {FF_DATA_TYPE_DML_MULTIINSERT,
     "Transaction Multiinsert",
     parsertrail_txnmultiinsertapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_TXNCOMMIT,
     "Transaction Commit",
     parsertrail_txncommitapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_REFRESH,
     "Transaction Refresh",
     parsertrail_txnrefreshapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_TXNBEGIN, "TXN BEGIN", NULL, NULL},
    {FF_DATA_TYPE_ONLINE_REFRESH_BEGIN,
     "Transaction Onlinerefresh begin",
     parsertrail_txnonlinerefreshbeginapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_ONLINE_REFRESH_END,
     "Transaction Onlinerefresh end",
     parsertrail_txnonlinerefreshendapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END,
     "Transaction Onlinerefresh increment end",
     parsertrail_txnonlinerefreshincrementendapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_BIGTXN_BEGIN,
     "Transaction Bigtxn start",
     parsertrail_txnbigtxnbeginapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_BIGTXN_END,
     "Transaction Bigtxn end",
     parsertrail_txnbigtxnendapply,
     parsertrail_txnsimpleclean},
    {FF_DATA_TYPE_ONLINEREFRESH_ABANDON,
     "Transaction Onlinerefresh Abandon",
     parsertrail_txntxnonlinerefreshabandonapply,
     parsertrail_txnsimpleclean}};

static void parsertrail_txnsimpleclean(parsertrail* parsertrail, void* data)
{
    UNUSED(parsertrail);

    if (data)
    {
        rfree(data);
    }
}

/*
 * Different processing logic based on subtype type
 */
bool parsertrail_traildataapply(parsertrail* parsertrail, void* data)
{
    ff_data* ffdata = NULL;

    if (NULL == data)
    {
        return true;
    }

    ffdata = (ff_data*)data;

    if (NULL == m_typmgr[ffdata->subtype].func)
    {
        elog(RLOG_WARNING,
             "%s need apply, Specify the corresponding processing logic, %d.%d",
             m_typmgr[ffdata->subtype].desc,
             ffdata->type,
             ffdata->subtype);

        return false;
    }

    return m_typmgr[ffdata->subtype].func(parsertrail, data);
}

/*
 * Dispatch cleanup function
 */
void parsertrail_traildataclean(parsertrail* parsertrail, void* data)
{
    ff_data* ffdata = NULL;

    if (NULL == data)
    {
        return;
    }

    ffdata = (ff_data*)data;

    /* Cleanup */
    if (m_typmgr[ffdata->subtype].clean)
    {
        m_typmgr[ffdata->subtype].clean(parsertrail, data);
    }
}

/*
 * File switch handling
 */
void parsertrail_traildata_shiftfile(parsertrail* parsertrail)
{
    fftrail_privdata* privdata = NULL;

    /* Swap */
    parsertrail->ffsmgrstate->status = FFSMGR_STATUS_USED;
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;
    parsertrail->ffsmgrstate->fdata->ffdata = parsertrail->ffsmgrstate->fdata->ffdata2;
    parsertrail->ffsmgrstate->fdata->ffdata2 = privdata;

    return;
}
