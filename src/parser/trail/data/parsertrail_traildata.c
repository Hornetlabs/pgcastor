#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/data/ripple_parsertrail_traildata.h"
#include "parser/trail/data/ripple_parsertrail_dbmetadata.h"
#include "parser/trail/data/ripple_parsertrail_tbmetadata.h"
#include "parser/trail/data/ripple_parsertrail_txninsert.h"
#include "parser/trail/data/ripple_parsertrail_txndelete.h"
#include "parser/trail/data/ripple_parsertrail_txnupdate.h"
#include "parser/trail/data/ripple_parsertrail_txnmultiinsert.h"
#include "parser/trail/data/ripple_parsertrail_txnddl.h"
#include "parser/trail/data/ripple_parsertrail_txncommit.h"
#include "parser/trail/data/ripple_parsertrail_txnrefresh.h"
#include "parser/trail/data/ripple_parsertrail_txnonlinerefreshend.h"
#include "parser/trail/data/ripple_parsertrail_txnonlinerefreshbegin.h"
#include "parser/trail/data/ripple_parsertrail_txnonlinerefresh_incrementend.h"
#include "parser/trail/data/ripple_parsertrail_txnbigtxnend.h"
#include "parser/trail/data/ripple_parsertrail_txnbigtxnbegin.h"
#include "parser/trail/data/ripple_parsertrail_txnonlinerefreshabandon.h"


static void ripple_parsertrail_txnsimpleclean(ripple_parsertrail* parsertrail, void* data);

typedef struct RIPPLE_PARSERTRAIL_TRAILDATAMGR
{
    int                             type;
    char*                           desc;
    parsertrailtokenapplyfunc       func;
    parsertrailtokenclean           clean;
} ripple_parsertrail_traildatamgr;

static ripple_parsertrail_traildatamgr m_typmgr[]=
{
    {
        RIPPLE_FF_DATA_TYPE_NOP,
        "NOP",
        NULL,
        NULL
    },
    {
        RIPPLE_FF_DATA_TYPE_DBMETADATA,
        "DatabaseMetaData",
        ripple_parsertrail_dbmetadataapply,
        ripple_parsertrail_dbmetadataclean
    },
    {
        RIPPLE_FF_DATA_TYPE_TBMETADATA,
        "TableMetaData",
        ripple_parsertrail_tbmetadataapply,
        ripple_parsertrail_tbmetadataclean
    },
    {
        RIPPLE_FF_DATA_TYPE_TXN,
        "Transaction Nop",
        NULL,
        NULL
    },
    {
        RIPPLE_FF_DATA_TYPE_DML_INSERT,
        "Transaction Insert",
        ripple_parsertrail_txninsertapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_DML_UPDATE,
        "Transaction Update",
        ripple_parsertrail_txnupdateapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_DML_DELETE,
        "Transaction Delete",
        ripple_parsertrail_txndeleteapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_DDL_STMT,
        "Transaction DDL STMT",
        ripple_parsertrail_txnddlapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_DDL_STRUCT,
        "Transaction DDL Struct",
        NULL,
        NULL
    },
    {
        RIPPLE_FF_DATA_TYPE_REC_CONTRECORD,
        "Record Cross",
        NULL,
        NULL
    },
    {
        RIPPLE_FF_DATA_TYPE_DML_MULTIINSERT,
        "Transaction Multiinsert",
        ripple_parsertrail_txnmultiinsertapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_TXNCOMMIT,
        "Transaction Commit",
        ripple_parsertrail_txncommitapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_REFRESH,
        "Transaction Refresh",
        ripple_parsertrail_txnrefreshapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_TXNBEGIN,
        "TXN BEGIN",
        NULL,
        NULL
    },
    {
        RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_BEGIN,
        "Transaction Onlinerefresh begin",
        ripple_parsertrail_txnonlinerefreshbeginapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_END,
        "Transaction Onlinerefresh end",
        ripple_parsertrail_txnonlinerefreshendapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END,
        "Transaction Onlinerefresh increment end",
        ripple_parsertrail_txnonlinerefreshincrementendapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_BIGTXN_BEGIN,
        "Transaction Bigtxn start",
        ripple_parsertrail_txnbigtxnbeginapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_BIGTXN_END,
        "Transaction Bigtxn end",
        ripple_parsertrail_txnbigtxnendapply,
        ripple_parsertrail_txnsimpleclean
    },
    {
        RIPPLE_FF_DATA_TYPE_ONLINEREFRESH_ABANDON,
        "Transaction Onlinerefresh Abandon",
        ripple_parsertrail_txntxnonlinerefreshabandonapply,
        ripple_parsertrail_txnsimpleclean
    }
};

static void ripple_parsertrail_txnsimpleclean(ripple_parsertrail* parsertrail, void* data)
{
    RIPPLE_UNUSED(parsertrail);

    if (data)
    {
        rfree(data);
    }
}

/*
 * 根据 subtype 的类型不同，走不同的处理逻辑
 */
bool ripple_parsertrail_traildataapply(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_data* ffdata = NULL;

    if(NULL == data)
    {
        return true;
    }

    ffdata = (ripple_ff_data*)data;

    if(NULL == m_typmgr[ffdata->subtype].func)
    {
        elog(RLOG_WARNING, "%s need apply, Specify the corresponding processing logic, %d.%d", 
                            m_typmgr[ffdata->subtype].desc, ffdata->type, ffdata->subtype);

        return false;
    }

    return m_typmgr[ffdata->subtype].func(parsertrail, data);
}

/*
 * 分发清理函数
 */
void ripple_parsertrail_traildataclean(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_data* ffdata = NULL;

    if(NULL == data)
    {
        return;
    }

    ffdata = (ripple_ff_data*)data;

    /* 清理 */
    if (m_typmgr[ffdata->subtype].clean)
    {
        m_typmgr[ffdata->subtype].clean(parsertrail, data);
    }
}

/*
 * 文件切换处理
 */
void ripple_parsertrail_traildata_shiftfile(ripple_parsertrail* parsertrail)
{
    ripple_fftrail_privdata* privdata = NULL;

    /* 交换 */
    parsertrail->ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_USED;
    ripple_fftrail_invalidprivdata(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);
    privdata = parsertrail->ffsmgrstate->fdata->ffdata;
    parsertrail->ffsmgrstate->fdata->ffdata = parsertrail->ffsmgrstate->fdata->ffdata2;
    parsertrail->ffsmgrstate->fdata->ffdata2 = privdata;

    return ;
}
