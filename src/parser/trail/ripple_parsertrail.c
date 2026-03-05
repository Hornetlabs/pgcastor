#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "loadrecords/ripple_record.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/head/ripple_parsertrail_head.h"
#include "parser/trail/data/ripple_parsertrail_traildata.h"
#include "parser/trail/tail/ripple_parsertrail_tail.h"
#include "parser/trail/reset/ripple_parsertrail_reset.h"

typedef struct RIPPLE_PARSERWORK_GROUPMGR
{
    int                     tokenid;
    char*                   desc;
    parsertrailtokenapplyfunc     func;
    parsertrailtokenclean         clean;
} ripple_parserwork_groupmgr;

static ripple_parserwork_groupmgr   m_groupmgr[] =
{
    {
        RIPPLE_FFTRAIL_CXT_TYPE_NOP,
        "NOP",
        NULL,
        NULL
    },
    {
        RIPPLE_FFTRAIL_CXT_TYPE_FHEADER,
        "FileHeader",
        ripple_parsertrail_trailheadapply,
        ripple_parsertrail_trailheadclean
    },
    {
        RIPPLE_FFTRAIL_CXT_TYPE_DATA,
        "FileData",
        ripple_parsertrail_traildataapply,
        ripple_parsertrail_traildataclean
    },
    {
        RIPPLE_FFTRAIL_CXT_TYPE_RESET,
        "FileReset",
        ripple_parsertrail_trailresetapply,
        ripple_parsertrail_trailresetclean
    },
    {
        RIPPLE_FFTRAIL_CXT_TYPE_FTAIL,
        "FileTail",
        ripple_parsertrail_trailtailapply,
        ripple_parsertrail_trailtailclean
    }
};


/* 将 records 解析为事务 */
bool ripple_parsertrail_traildecode(ripple_parsertrail* parsertrail)
{
    /* 调用反序列化接口，解析数据 */
    bool    bret = true;
    uint8   tokenid = RIPPLE_FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = RIPPLE_FFTRAIL_INFOTYPE_TOKEN;
    uint32  tokenlen = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;
    void*   result = NULL;

    uptr = parsertrail->ffsmgrstate->recptr;
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)

    /* 调用反序列化接口 */
    bret = parsertrail->ffsmgrstate->ffsmgr->ffsmgr_deserial(tokenid, &result, parsertrail->ffsmgrstate);
    if(false == bret)
    {
        /* 接收到退出信号 */
        elog(RLOG_WARNING, "parse trail record error");
        return false;
    }

    /* 根据出参的类型做不同的处理 */
    if(NULL == m_groupmgr[tokenid].func)
    {
        /* make gcc happy */
        tokenid = tokeninfo;
        uptr = tokendata;
        elog(RLOG_WARNING, "%s need apply, Specify the corresponding processing logic", m_groupmgr[tokenid].desc);
        return false;
    }

    /* 根据类型调用不同处理接口 */
    bret = m_groupmgr[tokenid].func(parsertrail, result);

    if (m_groupmgr[tokenid].clean)
    {
        m_groupmgr[tokenid].clean(parsertrail, result);
    }
    return bret;
}

/* 清理内存,并重置内容 */
void ripple_parsertrail_reset(ripple_parsertrail* parsertrail)
{
    int mbytes              = 0;
    uint64 bytes            = 0;

    if (parsertrail->lasttxn)
    {
        ripple_txn_free(parsertrail->lasttxn);
        if (RIPPLE_TXN_CHECK_TRANS_INHASH(parsertrail->lasttxn->flag))
        {
            rfree(parsertrail->lasttxn);
        }
        parsertrail->lasttxn = NULL;
    }

    parsertrail->ffsmgrstate->ffsmgr->ffsmgr_free(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate);

    rmemset0(parsertrail->ffsmgrstate, 0, '\0', sizeof(ripple_ffsmgr_state));
    parsertrail->ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
    parsertrail->ffsmgrstate->bufid = 0;
    parsertrail->ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);
    parsertrail->ffsmgrstate->privdata = (void*)parsertrail;
    parsertrail->ffsmgrstate->fdata = NULL;      /* fdata->extradata ListCell, 当前在处理的cell */
                                                    /* fdata->ffdata trail 文件对应的库/表结构      */

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    parsertrail->ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

    dlist_free(parsertrail->dtxns, ripple_txn_freevoid);
    parsertrail->dtxns = NULL;

    /* 调用初始化接口 */
    ripple_ffsmgr_init(RIPPLE_FFSMG_IF_TYPE_TRAIL, parsertrail->ffsmgrstate);
    parsertrail->ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate);
}

/* 将 records 解析为事务 */
bool ripple_parsertrail_parser(ripple_parsertrail* parsertrail)
{
    dlistnode* dlnode = NULL;
    ripple_record* record = NULL;

    for(dlnode = parsertrail->records->head; NULL != dlnode; dlnode = parsertrail->records->head)
    {
        parsertrail->records->head = dlnode->next;

        record = (ripple_record*)dlnode->value;
        parsertrail->ffsmgrstate->recptr = record->data;
        parsertrail->ffsmgrstate->fdata->extradata = (ripple_record*)record;
        /* 调用解析接口 */
        if(false == ripple_parsertrail_traildecode(parsertrail))
        {
            elog(RLOG_WARNING, "parse trail record error, record type:%d",
                                record->type,
                                record->start.trail.fileid,
                                record->start.trail.offset,
                                record->totallength,
                                record->reallength);
            return false;
        }
        dlist_node_free(dlnode, ripple_record_freevoid);
    }
    dlist_free(parsertrail->records, NULL);
    parsertrail->records = NULL;
    return true;
}

/* 清理内存 */
void ripple_parsertrail_free(ripple_parsertrail* parsertrail)
{
    if (NULL == parsertrail)
    {
        return;
    }

    dlist_free(parsertrail->records, ripple_record_freevoid);
    dlist_free(parsertrail->dtxns, ripple_txn_freevoid);

    parsertrail->records = NULL;

    if (parsertrail->lasttxn)
    {
        ripple_txn_free(parsertrail->lasttxn);
        if (RIPPLE_TXN_CHECK_TRANS_INHASH(parsertrail->lasttxn->flag))
        {
            rfree(parsertrail->lasttxn);
        }
        parsertrail->lasttxn = NULL;
    }

    /* transcache 释放 */
    ripple_transcache_free(parsertrail->transcache);
    rfree(parsertrail->transcache);
    parsertrail->transcache = NULL;

    if (parsertrail->ffsmgrstate)
    {
        if (parsertrail->ffsmgrstate->ffsmgr)
        {
            parsertrail->ffsmgrstate->ffsmgr->ffsmgr_free(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate);
        }
        rfree(parsertrail->ffsmgrstate);
    }
    parsertrail->parser2txn = NULL;

    return;
}
