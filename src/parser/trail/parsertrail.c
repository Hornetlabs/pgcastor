#include "app_incl.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/head/parsertrail_head.h"
#include "parser/trail/data/parsertrail_traildata.h"
#include "parser/trail/tail/parsertrail_tail.h"
#include "parser/trail/reset/parsertrail_reset.h"

typedef struct PARSERWORK_GROUPMGR
{
    int                     tokenid;
    char*                   desc;
    parsertrailtokenapplyfunc     func;
    parsertrailtokenclean         clean;
} parserwork_groupmgr;

static parserwork_groupmgr   m_groupmgr[] =
{
    {
        FFTRAIL_CXT_TYPE_NOP,
        "NOP",
        NULL,
        NULL
    },
    {
        FFTRAIL_CXT_TYPE_FHEADER,
        "FileHeader",
        parsertrail_trailheadapply,
        parsertrail_trailheadclean
    },
    {
        FFTRAIL_CXT_TYPE_DATA,
        "FileData",
        parsertrail_traildataapply,
        parsertrail_traildataclean
    },
    {
        FFTRAIL_CXT_TYPE_RESET,
        "FileReset",
        parsertrail_trailresetapply,
        parsertrail_trailresetclean
    },
    {
        FFTRAIL_CXT_TYPE_FTAIL,
        "FileTail",
        parsertrail_trailtailapply,
        parsertrail_trailtailclean
    }
};


/* 将 records 解析为事务 */
bool parsertrail_traildecode(parsertrail* parsertrail)
{
    /* 调用反序列化接口，解析数据 */
    bool    bret = true;
    uint8   tokenid = FFTRAIL_GROUPTYPE_NOP;
    uint8   tokeninfo = FFTRAIL_INFOTYPE_TOKEN;
    uint32  tokenlen = 0;

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;
    void*   result = NULL;

    uptr = parsertrail->ffsmgrstate->recptr;
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)

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
void parsertrail_reset(parsertrail* parsertrail)
{
    int mbytes              = 0;
    uint64 bytes            = 0;

    if (parsertrail->lasttxn)
    {
        txn_free(parsertrail->lasttxn);
        if (TXN_CHECK_TRANS_INHASH(parsertrail->lasttxn->flag))
        {
            rfree(parsertrail->lasttxn);
        }
        parsertrail->lasttxn = NULL;
    }

    parsertrail->ffsmgrstate->ffsmgr->ffsmgr_free(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate);

    rmemset0(parsertrail->ffsmgrstate, 0, '\0', sizeof(ffsmgr_state));
    parsertrail->ffsmgrstate->status = FFSMGR_STATUS_NOP;
    parsertrail->ffsmgrstate->bufid = 0;
    parsertrail->ffsmgrstate->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);
    parsertrail->ffsmgrstate->privdata = (void*)parsertrail;
    parsertrail->ffsmgrstate->fdata = NULL;      /* fdata->extradata ListCell, 当前在处理的cell */
                                                    /* fdata->ffdata trail 文件对应的库/表结构      */

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    bytes = MB2BYTE(mbytes);
    parsertrail->ffsmgrstate->maxbufid = (bytes/FILE_BUFFER_SIZE);

    dlist_free(parsertrail->dtxns, txn_freevoid);
    parsertrail->dtxns = NULL;

    /* 调用初始化接口 */
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, parsertrail->ffsmgrstate);
    parsertrail->ffsmgrstate->ffsmgr->ffsmgr_init(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate);
}

/* 将 records 解析为事务 */
bool parsertrail_parser(parsertrail* parsertrail)
{
    dlistnode* dlnode = NULL;
    record* record_obj = NULL;

    for(dlnode = parsertrail->records->head; NULL != dlnode; dlnode = parsertrail->records->head)
    {
        parsertrail->records->head = dlnode->next;

        record_obj = (record*)dlnode->value;
        parsertrail->ffsmgrstate->recptr = record_obj->data;
        parsertrail->ffsmgrstate->fdata->extradata = (record*)record_obj;
        /* 调用解析接口 */
        if(false == parsertrail_traildecode(parsertrail))
        {
            elog(RLOG_WARNING, "parse trail record error, record type:%d",
                                record_obj->type,
                                record_obj->start.trail.fileid,
                                record_obj->start.trail.offset,
                                record_obj->totallength,
                                record_obj->reallength);
            return false;
        }
        dlist_node_free(dlnode, record_freevoid);
    }
    dlist_free(parsertrail->records, NULL);
    parsertrail->records = NULL;
    return true;
}

/* 清理内存 */
void parsertrail_free(parsertrail* parsertrail)
{
    if (NULL == parsertrail)
    {
        return;
    }

    dlist_free(parsertrail->records, record_freevoid);
    dlist_free(parsertrail->dtxns, txn_freevoid);

    parsertrail->records = NULL;

    if (parsertrail->lasttxn)
    {
        txn_free(parsertrail->lasttxn);
        if (TXN_CHECK_TRANS_INHASH(parsertrail->lasttxn->flag))
        {
            rfree(parsertrail->lasttxn);
        }
        parsertrail->lasttxn = NULL;
    }

    /* transcache 释放 */
    transcache_free(parsertrail->transcache);
    rfree(parsertrail->transcache);
    parsertrail->transcache = NULL;

    if (parsertrail->ffsmgrstate)
    {
        if (parsertrail->ffsmgrstate->ffsmgr)
        {
            parsertrail->ffsmgrstate->ffsmgr->ffsmgr_free(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate);
        }
        rfree(parsertrail->ffsmgrstate);
    }
    parsertrail->parser2txn = NULL;

    return;
}
