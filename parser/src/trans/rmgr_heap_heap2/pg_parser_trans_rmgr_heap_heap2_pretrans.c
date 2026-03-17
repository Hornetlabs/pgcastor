#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_heap_heap2/pg_parser_trans_rmgr_heap_heap2.h"

#define PRE_RMGR_HEAP_MCXT NULL
#define PRE_RMGR_HEAP2_MCXT NULL

#define PG_PARSER_RMGR_HEAP_INFOCNT 5
#define PG_PARSER_RMGR_HEAP2_INFOCNT 1

#define PG_PARSER_TRANS_RMGR_HEAP_DELETE_MAINSZ 8
#define PG_PARSER_TRANS_RMGR_HEAP_UPDATE_MAINSZ 14

typedef bool (*pg_parser_trans_transrec_rmgr_info_func_pre)(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno);

typedef struct PG_PARSER_TRANS_RMGR_HEAP
{
    pg_parser_trans_rmgr_heap_info                    m_infoid;             /* info值 */
    pg_parser_trans_transrec_rmgr_info_func_pre       m_infofunc_pre;       /* 预解析接口info级的处理函数 */
} pg_parser_trans_rmgr_heap;

typedef struct PG_PARSER_TRANS_RMGR_HEAP2
{
    pg_parser_trans_rmgr_heap2_info                   m_infoid;             /* info值 */
    pg_parser_trans_transrec_rmgr_info_func_pre       m_infofunc_pre;       /* 预解析接口info级的处理函数 */
} pg_parser_trans_rmgr_heap2;

static bool pg_parser_trans_rmgr_heap_insert_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_update_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_hotupdate_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_delete_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno);

static bool pg_parser_trans_rmgr_heap2_minsert_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno);


static bool pg_parser_trans_rmgr_heap_truncate(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                  pg_parser_translog_pre_base **result,
                                                  int32_t *pg_parser_errno);

#if 0
static bool pg_parser_trans_rmgr_heap_inplace(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                 pg_parser_translog_pre_base **result,
                                                 int32_t *pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_confirm(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                 pg_parser_translog_pre_base **result,
                                                 int32_t *pg_parser_errno);
#endif

static pg_parser_trans_rmgr_heap2 m_record_rmgr_heap2_info[] =
{
    { PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT,
                    pg_parser_trans_rmgr_heap2_minsert_pre}
};

static pg_parser_trans_rmgr_heap m_record_rmgr_heap_info[] =
{
    { PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT, pg_parser_trans_rmgr_heap_insert_pre},
    { PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE, pg_parser_trans_rmgr_heap_delete_pre},
    { PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE, pg_parser_trans_rmgr_heap_update_pre},
    { PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_TRUNCATE, pg_parser_trans_rmgr_heap_truncate},
    { PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE, pg_parser_trans_rmgr_heap_hotupdate_pre}
};

bool pg_parser_trans_rmgr_heap_pre(pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = PG_PARSER_RMGR_HEAP_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap_info[index].m_infofunc_pre(state, result, pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;
}

bool pg_parser_trans_rmgr_heap2_pre(pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = PG_PARSER_RMGR_HEAP2_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap2_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap2_info[index].m_infofunc_pre(state, result, pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;
}

static bool pg_parser_trans_rmgr_heap_insert_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_heap * heap = NULL;

    /* 检查出入参的合法性 */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_INSERT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap insert], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&heap),
                                  sizeof(pg_parser_translog_pre_heap)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_01;
        return false;
    }
    heap->m_base.m_type = PG_PARSER_TRANSLOG_HEAP_INSERT;
    heap->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    /* insert 语句不需要tuple数据 */
    heap->m_needtuple = false;
    heap->m_tuplecnts = 0;
    heap->m_tupitemoff = 0;
    heap->m_pagenos = 0;

    heap->m_base.m_originid = state->record_origin;

    /* 检查是否有block */
    if (0 > state->max_block_id)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_INSERT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap insert], but record block id < 0\n");
        return false;
    }
    /* 检查block0的relfilenode是否有效 */
    if ( 0 == state->blocks[0].rnode.relNode)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_INSERT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap insert], but relfilenode invalid\n");
        return false;
    }
    heap->m_relfilenode = state->blocks[0].rnode.relNode;
    heap->m_dboid = state->blocks[0].rnode.dbNode;
    heap->m_tbspcoid = state->blocks[0].rnode.spcNode;
    heap->m_transid = state->decoded_record->xl_xid;
    *result = (pg_parser_translog_pre_base *) heap;

    return true;
}

static bool pg_parser_trans_rmgr_heap2_minsert_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_heap * heap = NULL;

    /* 检查出入参的合法性 */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP2_MINSERT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap2 multi_insert], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP2_MCXT,
                                 (void **) (&heap),
                                  sizeof(pg_parser_translog_pre_heap)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_05;
        return false;
    }
    heap->m_base.m_type = PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT;
    heap->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    /* mutli insert 语句不需要tuple数据 */
    heap->m_needtuple = false;
    heap->m_tuplecnts = 0;
    heap->m_tupitemoff = 0;
    heap->m_pagenos = 0;

    heap->m_base.m_originid = state->record_origin;

    /* 检查是否有block */
    if (0 > state->max_block_id)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP2_MINSERT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap2 multi_insert], invalid block\n");
        return false;
    }
    /* 检查block0的relfilenode是否有效 */
    if ( 0 == state->blocks[0].rnode.relNode)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP2_MINSERT_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap2 multi_insert], invalid refilenode\n");
        return false;
    }
    heap->m_relfilenode = state->blocks[0].rnode.relNode;
    heap->m_dboid = state->blocks[0].rnode.dbNode;
    heap->m_tbspcoid = state->blocks[0].rnode.spcNode;
    heap->m_transid = state->decoded_record->xl_xid;
    *result = (pg_parser_translog_pre_base *) heap;
    return true;
}


static bool pg_parser_trans_rmgr_heap_delete_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_heap * heap = NULL;
    uint32_t page_no = 0;

    /* 检查出入参的合法性 */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_DELETE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap delete], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&heap),
                                  sizeof(pg_parser_translog_pre_heap)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_04;
        return false;
    }
    heap->m_base.m_type = PG_PARSER_TRANSLOG_HEAP_DELETE;
    heap->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    /* 检查是否有block */
    if (0 > state->max_block_id)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_DELETE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap delete], invalid block\n");
        return false;
    }

    /* 
     * delete语句在replica级别下需要tuple数据 
     * 在logical级别下, 普通表的delete语句会在
     * 系统表需要tuple数据
     * delete语句系统表的main_data_len只有8
     */
    if (state->pre_trans_data->m_walLevel == PG_PARSER_WALLEVEL_LOGICAL
        && state->main_data_len > PG_PARSER_TRANS_RMGR_HEAP_DELETE_MAINSZ)
    {
        heap->m_needtuple = false;
        heap->m_tuplecnts = 0;
        heap->m_tupitemoff = 0;
        heap->m_pagenos = 0;
    }
    else
    {
        /* 首先检查是否有全页写 */
        if (state->blocks[0].has_image && 0 == state->max_block_id)
        {
            heap->m_needtuple = false;
            heap->m_tuplecnts = 0;
            heap->m_tupitemoff = 0;
            heap->m_pagenos = 0;
        }
        /* 没有全页写, 我们需要请求tuple数据 */
        else
        {
            heap->m_needtuple = true;
            heap->m_tuplecnts = 1;
            page_no = state->blocks[0].blkno;
            heap->m_tupitemoff = ((pg_parser_xl_heap_delete *)state->main_data)->offnum;
            heap->m_pagenos = page_no;
        }
    }

    heap->m_base.m_originid = state->record_origin;

    /* 检查block0的relfilenode是否有效 */
    if ( 0 == state->blocks[0].rnode.relNode)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_DELETE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap delete], invalid relfilenode\n");
        return false;
    }
    heap->m_relfilenode = state->blocks[0].rnode.relNode;
    heap->m_dboid = state->blocks[0].rnode.dbNode;
    heap->m_tbspcoid = state->blocks[0].rnode.spcNode;
    heap->m_transid = state->decoded_record->xl_xid;
    *result = (pg_parser_translog_pre_base *) heap;

    return true;
}

static bool pg_parser_trans_rmgr_heap_update_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_heap * heap = NULL;
    uint32_t page_no = 0;

    /* 检查出入参的合法性 */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_UPDATE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap update], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&heap),
                                  sizeof(pg_parser_translog_pre_heap)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_02;
        return false;
    }
    heap->m_base.m_type = PG_PARSER_TRANSLOG_HEAP_UPDATE;
    heap->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    /* 检查是否有block */
    if (0 > state->max_block_id)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_UPDATE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap update], invalid block\n");
        return false;
    }

    /* 
     * update语句在replica级别下需要全页写数据 
     * 在logical级别下, 普通表的delete语句会在
     * 系统表需要全页写数据
     * update语句系统表的main_data_len只有14
     */
    if (state->pre_trans_data->m_walLevel == PG_PARSER_WALLEVEL_LOGICAL
        && state->main_data_len > PG_PARSER_TRANS_RMGR_HEAP_UPDATE_MAINSZ)
    {
        heap->m_needtuple = false;
        heap->m_tuplecnts = 0;
        heap->m_tupitemoff = 0;
        heap->m_pagenos = 0;
    }
    else
    {
        /* update 我们只需要old tuple的全页写数据, 首先检查是否有全页写 */
        if (state->blocks[0].has_image && 0 == state->max_block_id)
        {
            heap->m_needtuple = false;
            heap->m_tuplecnts = 0;
            heap->m_tupitemoff = 0;
            heap->m_pagenos = 0;
        }
        /* 没有全页写, 我们需要请求全页写数据 */
        else
        {
            heap->m_needtuple = true;
            heap->m_tuplecnts = 1;
            heap->m_tupitemoff = ((pg_parser_xl_heap_update *)state->main_data)->old_offnum;
            if (0 == state->max_block_id)
                page_no = state->blocks[0].blkno;
            else
                page_no = state->blocks[1].blkno;
            heap->m_pagenos = page_no;
        }
    }

    heap->m_base.m_originid = state->record_origin;

    /* 检查block0的relfilenode是否有效 */
    if ( 0 == state->blocks[0].rnode.relNode)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_UPDATE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap update], invalid relfilenode\n");
        return false;
    }
    heap->m_relfilenode = state->blocks[0].rnode.relNode;
    heap->m_dboid = state->blocks[0].rnode.dbNode;
    heap->m_tbspcoid = state->blocks[0].rnode.spcNode;
    heap->m_transid = state->decoded_record->xl_xid;
    *result = (pg_parser_translog_pre_base *) heap;

    return true;
}

static bool pg_parser_trans_rmgr_heap_hotupdate_pre(
                            pg_parser_trans_transrec_decode_XLogReaderState *state,
                            pg_parser_translog_pre_base **result,
                            int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_heap * heap = NULL;
    uint32_t page_no = 0;

    /* 检查出入参的合法性 */
    if (!result || !state || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_HUPDATE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap hot_update], invalid param\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&heap),
                                  sizeof(pg_parser_translog_pre_heap)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_03;
        return false;
    }
    heap->m_base.m_type = PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE;
    heap->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    /* 检查是否有block */
    if (0 > state->max_block_id)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_HUPDATE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap hot_update], invalid block\n");
        return false;
    }

    /* 
     * update语句在replica级别下需要全页写数据 
     * 在logical级别下, 普通表的delete语句会在
     * 系统表需要全页写数据
     * update语句系统表的main_data_len只有14
     */
    if (state->pre_trans_data->m_walLevel == PG_PARSER_WALLEVEL_LOGICAL
        && state->main_data_len > PG_PARSER_TRANS_RMGR_HEAP_UPDATE_MAINSZ)
    {
        heap->m_needtuple = false;
        heap->m_tuplecnts = 0;
        heap->m_tupitemoff = 0;
        heap->m_pagenos = 0;
    }
    else
    {
        /* update 我们只需要old tuple的全页写数据, 首先检查是否有全页写 */
        if (state->blocks[0].has_image && 0 == state->max_block_id)
        {
            heap->m_needtuple = false;
            heap->m_tuplecnts = 0;
            heap->m_tupitemoff = 0;
            heap->m_pagenos = 0;
        }
        /* 没有全页写, 我们需要请求全页写数据 */
        else
        {
            heap->m_needtuple = true;
            heap->m_tuplecnts = 1;
            heap->m_tupitemoff = ((pg_parser_xl_heap_update *)state->main_data)->old_offnum;
            if (0 == state->max_block_id)
                page_no = state->blocks[0].blkno;
            else
                page_no = state->blocks[1].blkno;
            heap->m_pagenos = page_no;
        }
    }

    heap->m_base.m_originid = state->record_origin;

    /* 检查block0的relfilenode是否有效 */
    if ( 0 == state->blocks[0].rnode.relNode)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_HEAP_HUPDATE_CHECK;
        pg_parser_log_errlog(state->pre_trans_data->m_debugLevel,
                                "ERROR: pre record is [heap hot_update], invalid relfilenode\n");
        return false;
    }
    heap->m_relfilenode = state->blocks[0].rnode.relNode;
    heap->m_dboid = state->blocks[0].rnode.dbNode;
    heap->m_tbspcoid = state->blocks[0].rnode.spcNode;
    heap->m_transid = state->decoded_record->xl_xid;
    *result = (pg_parser_translog_pre_base *) heap;

    return true;
}

typedef struct pg_parser_xl_heap_truncate
{
    uint32_t    dbId;
    uint32_t    nrelids;
    uint8_t     flags;
    uint32_t    relids[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_heap_truncate;

#define PG_PARSER_XLH_TRUNCATE_CASCADE (1<<0)
#define PG_PARSER_XLH_RESTART_SEQS (1<<1)


static bool pg_parser_trans_rmgr_heap_truncate(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                  pg_parser_translog_pre_base **result,
                                                  int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_heap_truncate *trans = NULL;
    pg_parser_xl_heap_truncate *xlrec = NULL;
    xlrec = (pg_parser_xl_heap_truncate *) pg_parser_XLogRecGetData(state);

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&trans),
                                  sizeof(pg_parser_translog_pre_heap_truncate)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_base.m_type = PG_PARSER_TRANSLOG_HEAP_TRUNCATE;
    trans->m_base.m_xid = pg_parser_XLogRecGetXid(state);
    trans->m_base.m_originid = state->record_origin;

    trans->dbid = xlrec->dbId;
    trans->cascade = xlrec->flags & PG_PARSER_XLH_TRUNCATE_CASCADE;
    trans->reseq = xlrec->flags & PG_PARSER_XLH_RESTART_SEQS;
    trans->nrelids = xlrec->nrelids;

    pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                            (void**)&trans->relids,
                             sizeof(uint32_t) * trans->nrelids);

    rmemcpy0(trans->relids, 0, xlrec->relids, sizeof(uint32_t) * trans->nrelids);

    *result = (pg_parser_translog_pre_base *)trans;
    return true;
}

#if 0
static bool pg_parser_trans_rmgr_heap_inplace(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                 pg_parser_translog_pre_base **result,
                                                 int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_base *trans = NULL;

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&trans),
                                  sizeof(pg_parser_translog_pre_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_type = PG_PARSER_TRANSLOG_HEAP_INPLACE;
    trans->m_xid = pg_parser_XLogRecGetXid(state);
    *result = trans;
    return true;
}

static bool pg_parser_trans_rmgr_heap_confirm(pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                 pg_parser_translog_pre_base **result,
                                                 int32_t *pg_parser_errno)
{
    pg_parser_translog_pre_base *trans = NULL;

    if (!pg_parser_mcxt_malloc(PRE_RMGR_HEAP_MCXT,
                                 (void **) (&trans),
                                  sizeof(pg_parser_translog_pre_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_06;
        return false;
    }
    trans->m_type = PG_PARSER_TRANSLOG_HEAP_CONFIRM;
    trans->m_xid = pg_parser_XLogRecGetXid(state);
    *result = trans;
    return true;
}
#endif
