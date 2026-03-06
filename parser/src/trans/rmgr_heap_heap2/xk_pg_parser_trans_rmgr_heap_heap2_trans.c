#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_heap_heap2/xk_pg_parser_trans_rmgr_heap_heap2.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_itemptr.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_heaptuple.h"
#include "image/xk_pg_parser_image.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_fmgr.h"

#define XK_PG_PARSER_RMGR_HEAP_INFOCNT 4
#define XK_PG_PARSER_RMGR_HEAP2_INFOCNT 1

#define XK_PG_PARSER_RMGR_HEAP_GET_TUPLE_INFOCNT 4
#define XK_PG_PARSER_RMGR_HEAP2_GET_TUPLE_INFOCNT 1

#define TRANS_RMGR_HEAP_MCXT NULL


typedef bool (*xk_pg_parser_trans_transrec_rmgr_info_func_trans)(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno);

typedef struct XK_PG_PARSER_TRANS_RMGR_HEAP
{
    xk_pg_parser_trans_rmgr_heap_info                    m_infoid;             /* info值 */
    xk_pg_parser_trans_transrec_rmgr_info_func_trans     m_infofunc_trans;     /* 二次解析时info级的处理函数 */
} xk_pg_parser_trans_rmgr_heap;

typedef struct XK_PG_PARSER_TRANS_RMGR_HEAP2
{
    xk_pg_parser_trans_rmgr_heap2_info                   m_infoid;             /* info值 */
    xk_pg_parser_trans_transrec_rmgr_info_func_trans     m_infofunc_trans;     /* 二次解析时info级的处理函数 */
} xk_pg_parser_trans_rmgr_heap2;

typedef struct XK_PG_PARSER_TRANS_RMGR_HEAP_GET_TUPLE
{
    xk_pg_parser_trans_rmgr_heap_info                    m_infoid;             /* info值 */
    xk_pg_parser_trans_transrec_rmgr_info_func_trans     m_infofunc_trans;     /* 二次解析时info级的处理函数 */
} xk_pg_parser_trans_rmgr_heap_get_tuple;

typedef struct XK_PG_PARSER_TRANS_RMGR_HEAP2_GET_TUPLE
{
    xk_pg_parser_trans_rmgr_heap2_info                   m_infoid;             /* info值 */
    xk_pg_parser_trans_transrec_rmgr_info_func_trans     m_infofunc_trans;     /* 二次解析时info级的处理函数 */
} xk_pg_parser_trans_rmgr_heap2_get_tuple;

/* static statement */
static bool xk_pg_parser_trans_rmgr_heap_insert_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap2_minsert_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap_delete_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap_update_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap_insert_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap_delete_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap2_minsert_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool xk_pg_parser_trans_rmgr_heap_update_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno);

static bool reassemble_tuplenew_from_wal_data(char *data,
                                              size_t len,
                                              xk_pg_parser_ReorderBufferTupleBuf **result_new,
                                              xk_pg_parser_xl_heap_update *xlrec,
                                              uint32_t blknum_new,
                                              xk_pg_parser_TransactionId xid,
                                              xk_pg_parser_HeapTupleHeader htup_old,
                                              int32_t old_tuple_len,
                                              int16_t dbtype,
                                              char *dbversion);

static void reassemble_mutituple_from_wal_data(char *data, size_t len,
                                   xk_pg_parser_ReorderBufferTupleBuf *tuple,
                                   xk_pg_parser_xl_multi_insert_tuple *xlhdr,
                                   int16_t dbtype,
                                   char *dbversion);

static xk_pg_parser_trans_rmgr_heap2 m_record_rmgr_heap2_info[] =
{
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT, xk_pg_parser_trans_rmgr_heap2_minsert_trans}
};

static xk_pg_parser_trans_rmgr_heap m_record_rmgr_heap_info[] =
{
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT, xk_pg_parser_trans_rmgr_heap_insert_trans},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE, xk_pg_parser_trans_rmgr_heap_delete_trans},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE, xk_pg_parser_trans_rmgr_heap_update_trans},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE, xk_pg_parser_trans_rmgr_heap_update_trans}
};

static xk_pg_parser_trans_rmgr_heap2_get_tuple m_record_rmgr_heap2_get_tuple_info[] =
{
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT, xk_pg_parser_trans_rmgr_heap2_minsert_get_tuple}
};

static xk_pg_parser_trans_rmgr_heap_get_tuple m_record_rmgr_heap_get_tuple_info[] =
{
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT, xk_pg_parser_trans_rmgr_heap_insert_get_tuple},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE, xk_pg_parser_trans_rmgr_heap_delete_get_tuple},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE, xk_pg_parser_trans_rmgr_heap_update_get_tuple},
    { XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE, xk_pg_parser_trans_rmgr_heap_update_get_tuple}
};

//static uint8_t check_table_is_catalog(uint32_t oid)
//{
//    return oid < 16384 ? (uint8_t)1 : (uint8_t)0;
//}

#if 0
static bool check_table_need_add_oid(uint32_t oid)
{
    switch (oid)
    {
        case TypeRelationId:
        case ProcedureRelationId:
        case RelationRelationId:
        case AccessMethodRelationId:
        case NamespaceRelationId:
        case OperatorClassRelationId:
        case CollationRelationId:
        case EnumRelationId:
        case ConstraintRelationId:
        case AttrDefaultRelationId:
            return true;
        default:
            return false;
    }
}
#endif

static uint8_t check_table_type(uint32_t oid)
{
    switch (oid)
    {
        case TypeRelationId:
        case AttributeRelationId:
        case ProcedureRelationId:
        case RelationRelationId:
        case AccessMethodRelationId:
        case NamespaceRelationId:
        case OperatorClassRelationId:
        case CollationRelationId:
        case EnumRelationId:
        case RangeRelationId:
        case OperatorRelationId:
            return XK_PG_PARSER_TRANSLOG_TABLETYPE_DICT;
        default:
            return XK_PG_PARSER_TRANSLOG_TABLETYPE_SYS;
    }
}

bool xk_pg_parser_trans_rmgr_heap_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = XK_PG_PARSER_RMGR_HEAP_INFOCNT;
    int32_t index = 0;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap_info[index].m_infofunc_trans(state, result, xk_pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;
}

bool xk_pg_parser_trans_rmgr_heap2_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = XK_PG_PARSER_RMGR_HEAP2_INFOCNT;
    int32_t index = 0;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap2_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap2_info[index].m_infofunc_trans(state, result, xk_pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;
}

bool xk_pg_parser_trans_rmgr_heap_trans_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = XK_PG_PARSER_RMGR_HEAP_GET_TUPLE_INFOCNT;
    int32_t index = 0;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap_get_tuple_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap_get_tuple_info[index].m_infofunc_trans(state, result, xk_pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;
}

bool xk_pg_parser_trans_rmgr_heap2_trans_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t infocnts = XK_PG_PARSER_RMGR_HEAP2_GET_TUPLE_INFOCNT;
    int32_t index = 0;
    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap2_get_tuple_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap2_get_tuple_info[index].m_infofunc_trans(state, result, xk_pg_parser_errno);
        break;
    }
    /* 没有找到时返回false */
    return false;
}

/* insert */
static bool xk_pg_parser_trans_rmgr_heap_insert_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *insert_record = NULL;
    xk_pg_parser_xl_heap_insert *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple = NULL;
    xk_pg_parser_sysdict_tableInfo tbinfo = {'\0'};
    xk_pg_parser_TupleDesc tupdesc = NULL;
    xk_pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t  natt = 0;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    size_t tuplelen = 0;
    char *page = NULL;
    char *tupledata = NULL;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;
    bool have_oid = false;
    uint32_t tuple_oid = xk_pg_parser_InvalidOid;

    /* 检查出入参的合法性 */
    if (!state || !result || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap insert], invalid param\n");
        return false;
    }

    xlrec = (xk_pg_parser_xl_heap_insert *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&insert_record),
                                  sizeof(xk_pg_parser_translog_tbcol_values)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_00;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap insert], MALLOC failed\n");
        return false;
    }

    insert_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
    insert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if(!xk_pg_parser_sysdict_getTableInfo(relfilenode,
                                      state->trans_data->m_sysdicts,
                                      &tbinfo))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_TBINFO;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap insert], get table info failed\n");
        return false;
    }
    insert_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    insert_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    insert_record->m_relid = tbinfo.oid;
    insert_record->m_valueCnt = (uint16_t)tbinfo.natts;
    natt = tbinfo.natts;
    if (state->trans_data->m_iscatalog)
        insert_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    else
        insert_record->m_base.m_tabletype = XK_PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    /* replica模式和系统表 */
    if (true)
    {
        void *temp_tuple = NULL;
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page,
                                          state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_01;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page,
                                                    state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make full image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            insert_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        state->trans_data->m_dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page,
                                                                        &insert_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap insert], get %u tuples from image\n",
                                    insert_record->m_tupleCnt);
            if (!insert_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make return tuple failed\n");
                return false;
            }
            /*设置返回类型*/
            insert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            insert_record->m_relfilenode = relfilenode;

            temp_tuple = (void *)xk_pg_parser_assemble_tuple(dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page,
                                                xlrec->offnum);
            if (!temp_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make tuple failed\n");
                return false;
            }
            tuple = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple;
            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            uint32_t tuplen = 0;
            tupledata = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE2;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], get tuple from block failed\n");
                return false;
            }
            tuplelen = datalen - xk_pg_parser_SizeOfHeapHeader;
            temp_tuple = (void *)xk_pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
            if (!temp_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE2;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make tuple from block failed\n");
                return false;
            }
            xk_pg_parser_reassemble_tuple_from_wal_data(tupledata,
                                                        datalen,
                                                        (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple,
                                                        state->decoded_record->xl_xid,
                                                        xk_pg_parser_InvalidTransactionId,
                                                        dbtype,
                                                        dbversion);

            tuple = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple;
            tuplen = tuple->tuple.t_len;

            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(insert_record->m_tuple),
                                     sizeof(xk_pg_parser_translog_tuplecache)))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_02;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            insert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* 处理要返回的tuple数据 */
            insert_record->m_tuple->m_pageno = state->blocks[0].blkno;
            insert_record->m_tupleCnt = 1;
            insert_record->m_relfilenode = relfilenode;
            insert_record->m_tuple->m_tuplelen = tuplen;
            insert_record->m_tuple->m_itemoffnum = xlrec->offnum;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "DEBUG: trans record is [heap insert], return tuple "
                                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                        insert_record->m_tuple->m_itemoffnum,
                                        insert_record->m_tuple->m_pageno,
                                        insert_record->m_tuple->m_tuplelen);
            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(insert_record->m_tuple->m_tupledata),
                                     tuplen))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_03;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }

            rmemcpy0(insert_record->m_tuple->m_tupledata,
                     0,
                     tuple->tuple.t_data,
                     tuple->tuple.t_len);
        }
    }
    tupdesc = xk_pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_MAKE_DESC;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap insert],"
                                "make DESC failed \n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **)&insert_record->m_new_values,
                                 insert_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_04;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap insert],"
                                "MALLOC col value failed \n");
        return false;
    }
    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "DEBUG: tupdesc->natts:%d\n", tupdesc->natts);
    zicinfo.convertinfo = state->trans_data->m_convert;

    /* tuple取出列值, 并转换为可读信息 */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        xk_pg_parser_Datum origval = (xk_pg_parser_Datum)0;
        xk_pg_parser_translog_tbcol_value *colvalue = &insert_record->m_new_values[natt];
        bool isnull = true;
        bool ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }

        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column name: %s\n", colvalue->m_colName ? colvalue->m_colName : "NULL");

        origval = xk_pg_parser_heap_getattr(&tuple->tuple,
                                             natt + 1,
                                             tupdesc,
                                             &isnull,
                                             &ismissing,
                                             dbtype,
                                             dbversion);

        colvalue->m_coltype = tupdesc->attrs[natt].atttypid;

        if (!strcmp(insert_record->m_base.m_schemaname, PG_TOAST_NAME))
            zicinfo.istoast = true;
        else
            zicinfo.istoast = false;

        zicinfo.dbtype = dbtype;
        zicinfo.dbversion = dbversion;
        zicinfo.errorno = xk_pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (isnull)
        {
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: IS NULL\n");
            colvalue->m_info = INFO_COL_IS_NULL;
            continue;
        }
        if (ismissing)
        {
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: IS MISSING\n");
            colvalue->m_info = INFO_COL_MAY_NULL;
            continue;
        }
        else if (!xk_pg_parser_convert_attr_to_str_value(origval,
                                                         state->trans_data->m_sysdicts,
                                                         colvalue,
                                                         &zicinfo))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_CONVERT_ATTR_TO_CHAR;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap insert],"
                                    "convert attr to char failed \n");
            return false;
        }
    }
    if (have_oid)
    {
        char *result = NULL;
        if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void **) &result, 12))
            return (xk_pg_parser_Datum) 0;
        snprintf(result, 12, "%u", tuple_oid);
        insert_record->m_new_values[tupdesc->natts].m_info = INFO_NOTHING;
        insert_record->m_new_values[tupdesc->natts].m_freeFlag = true;
        insert_record->m_new_values[tupdesc->natts].m_value = result;
        insert_record->m_new_values[tupdesc->natts].m_valueLen = strlen(result);
        insert_record->m_new_values[tupdesc->natts].m_coltype = XK_PG_SYSDICT_OIDOID;
        insert_record->m_new_values[tupdesc->natts].m_colName = xk_pg_parser_mcxt_strndup("oid", 3);
    }

    *result = (xk_pg_parser_translog_tbcolbase*)insert_record;
    /* 释放tuple, desc, tbinfo.attr */
    if (tuple)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    if (tupdesc)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    if (tbinfo.pgattr)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    if (zicinfo.zicdata)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    return true;
}

/* insert */
static bool xk_pg_parser_trans_rmgr_heap_insert_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *insert_record = NULL;
    xk_pg_parser_xl_heap_insert *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple = NULL;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    size_t tuplelen = 0;
    char *page = NULL;
    char *tupledata = NULL;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;

    xlrec = (xk_pg_parser_xl_heap_insert *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&insert_record),
                                  sizeof(xk_pg_parser_translog_tbcol_values)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_00;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap insert], MALLOC failed\n");
        return false;
    }

    insert_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
    insert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    /* replica模式和系统表 */
    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        void *temp_tuple = NULL;
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page,
                                          state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_01;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page,
                                                    state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make full image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            insert_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        state->trans_data->m_dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page,
                                                                        &insert_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap insert], get %u tuples from image\n",
                                    insert_record->m_tupleCnt);
            if (!insert_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make return tuple failed\n");
                return false;
            }
            /*设置返回类型*/
            insert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            insert_record->m_relfilenode = relfilenode;

            temp_tuple = (void *)xk_pg_parser_assemble_tuple(dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page,
                                                xlrec->offnum);
            if (!temp_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make tuple failed\n");
                return false;
            }

            tuple = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple;

            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            uint32_t tuplen = 0;
            tupledata = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE2;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], get tuple from block failed\n");
                return false;
            }

            tuplelen = datalen - xk_pg_parser_SizeOfHeapHeader;

            temp_tuple = (void *)xk_pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
            if (!temp_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE2;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], make tuple from block failed\n");
                return false;
            }
            xk_pg_parser_reassemble_tuple_from_wal_data(tupledata,
                                                        datalen,
                                                        (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple,
                                                        state->decoded_record->xl_xid,
                                                        xk_pg_parser_InvalidTransactionId,
                                                        dbtype,
                                                        dbversion);

            tuple = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple;
            tuplen = tuple->tuple.t_len;

            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(insert_record->m_tuple),
                                     sizeof(xk_pg_parser_translog_tuplecache)))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_02;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            insert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* 处理要返回的tuple数据 */
            insert_record->m_tuple->m_pageno = state->blocks[0].blkno;
            insert_record->m_tupleCnt = 1;
            insert_record->m_relfilenode = relfilenode;
            insert_record->m_tuple->m_tuplelen = tuplen;
            insert_record->m_tuple->m_itemoffnum = xlrec->offnum;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "DEBUG: trans record is [heap insert], return tuple "
                                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                        insert_record->m_tuple->m_itemoffnum,
                                        insert_record->m_tuple->m_pageno,
                                        insert_record->m_tuple->m_tuplelen);
            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(insert_record->m_tuple->m_tupledata),
                                     tuplen))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_03;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }

            rmemcpy0(insert_record->m_tuple->m_tupledata,
                     0,
                     tuple->tuple.t_data,
                     tuple->tuple.t_len);
        }
    }

    *result = (xk_pg_parser_translog_tbcolbase*)insert_record;
    /* 释放tuple, desc, tbinfo.attr */
    if (tuple)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);

    return true;
}

/* multi insert */
static bool xk_pg_parser_trans_rmgr_heap2_minsert_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_nvalues *minsert_record = NULL;
    xk_pg_parser_xl_heap_multi_insert *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf **tuple = NULL;
    xk_pg_parser_sysdict_tableInfo tbinfo = {'\0'};
    xk_pg_parser_TupleDesc tupdesc = NULL;
    xk_pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t  natt = 0;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    char *page = NULL;
    char *data = NULL;
    bool  isinit = false;
    int32_t i = 0;
    uint16_t off_num = 0;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;

    /* 检查出入参的合法性 */
    if (!state || !result || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MINSERT_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], invalid param\n");
        return false;
    }

    xlrec = (xk_pg_parser_xl_heap_multi_insert *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&minsert_record),
                                  sizeof(xk_pg_parser_translog_tbcol_nvalues)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_05;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    minsert_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT;
    minsert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if(!xk_pg_parser_sysdict_getTableInfo(relfilenode,
                                      state->trans_data->m_sysdicts,
                                      &tbinfo))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_TBINFO;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], get table info failed\n");
        return false;
    }
    minsert_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    minsert_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    minsert_record->m_relid = tbinfo.oid;
    minsert_record->m_valueCnt = (uint16_t)tbinfo.natts;
    minsert_record->m_rowCnt = xlrec->ntuples;
    natt = tbinfo.natts;

    if (XK_PG_PARSER_XLOG_HEAP_INIT_PAGE & xk_pg_parser_XLogRecGetInfo(state))
    {
        isinit = true;
    }

    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void**) &tuple,
                                  sizeof(xk_pg_parser_ReorderBufferTupleBuf *) * xlrec->ntuples))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_07;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    if (state->trans_data->m_iscatalog)
    {
        minsert_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    }
    else
    {
        minsert_record->m_base.m_tabletype = XK_PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    }
    /* replica模式和系统表 */
    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            //todo deal free
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page,
                                          state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_08;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page,
                                                    state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert], get image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            minsert_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        state->trans_data->m_dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page,
                                                                        &minsert_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            if (!minsert_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert],"
                                        "get tuple from image failed\n");
                return false;
            }
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap nulti insert], get %u tuples from image\n",
                                    minsert_record->m_tupleCnt);
            /*设置返回类型*/
            minsert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            minsert_record->m_relfilenode = relfilenode;

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                    off_num = i + 1;
                else
                    off_num = xlrec->offsets[i];
                tuple[i] = xk_pg_parser_assemble_tuple(dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page,
                                                off_num);
                if (!tuple[i])
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 multi_insert],"
                                            "get tuple failed\n");
                    return false;
                }
            }

            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            data = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                xk_pg_parser_xl_multi_insert_tuple *xlhdr = NULL;
                xlhdr = (xk_pg_parser_xl_multi_insert_tuple *)XK_PG_PARSER_SHORTALIGN(data);
                data = ((char *)xlhdr) + xk_pg_parser_SizeOfMultiInsertTuple;
                datalen = xlhdr->datalen;
                tuple[i] = xk_pg_parser_heaptuple_get_tuple_space(datalen, dbtype, dbversion);
                if (!tuple[i])
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 multi_insert],"
                                            "get tuple failed\n");
                    return false;
                }
                reassemble_mutituple_from_wal_data(data,
                                                   datalen,
                                                   tuple[i],
                                                   xlhdr,
                                                   dbtype,
                                                   dbversion);
                data += datalen;
            }

            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                        (void**)&(minsert_record->m_tuple),
                        sizeof(xk_pg_parser_translog_tuplecache) * minsert_record->m_rowCnt))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_09;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            minsert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* 处理要返回的tuple数据 */
            minsert_record->m_tupleCnt = minsert_record->m_rowCnt;
            minsert_record->m_relfilenode = relfilenode;
            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                    off_num = i + 1;
                else
                    off_num = xlrec->offsets[i];
                minsert_record->m_tuple[i].m_pageno = state->blocks[0].blkno;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;

                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                        (void**)&(minsert_record->m_tuple[i].m_tupledata),
                                         tuple[i]->tuple.t_len))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0B;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                    return false;
                }
                rmemcpy0(minsert_record->m_tuple[i].m_tupledata,
                         0,
                         tuple[i]->tuple.t_data,
                         tuple[i]->tuple.t_len);
            }
            if ( XK_PG_PARSER_DEBUG_SILENCE < state->trans_data->m_debugLevel)
            {
                for (i = 0; i < (int32_t)minsert_record->m_tupleCnt; i++)
                printf("DEBUG: trans record is [heap multi insert], return tuple "
                                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                        minsert_record->m_tuple[i].m_itemoffnum,
                                        minsert_record->m_tuple[i].m_pageno,
                                        minsert_record->m_tuple[i].m_tuplelen);
            }
        }
    }
    else
    {
        /* logical模式 */
        if (!(xlrec->flags & XK_PG_PARSER_TRANS_XLH_INSERT_CONTAINS_NEW_TUPLE))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_LOGICAL_NEW_FLAG;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap2 multi_insert],"
                                    "logical mode but don't have new tuple \n");
            return false;
        }
        data = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);
        if (!data)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_LOGICAL_DATA;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap2 multi_insert],"
                                    "logical mode don't have data \n");
            return false;
        }
        for (i = 0; i < minsert_record->m_rowCnt; i++)
        {
            xk_pg_parser_xl_multi_insert_tuple *xlhdr = NULL;
            xlhdr = (xk_pg_parser_xl_multi_insert_tuple *)XK_PG_PARSER_SHORTALIGN(data);
            data = ((char *)xlhdr) + xk_pg_parser_SizeOfMultiInsertTuple;
            datalen = xlhdr->datalen;

            tuple[i] = xk_pg_parser_heaptuple_get_tuple_space(datalen, dbtype, dbversion);
            if (!tuple[i])
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert],"
                                        "get tuple failed\n");
                return false;
            }
            reassemble_mutituple_from_wal_data(data,
                                               datalen,
                                               tuple[i],
                                               xlhdr,
                                               dbtype,
                                               dbversion);

            data += datalen;
        }
        /* 设置返回类型, logical模式无需返回page、tuple信息 */
        minsert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
    }
    tupdesc = xk_pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_MAKE_DESC;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert],"
                                "get tuple failed\n");
        return false;
    }

    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void **)&minsert_record->m_rows,
                                    minsert_record->m_rowCnt * sizeof(xk_pg_parser_translog_tbcol_rows)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0C;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    for (i = 0; i < minsert_record->m_rowCnt; i++)
    {

        if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void **)&minsert_record->m_rows[i].m_new_values,
                                    minsert_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0D;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
            return false;
        }
        zicinfo.convertinfo = state->trans_data->m_convert;
        /* tuple取出列值, 并转换为可读信息 */
        for (natt = 0; natt < tupdesc->natts; natt++)
        {
            xk_pg_parser_Datum origval = (xk_pg_parser_Datum)0;
            xk_pg_parser_translog_tbcol_value *colvalue = &minsert_record->m_rows[i].m_new_values[natt];
            bool isnull = true;
            bool ismissing = false;
            colvalue->m_info = INFO_NOTHING;
            if (tupdesc->attrs[natt].attisdropped)
            {
                colvalue->m_info = INFO_COL_IS_DROPED;
                continue;
            }
            /* 列名赋值, 复用sysdict中的内存地址, 无需释放 */
            colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
            colvalue->m_freeFlag = true;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column name: %s\n", colvalue->m_colName ? colvalue->m_colName : "NULL");

            origval = xk_pg_parser_heap_getattr(&tuple[i]->tuple,
                                                natt + 1,
                                                tupdesc,
                                                &isnull,
                                                &ismissing,
                                                dbtype,
                                                dbversion);

            colvalue->m_coltype = tupdesc->attrs[natt].atttypid;

            zicinfo.dbtype = dbtype;
            zicinfo.dbversion = dbversion;
            zicinfo.errorno = xk_pg_parser_errno;
            zicinfo.debuglevel = state->trans_data->m_debugLevel;
            if (isnull)
            {
                colvalue->m_info = INFO_COL_IS_NULL;
                continue;
            }
            if (ismissing)
            {
                colvalue->m_info = INFO_COL_MAY_NULL;
                continue;
            }
            else if (!xk_pg_parser_convert_attr_to_str_value(origval,
                                                            state->trans_data->m_sysdicts,
                                                            colvalue,
                                                            &zicinfo))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_CONVERT_ATTR_TO_CHAR;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert],"
                                "convert attr to str failed\n");
                return false;
            }
        }
    }

    *result = (xk_pg_parser_translog_tbcolbase*)minsert_record;
    /* 释放tuple, desc, tbinfo.pgattr */
    if (tuple)
    {
        for (i = 0; i < minsert_record->m_rowCnt; i++)
            xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple[i]);
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }

    if (tupdesc)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    if (tbinfo.pgattr)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    if (zicinfo.zicdata)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);

    return true;
}

/* multi insert */
static bool xk_pg_parser_trans_rmgr_heap2_minsert_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_nvalues *minsert_record = NULL;
    xk_pg_parser_xl_heap_multi_insert *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf **tuple = NULL;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    char *page = NULL;
    char *data = NULL;
    bool  isinit = false;
    int32_t i = 0;
    uint16_t off_num = 0;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;

    xlrec = (xk_pg_parser_xl_heap_multi_insert *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&minsert_record),
                                  sizeof(xk_pg_parser_translog_tbcol_nvalues)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_05;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    minsert_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT;
    minsert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    minsert_record->m_rowCnt = xlrec->ntuples;

    if (XK_PG_PARSER_XLOG_HEAP_INIT_PAGE & xk_pg_parser_XLogRecGetInfo(state))
        isinit = true;

    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void**) &tuple,
                                  sizeof(xk_pg_parser_ReorderBufferTupleBuf *) * xlrec->ntuples))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_07;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    /* replica模式和系统表 */
    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            //todo deal free
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page,
                                          state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_08;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page,
                                                    state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert], get image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            minsert_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        state->trans_data->m_dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page,
                                                                        &minsert_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            if (!minsert_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert],"
                                        "get tuple from image failed\n");
                return false;
            }
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap nulti insert], get %u tuples from image\n",
                                    minsert_record->m_tupleCnt);
            /*设置返回类型*/
            minsert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            minsert_record->m_relfilenode = relfilenode;

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                    off_num = i + 1;
                else
                    off_num = xlrec->offsets[i];
                tuple[i] = xk_pg_parser_assemble_tuple(dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page,
                                                off_num);
                if (!tuple[i])
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 multi_insert],"
                                            "get tuple failed\n");
                    return false;
                }
            }

            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            data = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                xk_pg_parser_xl_multi_insert_tuple *xlhdr = NULL;
                xlhdr = (xk_pg_parser_xl_multi_insert_tuple *)XK_PG_PARSER_SHORTALIGN(data);
                data = ((char *)xlhdr) + xk_pg_parser_SizeOfMultiInsertTuple;
                datalen = xlhdr->datalen;
                tuple[i] = xk_pg_parser_heaptuple_get_tuple_space(datalen, dbtype, dbversion);
                if (!tuple[i])
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 multi_insert],"
                                            "get tuple failed\n");
                    return false;
                }
                reassemble_mutituple_from_wal_data(data,
                                                   datalen,
                                                   tuple[i],
                                                   xlhdr,
                                                   dbtype,
                                                   dbversion);
                data += datalen;
            }

            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                        (void**)&(minsert_record->m_tuple),
                        sizeof(xk_pg_parser_translog_tuplecache) * minsert_record->m_rowCnt))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_09;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            minsert_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* 处理要返回的tuple数据 */
            minsert_record->m_tupleCnt = minsert_record->m_rowCnt;
            minsert_record->m_relfilenode = relfilenode;
            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                    off_num = i + 1;
                else
                    off_num = xlrec->offsets[i];

                minsert_record->m_tuple[i].m_pageno = state->blocks[0].blkno;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                        (void**)&(minsert_record->m_tuple[i].m_tupledata),
                                         tuple[i]->tuple.t_len))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0B;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                    return false;
                }
                rmemcpy0(minsert_record->m_tuple[i].m_tupledata,
                         0,
                         tuple[i]->tuple.t_data,
                         tuple[i]->tuple.t_len);
                
            }
            if ( XK_PG_PARSER_DEBUG_SILENCE < state->trans_data->m_debugLevel)
            {
                for (i = 0; i < (int32_t)minsert_record->m_tupleCnt; i++)
                printf("DEBUG: trans record is [heap multi insert], return tuple "
                                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                        minsert_record->m_tuple[i].m_itemoffnum,
                                        minsert_record->m_tuple[i].m_pageno,
                                        minsert_record->m_tuple[i].m_tuplelen);
            }
        }
    }

    *result = (xk_pg_parser_translog_tbcolbase*)minsert_record;
    /* 释放tuple, desc, tbinfo.pgattr */
    if (tuple)
    {
        for (i = 0; i < minsert_record->m_rowCnt; i++)
            xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple[i]);
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }

    return true;
}

static bool xk_pg_parser_trans_rmgr_heap_delete_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *delete_record = NULL;
    xk_pg_parser_xl_heap_delete *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple = NULL;
    xk_pg_parser_sysdict_tableInfo tbinfo = {'\0'};
    xk_pg_parser_TupleDesc tupdesc = NULL;
    xk_pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t  natt = 0;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    size_t tuplelen = 0;
    char *page = NULL;
    bool  use_logical = false;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;
    bool have_oid = false;
    uint32_t tuple_oid = xk_pg_parser_InvalidOid;

    /* 检查出入参的合法性 */
    if (!state || !result || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete], invalid param\n");
        return false;
    }

    xlrec = (xk_pg_parser_xl_heap_delete *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&delete_record),
                                  sizeof(xk_pg_parser_translog_tbcol_values)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0E;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete], MALLOC failed\n");
        return false;
    }

    delete_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE;
    delete_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if(!xk_pg_parser_sysdict_getTableInfo(relfilenode,
                                      state->trans_data->m_sysdicts,
                                      &tbinfo))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TBINFO;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete],"
                                "get tbinfo failed\n");
        return false;
    }

    delete_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    delete_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    delete_record->m_relid = tbinfo.oid;
    delete_record->m_valueCnt = (uint16_t)tbinfo.natts;
    natt = tbinfo.natts;

    tupdesc = xk_pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_DESC;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete],"
                                "make desc failed\n");
        return false;
    }

    /* replica模式和系统表 */
    if (state->trans_data->m_iscatalog)
        delete_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    else
        delete_record->m_base.m_tabletype = XK_PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page,
                                          state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0F;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page,
                                                    state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "get image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            delete_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        state->trans_data->m_dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page,
                                                                        &delete_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            if (!delete_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "get tuple failed\n");
                return false;
            }
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap delete], get %u tuples from image\n",
                                    delete_record->m_tupleCnt);

            /*设置返回类型*/
            delete_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            delete_record->m_relfilenode = relfilenode;

            tuple = xk_pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page,
                                                xlrec->offnum);
            if (!tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPEL_ALLOC;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "malloc tuple failed\n");
                return false;
            }

            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        /* 没有全页写的情况下, 我们需要使用传进的tuple数据 */
        else
        {
            void *temp_tuple = NULL;
            void *tuphdr = NULL;
            xk_pg_parser_translog_translog2col *trans_data = state->trans_data;
            xk_pg_parser_translog_tuplecache *current_tuplecache = NULL;

            /* 检查传入的页数据 */
            if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
            {
                if (!strcmp(tbinfo.scname, "pg_toast") && !strncmp(tbinfo.tbname, "pg_toast", 8))
                {
                    int index_colnum = 0;
                    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                                (void **)&delete_record->m_old_values,
                                                delete_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
                    {
                        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_10;
                        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                                "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                        return false;
                    }
                    for (index_colnum = 0; index_colnum < delete_record->m_valueCnt; index_colnum++)
                    {
                        xk_pg_parser_translog_tbcol_value *colvalue = &delete_record->m_old_values[index_colnum];
                        colvalue->m_colName = rstrdup(tbinfo.pgattr[index_colnum]->attname.data);
                        colvalue->m_freeFlag = true;
                        colvalue->m_coltype = tupdesc->attrs[index_colnum].atttypid;
                        colvalue->m_info = INFO_COL_MAY_NULL;
                    }
                    *result = (xk_pg_parser_translog_tbcolbase*)delete_record;
                    /* 释放tuple, desc, tbinfo.pgattr */
                    if (tuple)
                        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
                    if (tupdesc)
                        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
                    if (tbinfo.pgattr)
                        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
                    if (zicinfo.zicdata)
                        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
                    return true;
                }
                else
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE_IN;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap2 delete],"
                                            "tuple input check failed\n");
                    return false;
                }
            }
            delete_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
            current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                 trans_data->m_tuplecnt,
                                                                 xlrec->offnum,
                                                                 state->blocks[0].blkno);
            if (!current_tuplecache)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE_IN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "tuple input check failed\n");
                return false;
            }
            tuphdr = (void*) current_tuplecache->m_tupledata;
            tuplelen = (size_t)current_tuplecache->m_tuplelen;
            temp_tuple = (void *)xk_pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
            if (!temp_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TEMP_ALLOC;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "malloc temp tuple failed\n");
                return false;
            }
            xk_pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr,
                                                                 tuplelen,
                                                                 (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple,
                                                                 dbtype,
                                                                 dbversion);

            tuple = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple;
        }
    }
    else
    {
        /* logical模式, delete语句在logical模式下会将旧元组存放在maindata后 */
        use_logical = true;
        if (!(xlrec->flags & XK_PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD))
        {
            /* logical下对toast进行过滤处理, 这里不使用goto */
            //if (!strcmp(tbinfo.scname, "pg_toast") && !strncmp(tbinfo.tbname, "pg_toast", 8))
            //{
            int index_colnum = 0;
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                        (void **)&delete_record->m_old_values,
                                        delete_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_10;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                return false;
            }
            for (index_colnum = 0; index_colnum < delete_record->m_valueCnt; index_colnum++)
            {
                xk_pg_parser_translog_tbcol_value *colvalue = &delete_record->m_old_values[index_colnum];
                colvalue->m_colName = rstrdup(tbinfo.pgattr[index_colnum]->attname.data);
                colvalue->m_freeFlag = true;
                colvalue->m_coltype = tupdesc->attrs[index_colnum].atttypid;
                colvalue->m_info = INFO_COL_MAY_NULL;
            }
            *result = (xk_pg_parser_translog_tbcolbase*)delete_record;
            /* 释放tuple, desc, tbinfo.pgattr */
            if (tuple)
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
            if (tupdesc)
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
            if (tbinfo.pgattr)
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
            if (zicinfo.zicdata)
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
            return true;
            //}
            //else
            //{
            //    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_LOGICAL_FLAG;
            //    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
            //                            "ERROR: trans record is [heap2 delete],"
            //                            "logical flag check failed\n");
            //    return false;
            //}
            
        }
        datalen = xk_pg_parser_XLogRecGetDataLen(state) - xk_pg_parser_SizeOfHeapDelete;
        if (0 == datalen)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_LOGICAL_DATA;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap2 delete],"
                                    "logical mode check data failed\n");
            return false;
        }

        tuplelen = datalen - xk_pg_parser_SizeOfHeapHeader;
        tuple = xk_pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
        if (!tuple)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPEL_ALLOC;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap2 delete],"
                                    "malloc tuple failed\n");
            return false;
        }
        xk_pg_parser_DecodeXLogTuple((char *) xlrec + xk_pg_parser_SizeOfHeapDelete,
                                     datalen, tuple, dbtype, dbversion);

        /* 设置返回类型, logical模式无需返回page、tuple信息 */
        delete_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
    }
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **)&delete_record->m_old_values,
                                 delete_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_10;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete], MALLOC failed\n");
        return false;
    }
    zicinfo.convertinfo = state->trans_data->m_convert;
    /* tuple取出列值, 并转换为可读信息 */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        xk_pg_parser_Datum origval = (xk_pg_parser_Datum)0;
        xk_pg_parser_translog_tbcol_value *colvalue = &delete_record->m_old_values[natt];
        bool isnull = true;
        bool ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }


        if (!strcmp(delete_record->m_base.m_schemaname, PG_TOAST_NAME))
            zicinfo.istoast = true;
        else
            zicinfo.istoast = false;

        /* 列名赋值, 复用sysdict中的内存地址, 无需释放 */
        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column name: %s\n", colvalue->m_colName ? colvalue->m_colName : "NULL");

        origval = xk_pg_parser_heap_getattr(&tuple->tuple,
                                                natt + 1,
                                                tupdesc,
                                                &isnull,
                                                &ismissing,
                                                dbtype,
                                                dbversion);

        if (isnull)
        {
            colvalue->m_info = INFO_COL_IS_NULL;
            continue;
        }
        if (ismissing)
        {
            colvalue->m_info = INFO_COL_MAY_NULL;
            continue;
        }
        colvalue->m_coltype = tupdesc->attrs[natt].atttypid;
        zicinfo.dbtype = dbtype;
        zicinfo.dbversion = dbversion;
        zicinfo.errorno = xk_pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (!xk_pg_parser_convert_attr_to_str_value(origval,
                                                    state->trans_data->m_sysdicts,
                                                    colvalue,
                                                    &zicinfo))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_ATTR_TO_STR;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap2 delete],"
                                    "convert attr to str failed\n");
            return false;
        }
    }

    if (have_oid)
    {
        char *result = NULL;
        if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void **) &result, 12))
            return (xk_pg_parser_Datum) 0;
        snprintf(result, 12, "%u", tuple_oid);
        delete_record->m_old_values[tupdesc->natts].m_info = INFO_NOTHING;
        delete_record->m_old_values[tupdesc->natts].m_freeFlag = true;
        delete_record->m_old_values[tupdesc->natts].m_value = result;
        delete_record->m_old_values[tupdesc->natts].m_valueLen = strlen(result);
        delete_record->m_old_values[tupdesc->natts].m_coltype = XK_PG_SYSDICT_OIDOID;
        delete_record->m_old_values[tupdesc->natts].m_colName = xk_pg_parser_mcxt_strndup("oid", 3);
    }

    *result = (xk_pg_parser_translog_tbcolbase*)delete_record;
    /* 释放tuple, desc, tbinfo.pgattr */
    if (tuple)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    if (tupdesc)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    if (tbinfo.pgattr)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    if (zicinfo.zicdata)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    return true;
}

static bool xk_pg_parser_trans_rmgr_heap_delete_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *delete_record = NULL;
    uint32_t relfilenode = 0;
    char *page = NULL;

    /* 检查出入参的合法性 */
    if (!state || !result || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete], invalid param\n");
        return false;
    }

    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&delete_record),
                                  sizeof(xk_pg_parser_translog_tbcol_values)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0E;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap2 delete], MALLOC failed\n");
        return false;
    }

    delete_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE;
    delete_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    /* replica模式和系统表 */
    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page,
                                          state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0F;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page,
                                                    state->trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "get image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            delete_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        state->trans_data->m_dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page,
                                                                        &delete_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            if (!delete_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap2 delete],"
                                        "get tuple failed\n");
                return false;
            }
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap delete], get %u tuples from image\n",
                                    delete_record->m_tupleCnt);

            /*设置返回类型*/
            delete_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            delete_record->m_relfilenode = relfilenode;

            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }

            *result = (xk_pg_parser_translog_tbcolbase*)delete_record;
        }
        else
            xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, delete_record);
    }
    else
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, delete_record);

    return true;
}

static bool xk_pg_parser_trans_rmgr_heap_update_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *update_record = NULL;
    xk_pg_parser_xl_heap_update *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple_new = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple_old = NULL;
    xk_pg_parser_sysdict_tableInfo tbinfo = {'\0'};
    xk_pg_parser_TupleDesc tupdesc = NULL;
    xk_pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t  natt = 0;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    size_t tuplelen_new = 0;
    size_t tuplelen_old = 0;
    char *page_new = NULL;
    char *page_old = NULL;
    char *tupledata_new = NULL;
    bool  use_logical = false;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;
    bool have_oid = false;
    bool have_old_tuple_data = true;
    uint32_t tuple_oid = xk_pg_parser_InvalidOid;

    /* 检查出入参的合法性 */
    if (!state || !result || !xk_pg_parser_errno)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_CHECK;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update], invalid param\n");
        return false;
    }

    xlrec = (xk_pg_parser_xl_heap_update *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&update_record),
                                  sizeof(xk_pg_parser_translog_tbcol_values)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_11;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }

    update_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE;
    update_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if(!xk_pg_parser_sysdict_getTableInfo(relfilenode,
                                      state->trans_data->m_sysdicts,
                                      &tbinfo))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TBINFO;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update],"
                                "MALLOC failed\n");
        return false;
    }

    update_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    update_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    update_record->m_relid = tbinfo.oid;
    update_record->m_valueCnt = tbinfo.natts;
    natt = tbinfo.natts;
    /* replica模式和系统表 */
    if (state->trans_data->m_iscatalog)
        update_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    else
        update_record->m_base.m_tabletype = XK_PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            xk_pg_parser_translog_translog2col *trans_data = state->trans_data;
            void *tuphdr_old = NULL;
            void *temp_tuple_new = NULL;

            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page_new,
                                          trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_12;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page_new,
                                                    trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "make image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            update_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page_new,
                                                                        &update_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            if (!update_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "make return tuple failed\n");
                return false;
            }
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap update], get %u tuples from image\n",
                                    update_record->m_tupleCnt);
            /*设置返回类型*/
            update_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            update_record->m_relfilenode = relfilenode;

            /* 判断新旧tuple是否在同一个页块中 */
            if (0 == state->max_block_id)
            {
                /* 在同一个页块时 */
                void *temp_tuple_old = NULL;
                page_old = page_new;
                temp_tuple_old = (void *)xk_pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page_old,
                                                xlrec->old_offnum);
                tuple_old = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old;
            }
            else
            {
                xk_pg_parser_translog_tuplecache *current_tuplecache = NULL;
                void *temp_tuple_old = NULL;

                /* 不在同一个页块时, 使用传入的tuple, 检查传入的页数据 */
                if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap update],"
                                            "input tuple check failed\n");
                    return false;
                }
                current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                     trans_data->m_tuplecnt,
                                                                     xlrec->old_offnum,
                                                                     state->blocks[1].blkno);
                if (!current_tuplecache)
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap update],"
                                            "off:%u, block:%u,"
                                            " input tuple check failed\n", xlrec->old_offnum, state->blocks[0].blkno);
                    return false;
                }
                tuphdr_old = (void*) current_tuplecache->m_tupledata;
                tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
                temp_tuple_old = xk_pg_parser_heaptuple_get_tuple_space(tuplelen_old, dbtype, dbversion);
                if (!temp_tuple_old)
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap update],"
                                            "get old tuple failed\n");
                    return false;
                }
                xk_pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                                     tuplelen_old,
                                                                     (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old,
                                                                     dbtype,
                                                                     dbversion);
                tuple_old = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old;
            }
            temp_tuple_new = (void *)xk_pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page_new,
                                                xlrec->new_offnum);
            if (!temp_tuple_new)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "get new tuple failed\n");
                return false;
            }
            tuple_new = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_new;
            /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page_new)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page_new);
                page_new = NULL;
            }
        }
        /* 没有全页写的情况下, 旧元组我们需要使用传进的页数据 */
        else
        {
            void *tuphdr_old = NULL;
            void *temp_tuple_old = NULL;
            void *temp_tuple_new = NULL;
            size_t temp_len = 0;
            uint32_t temp_tuplen = 0;
            xk_pg_parser_translog_translog2col *trans_data = state->trans_data;

            xk_pg_parser_translog_tuplecache *current_tuplecache = NULL;
            /* 使用传入的tuple, 检查传入的页数据 */
            if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "input tuple check failed\n");
                return false;
            }
            if (0 == state->max_block_id)
                current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                 trans_data->m_tuplecnt,
                                                                 xlrec->old_offnum,
                                                                 state->blocks[0].blkno);
            else
                current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                 trans_data->m_tuplecnt,
                                                                 xlrec->old_offnum,
                                                                 state->blocks[1].blkno);
            if (!current_tuplecache)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "input tuple check failed\n");
                return false;
            }
            tuphdr_old = (void*) current_tuplecache->m_tupledata;
            tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
            temp_len = tuplelen_old - xk_pg_parser_SizeOfHeapHeader;
            temp_tuple_old = (void *)xk_pg_parser_heaptuple_get_tuple_space(temp_len, dbtype, dbversion);
            if (!temp_tuple_old)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "get old tuple failed\n");
                return false;
            }
            xk_pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                tuplelen_old,
                                                (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old,
                                                dbtype,
                                                dbversion);
            tuple_old = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old;
            /* 结合旧数据, 从record中组装出new tuple */
            tupledata_new = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata_new)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "get new tuple failed\n");
                return false;
            }
            if (!reassemble_tuplenew_from_wal_data(tupledata_new,
                                                   datalen,
                                                   (xk_pg_parser_ReorderBufferTupleBuf **)(&temp_tuple_new),
                                                   xlrec,
                                                   state->blocks[0].blkno,
                                                   xk_pg_parser_InvalidTransactionId,
                                                   tuphdr_old,
                                                   tuplelen_old,
                                                   dbtype,
                                                   dbversion))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_MAKE_NEW_TUPLE_FROM_OLD;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "make new tuple from old tuple failed\n");
                return false;
            }
            tuple_new = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_new;
            /*设置返回类型, update返回新tuple, 旧tuple无需返回*/
            update_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(update_record->m_tuple),
                                     sizeof(xk_pg_parser_translog_tuplecache)))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_13;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* 处理要返回的tuple数据 */
            update_record->m_tuple->m_pageno = state->blocks[0].blkno;
            update_record->m_tupleCnt = 1;
            update_record->m_relfilenode = relfilenode;
            temp_tuplen = tuple_new->tuple.t_len;
            update_record->m_tuple->m_tuplelen = temp_tuplen;
            update_record->m_tuple->m_itemoffnum = xlrec->new_offnum;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "DEBUG: trans record is [heap insert], return tuple "
                                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                        update_record->m_tuple->m_itemoffnum,
                                        update_record->m_tuple->m_pageno,
                                        update_record->m_tuple->m_tuplelen);
            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(update_record->m_tuple->m_tupledata),
                                     temp_tuplen))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_14;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }

            rmemcpy0(update_record->m_tuple->m_tupledata,
                     0,
                     tuple_new->tuple.t_data,
                     tuple_new->tuple.t_len);
        }
    }
    else
    {
        /* logical模式, update语句在logical模式下会将旧元组存放在maindata后 */
        use_logical = true;
        datalen = xk_pg_parser_XLogRecGetDataLen(state) - xk_pg_parser_SizeOfHeapUpdate;

        have_old_tuple_data = ((xlrec->flags & XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD)
                                || datalen > 0) ? true : false;
        /* 只有在有旧数据标识情况下, 才对数据进行处理 */
        if (have_old_tuple_data)
        {
            /* 从record中取出old tuple */
            tuplelen_old = datalen - xk_pg_parser_SizeOfHeapHeader;
            tuple_old = xk_pg_parser_heaptuple_get_tuple_space(tuplelen_old, dbtype, dbversion);
            if (!tuple_old)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_LOGICAL_MALLOC_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "logical malloc tuple failed\n");
                return false;
            }
            xk_pg_parser_DecodeXLogTuple((char *) xlrec + xk_pg_parser_SizeOfHeapUpdate,
                                        datalen, tuple_old, dbtype, dbversion);
        }
    
        /* 从record中取出new tuple */
        datalen = 0;
        tupledata_new = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);
        if (!tupledata_new)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_LOGICAL_GET_NEW_TUPLE_DATA;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap update],"
                                    "logical get new tuple data failed\n");
            return false;
        }
        tuplelen_new = datalen - xk_pg_parser_SizeOfHeapHeader;
        tuple_new = xk_pg_parser_heaptuple_get_tuple_space(tuplelen_new, dbtype, dbversion);
        if (!tuple_new)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_LOGICAL_MALLOC_TUPLE;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap update],"
                                    "logical malloc tuple failed\n");
            return false;
        }
        xk_pg_parser_reassemble_tuple_from_wal_data(tupledata_new,
                                                    datalen,
                                                    tuple_new,
                                                    state->decoded_record->xl_xid,
                                                    xk_pg_parser_InvalidTransactionId,
                                                    dbtype,
                                                    dbversion);

        /* 设置返回类型, logical模式无需返回page、tuple信息 */
        update_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_DATA;

    }
    tupdesc = xk_pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_DESC;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update],"
                                "get desc failed\n");
        return false;
    }

    /* 为旧列分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **)&update_record->m_old_values,
                                 update_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_15;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }

    /* 为新列分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **)&update_record->m_new_values,
                                 update_record->m_valueCnt * sizeof(xk_pg_parser_translog_tbcol_value)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_16;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }
    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: tuple block[%u] old off[%u], new off[%u]\n",
                                            state->blocks[0].blkno,
                                            xlrec->old_offnum,
                                            xlrec->new_offnum);

    zicinfo.convertinfo = state->trans_data->m_convert;
    /* tuple取出旧列值, 并转换为可读信息 */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        xk_pg_parser_Datum origval = (xk_pg_parser_Datum)0;
        xk_pg_parser_translog_tbcol_value *colvalue = &update_record->m_old_values[natt];
        bool isnull = true;
        bool ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }
        /* 列名赋值, 复用sysdict中的内存地址, 无需释放 */
        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column name: %s\n", colvalue->m_colName ? colvalue->m_colName : "NULL");
        if (!have_old_tuple_data)
        {
            colvalue->m_info = INFO_COL_MAY_NULL;
            continue;
        }

        origval = xk_pg_parser_heap_getattr(&tuple_old->tuple,
                                                    natt + 1,
                                                    tupdesc,
                                                    &isnull,
                                                    &ismissing,
                                                    dbtype,
                                                    dbversion);

        if (isnull)
        {
            if (use_logical)
                colvalue->m_info = INFO_COL_MAY_NULL;
            else
                colvalue->m_info = INFO_COL_IS_NULL;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column value is null\n");
            continue;
        }
        if (ismissing)
        {
            colvalue->m_info = INFO_COL_MAY_NULL;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column value is null\n");
            continue;
        }
        zicinfo.dbtype = dbtype;
        zicinfo.dbversion = dbversion;
        colvalue->m_coltype = tupdesc->attrs[natt].atttypid;
        zicinfo.errorno = xk_pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (!xk_pg_parser_convert_attr_to_str_value(origval,
                                                    state->trans_data->m_sysdicts,
                                                    colvalue,
                                                    &zicinfo))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_OLD_VALUE_ATTR_TO_STR;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap update],"
                                    "old value convert attr to str failed\n");
            return false;
        }
    }

    /* tuple取出新列值, 并转换为可读信息 */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        xk_pg_parser_Datum origval = (xk_pg_parser_Datum)0;
        xk_pg_parser_translog_tbcol_value *colvalue = &update_record->m_new_values[natt];
        bool isnull = true;
        bool ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }
        /* 列名赋值, 复用sysdict中的内存地址, 无需释放 */
        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: column name: %s\n", colvalue->m_colName ? colvalue->m_colName : "NULL");

        origval = xk_pg_parser_heap_getattr(&tuple_new->tuple,
                                                    natt + 1,
                                                    tupdesc,
                                                    &isnull,
                                                    &ismissing,
                                                    dbtype,
                                                    dbversion);
        if (isnull)
        {
            colvalue->m_info = INFO_COL_IS_NULL;
            continue;
        }
        if (ismissing)
        {
            colvalue->m_info = INFO_COL_MAY_NULL;
            continue;
        }
        zicinfo.dbtype = dbtype;
        zicinfo.dbversion = dbversion;
        colvalue->m_coltype = tupdesc->attrs[natt].atttypid;
        zicinfo.errorno = xk_pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (!xk_pg_parser_convert_attr_to_str_value(origval,
                                                    state->trans_data->m_sysdicts,
                                                    colvalue,
                                                    &zicinfo))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_NEW_VALUE_ATTR_TO_STR;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "ERROR: trans record is [heap update],"
                                    "new value convert attr to str failed\n");
            return false;
        }
    }

    if (have_oid)
    {
        char *result = NULL;
        if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void **) &result, 12))
            return (xk_pg_parser_Datum) 0;
        snprintf(result, 12, "%u", tuple_oid);
        update_record->m_new_values[tupdesc->natts].m_info = INFO_NOTHING;
        update_record->m_new_values[tupdesc->natts].m_freeFlag = true;
        update_record->m_new_values[tupdesc->natts].m_value = result;
        update_record->m_new_values[tupdesc->natts].m_valueLen = strlen(result);
        update_record->m_new_values[tupdesc->natts].m_coltype = XK_PG_SYSDICT_OIDOID;
        update_record->m_new_values[tupdesc->natts].m_colName = xk_pg_parser_mcxt_strndup("oid", 3);

        update_record->m_old_values[tupdesc->natts].m_info = INFO_NOTHING;
        update_record->m_old_values[tupdesc->natts].m_freeFlag = true;
        update_record->m_old_values[tupdesc->natts].m_value = xk_pg_parser_mcxt_strndup(result, 12);
        update_record->m_old_values[tupdesc->natts].m_valueLen = strlen(result);
        update_record->m_old_values[tupdesc->natts].m_coltype = XK_PG_SYSDICT_OIDOID;
        update_record->m_old_values[tupdesc->natts].m_colName = xk_pg_parser_mcxt_strndup("oid", 3);
    }

    *result = (xk_pg_parser_translog_tbcolbase*)update_record;
    /* 释放用完的tuple_new, tuple_old, desc, tbinfo.pgattr */
    if (tuple_new)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_new);
    if (tuple_old)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_old);
    if (tupdesc)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    if (tbinfo.pgattr)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    if (zicinfo.zicdata)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    return true;
}

static bool xk_pg_parser_trans_rmgr_heap_update_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                                        xk_pg_parser_translog_tbcolbase **result,
                                                        int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *update_record = NULL;
    xk_pg_parser_xl_heap_update *xlrec = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple_new = NULL;
    xk_pg_parser_ReorderBufferTupleBuf *tuple_old = NULL;
    uint32_t relfilenode = 0;
    size_t datalen = 0;
    size_t tuplelen_old = 0;
    char *page_new = NULL;
    char *page_old = NULL;
    char *tupledata_new = NULL;
    int16_t dbtype = state->trans_data->m_dbtype;
    char *dbversion = state->trans_data->m_dbversion;

    xlrec = (xk_pg_parser_xl_heap_update *)xk_pg_parser_XLogRecGetData(state);
    /* 为返回值分配内存 */
    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                 (void **) (&update_record),
                                  sizeof(xk_pg_parser_translog_tbcol_values)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_11;
        xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }

    update_record->m_base.m_dmltype = XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE;
    update_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    if (XK_PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel
        || state->trans_data->m_iscatalog)
    {
        /* 有全页写取出tuple */
        if (xk_pg_parser_XLogRecHasBlockImage(state, 0))
        {
            xk_pg_parser_translog_translog2col *trans_data = state->trans_data;
            void *tuphdr_old = NULL;
            void *temp_tuple_new = NULL;

            if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                         (void **) &page_new,
                                          trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_12;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* 组装完整page */
            if (!xk_pg_parser_image_get_block_image(state,
                                                    0,
                                                    page_new,
                                                    trans_data->m_pagesize))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_IMAGE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "make image failed\n");
                return false;
            }
            /* 处理要返回的全页写数据, 从全页写中提取tuple */
            update_record->m_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                                                        dbtype,
                                                                        state->trans_data->m_dbversion,
                                                                        state->trans_data->m_pagesize,
                                                                        page_new,
                                                                        &update_record->m_tupleCnt,
                                                                        state->blocks[0].blkno,
                                                                        state->trans_data->m_debugLevel);
            if (!update_record->m_tuple)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "make return tuple failed\n");
                return false;
            }
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                    "DEBUG: trans record is [heap update], get %u tuples from image\n",
                                    update_record->m_tupleCnt);
            /*设置返回类型*/
            update_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            update_record->m_relfilenode = relfilenode;

            /* 判断新旧tuple是否在同一个页块中 */
            if (0 == state->max_block_id)
            {
                /* 在同一个页块时 */
                void *temp_tuple_old = NULL;
                page_old = page_new;
                temp_tuple_old = (void *)xk_pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page_old,
                                                xlrec->old_offnum);

                tuple_old = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old;
            }
            else
            {
                xk_pg_parser_translog_tuplecache *current_tuplecache = NULL;
                void *temp_tuple_old = NULL;

                /* 不在同一个页块时, 使用传入的tuple, 检查传入的页数据 */
                if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap update],"
                                            "input tuple check failed\n");
                    return false;
                }
                current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                     trans_data->m_tuplecnt,
                                                                     xlrec->old_offnum,
                                                                     state->blocks[1].blkno);
                if (!current_tuplecache)
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap update],"
                                            "off:%u, block:%u,"
                                            " input tuple check failed\n", xlrec->old_offnum, state->blocks[0].blkno);
                    return false;
                }
                tuphdr_old = (void*) current_tuplecache->m_tupledata;
                tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
                temp_tuple_old = xk_pg_parser_heaptuple_get_tuple_space(tuplelen_old, dbtype, dbversion);
                if (!temp_tuple_old)
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                    xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                            "ERROR: trans record is [heap update],"
                                            "get old tuple failed\n");
                    return false;
                }
                xk_pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                                     tuplelen_old,
                                                                     (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old,
                                                                     dbtype,
                                                                     dbversion);

                tuple_old = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old;
            }
            temp_tuple_new = (void *)xk_pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                state->trans_data->m_dbversion,
                                                state->trans_data->m_pagesize,
                                                page_new,
                                                xlrec->new_offnum);
            if (!temp_tuple_new)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "get new tuple failed\n");
                return false;
            }

            tuple_new = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_new;

             /* 全页写数据已经提取, tuple也组装完毕, 释放page */
            if (page_new)
            {
                xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page_new);
                page_new = NULL;
            }
        }
        /* 没有全页写的情况下, 旧元组我们需要使用传进的页数据 */
        else
        {
            void *tuphdr_old = NULL;
            void *temp_tuple_old = NULL;
            void *temp_tuple_new = NULL;
            size_t temp_len = 0;
            uint32_t temp_tuplen = 0;
            xk_pg_parser_translog_translog2col *trans_data = state->trans_data;

            xk_pg_parser_translog_tuplecache *current_tuplecache = NULL;
            /* 使用传入的tuple, 检查传入的页数据 */
            if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "input tuple check failed\n");
                return false;
            }
            if (0 == state->max_block_id)
                current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                 trans_data->m_tuplecnt,
                                                                 xlrec->old_offnum,
                                                                 state->blocks[0].blkno);
            else
                current_tuplecache = xk_pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                 trans_data->m_tuplecnt,
                                                                 xlrec->old_offnum,
                                                                 state->blocks[1].blkno);
            if (!current_tuplecache)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "input tuple check failed\n");
                return false;
            }
            tuphdr_old = (void*) current_tuplecache->m_tupledata;
            tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
            temp_len = tuplelen_old - xk_pg_parser_SizeOfHeapHeader;
            temp_tuple_old = (void *)xk_pg_parser_heaptuple_get_tuple_space(temp_len, dbtype, dbversion);
            if (!temp_tuple_old)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "get old tuple failed\n");
                return false;
            }
            xk_pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                tuplelen_old,
                                                (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old,
                                                dbtype,
                                                dbversion);
            tuple_old = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_old;
            /* 结合旧数据, 从record中组装出new tuple */
            tupledata_new = xk_pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata_new)
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "get new tuple failed\n");
                return false;
            }
            if (!reassemble_tuplenew_from_wal_data(tupledata_new,
                                                   datalen,
                                                   (xk_pg_parser_ReorderBufferTupleBuf **)(&temp_tuple_new),
                                                   xlrec,
                                                   state->blocks[0].blkno,
                                                   xk_pg_parser_InvalidTransactionId,
                                                   tuphdr_old,
                                                   tuplelen_old,
                                                   dbtype,
                                                   dbversion))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_MAKE_NEW_TUPLE_FROM_OLD;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update],"
                                        "make new tuple from old tuple failed\n");
                return false;
            }
            tuple_new = (xk_pg_parser_ReorderBufferTupleBuf *)temp_tuple_new;
            /*设置返回类型, update返回新tuple, 旧tuple无需返回*/
            update_record->m_base.m_type |= XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(update_record->m_tuple),
                                     sizeof(xk_pg_parser_translog_tuplecache)))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_13;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* 处理要返回的tuple数据 */
            update_record->m_tuple->m_pageno = state->blocks[0].blkno;
            update_record->m_tupleCnt = 1;
            update_record->m_relfilenode = relfilenode;
            temp_tuplen = tuple_new->tuple.t_len;
            update_record->m_tuple->m_tuplelen = temp_tuplen;
            update_record->m_tuple->m_itemoffnum = xlrec->new_offnum;
            xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "DEBUG: trans record is [heap insert], return tuple "
                                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                        update_record->m_tuple->m_itemoffnum,
                                        update_record->m_tuple->m_pageno,
                                        update_record->m_tuple->m_tuplelen);
            if(!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void**)&(update_record->m_tuple->m_tupledata),
                                     temp_tuplen))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_14;
                xk_pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                        "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            rmemcpy0(update_record->m_tuple->m_tupledata,
                     0,
                     tuple_new->tuple.t_data,
                     tuple_new->tuple.t_len);

        }
    }

    *result = (xk_pg_parser_translog_tbcolbase*)update_record;
    /* 释放用完的tuple_new, tuple_old, desc, tbinfo.pgattr */
    if (tuple_new)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_new);
    if (tuple_old)
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_old);

    return true;
}

bool xk_pg_parser_check_fpw(xk_pg_parser_trans_transrec_decode_XLogReaderState *readstate,
                            xk_pg_parser_translog_pre_base **xk_pg_parser_result,
                            int32_t *xk_pg_parser_errno,
                            int16_t dbtype)
{
    xk_pg_parser_translog_pre_image_tuple *pre_tuple = NULL;
    uint8_t info = readstate->decoded_record->xl_info;
    int32_t blcknum = 0;
    bool hasimage = false;

    info &= ~XK_PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;

    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
    XK_PG_PARSER_UNUSED(dbtype);

    if (!(XK_PG_PARSER_TRANSLOG_RMGR_HEAP_ID == readstate->decoded_record->xl_rmid
         || XK_PG_PARSER_TRANSLOG_RMGR_HEAP2_ID == readstate->decoded_record->xl_rmid
         || XK_PG_PARSER_TRANSLOG_RMGR_XLOG_ID == readstate->decoded_record->xl_rmid))
        return false;

    for (blcknum = 0; blcknum < readstate->max_block_id + 1; blcknum++)
    {
        if (xk_pg_parser_XLogRecHasBlockImage(readstate, blcknum) && readstate->blocks[blcknum].in_use)
        {
            hasimage = true;
            break;
        }
    }
    if (!hasimage)
        return false;

    if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                (void **) &pre_tuple,
                                sizeof(xk_pg_parser_translog_pre_image_tuple)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_18;
        xk_pg_parser_log_errlog(readstate->trans_data->m_debugLevel,
                                "ERROR: check FPW, MALLOC failed\n");
        return false;
    }

    for (blcknum = 0; blcknum < readstate->max_block_id + 1; blcknum++)
    {
        char *page = NULL;
        uint32_t pageno = 0;
        xk_pg_parser_translog_tuplecache *temp_tuple = NULL;
        uint32_t block_tuple_cnt = 0;

        if (!xk_pg_parser_XLogRecHasBlockImage(readstate, blcknum) || !readstate->blocks[blcknum].in_use)
        {
            continue;
        }

        if (!xk_pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                    (void **) &page,
                                    readstate->pre_trans_data->m_pagesize))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_17;
            xk_pg_parser_log_errlog(readstate->trans_data->m_debugLevel,
                                    "ERROR: check FPW, MALLOC failed\n");
            return false;
        }
        /* 组装完整page */
        if (!xk_pg_parser_image_get_block_image(readstate,
                                                blcknum,
                                                page,
                                                readstate->pre_trans_data->m_pagesize))
        {
            xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_IMAGE_FPW;
            return false;
        }

        pageno = readstate->blocks[blcknum].blkno;

        /* 从页中提取tuple */
        temp_tuple = xk_pg_parser_image_get_tuple_from_image_with_dbtype(readstate->pre_trans_data->m_dbtype,
                                                                         readstate->pre_trans_data->m_dbversion,
                                                                         readstate->pre_trans_data->m_pagesize,
                                                                         page,
                                                                         &block_tuple_cnt,
                                                                         pageno,
                                                                         readstate->pre_trans_data->m_debugLevel);
        if (!temp_tuple)
        {
            /* 如果这个页里没有获取到有效tuple, 不报错并继续 */
            xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
            xk_pg_parser_log_errlog(readstate->pre_trans_data->m_debugLevel,
                                    "DEBUG: pre trans record is [xlog/heap/heap2 FPW],"
                                    "try get tuples from image, but get NULL\n");
            continue;
        }
        xk_pg_parser_log_errlog(readstate->pre_trans_data->m_debugLevel,
                                        "DEBUG: trans record is [xlog/heap/heap2 FPW], get %u tuples from image\n",
                                        block_tuple_cnt);
        if (pre_tuple->m_tuples)
        {
            xk_pg_parser_mcxt_realloc(TRANS_RMGR_HEAP_MCXT,
                                     (void **)&pre_tuple->m_tuples,
                                     (pre_tuple->m_tuplecnt + block_tuple_cnt) * sizeof(xk_pg_parser_translog_tuplecache));
            rmemcpy1((char*)(pre_tuple->m_tuples),
                     pre_tuple->m_tuplecnt * sizeof(xk_pg_parser_translog_tuplecache),
                     temp_tuple,
                     block_tuple_cnt * sizeof(xk_pg_parser_translog_tuplecache));
            pre_tuple->m_tuplecnt += block_tuple_cnt;
            xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, temp_tuple);
        }
        else
        {
            pre_tuple->m_tuplecnt = block_tuple_cnt;
            pre_tuple->m_tuples = temp_tuple;
        }
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
    }

    /* 如果这个页里没有获取到有效tuple, 不报错并返回false */
    if (!pre_tuple->m_tuples)
    {
        xk_pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, pre_tuple);
        return false;
    }

    /* 处理要返回的tuple数据的relfilenode信息*/
    pre_tuple->m_relfilenode = readstate->blocks[0].rnode.relNode;
    pre_tuple->m_dboid = readstate->blocks[0].rnode.dbNode;
    pre_tuple->m_tbspcoid = readstate->blocks[0].rnode.spcNode;

    pre_tuple->m_transid = readstate->decoded_record->xl_xid;
    /*设置返回类型*/
    pre_tuple->m_base.m_type = XK_PG_PARSER_TRANSLOG_FPW_TUPLE;
    pre_tuple->m_base.m_xid = xk_pg_parser_XLogRecGetXid(readstate);
    pre_tuple->m_base.m_originid = readstate->record_origin;
    *xk_pg_parser_result = (xk_pg_parser_translog_pre_base*) pre_tuple;

    return true;
}

static bool reassemble_tuplenew_from_wal_data(char *data,
                                              size_t len,
                                              xk_pg_parser_ReorderBufferTupleBuf **result_new,
                                              xk_pg_parser_xl_heap_update *xlrec,
                                              uint32_t blknum_new,
                                              xk_pg_parser_TransactionId xid,
                                              xk_pg_parser_HeapTupleHeader htup_old_in,
                                              int32_t old_tuple_len,
                                              int16_t dbtype,
                                              char *dbversion)
{
    xk_pg_parser_xl_heap_header xlhdr;
    xk_pg_parser_HeapTupleHeader htup;             /* 组装新 tuple 时，便于 coding 的指针。 */
    xk_pg_parser_ReorderBufferTupleBuf *recorbuff_new = NULL;
    xk_pg_parser_HeapTupleHeader htup_old = htup_old_in;
    char *newp = NULL;
    char *recdata, *recdata_end;
    int suffixlen = 0;
    int prefixlen = 0;
    int tuplen = 0; /* 记录 update wal record 中用户数据的长度。 */
    int reass_tuple_len = 0;
    xk_pg_parser_ItemPointerData target_tid;

    recdata = data;
    recdata_end = data + len;

    if (xlrec->flags & XK_PG_PARSER_TRANS_XLH_UPDATE_PREFIX_FROM_OLD)
    {
        rmemcpy1(&prefixlen, 0, recdata, sizeof(uint16_t));
        recdata += sizeof(uint16_t);
    }
    if (xlrec->flags & XK_PG_PARSER_TRANS_XLH_UPDATE_SUFFIX_FROM_OLD)
    {
        rmemcpy1(&suffixlen, 0, recdata, sizeof(uint16_t));
        recdata += sizeof(uint16_t);
    }
    rmemcpy1((char *)&xlhdr, 0, recdata, xk_pg_parser_SizeOfHeapHeader);
    recdata += xk_pg_parser_SizeOfHeapHeader;

    tuplen = recdata_end - recdata;
    reass_tuple_len = xk_pg_parser_SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;

    recorbuff_new = xk_pg_parser_heaptuple_get_tuple_space(tuplen + prefixlen + suffixlen, dbtype, dbversion);
    if (!recorbuff_new)
        return false;
    recorbuff_new->tuple.t_len = reass_tuple_len;

    htup = recorbuff_new->tuple.t_data;

    xk_pg_parser_ItemPointerSetInvalid(&recorbuff_new->tuple.t_self);
    xk_pg_parser_ItemPointerSet(&target_tid, blknum_new, xlrec->new_offnum);

    /* we can only figure this out after reassembling the transactions. */
    recorbuff_new->tuple.t_tableOid = xk_pg_parser_InvalidOid;

    htup = (xk_pg_parser_HeapTupleHeader)recorbuff_new->tuple.t_data;

    newp = (char *)htup + xk_pg_parser_SizeofHeapTupleHeader;

    if (prefixlen > 0)
    {
        int len;

        /* 拷贝实际数据前的填充部分。 */
        len = xlhdr.t_hoff - xk_pg_parser_SizeofHeapTupleHeader;
        rmemcpy1(newp, 0, recdata, len);
        recdata += len;
        newp += len;

        /* copy prefix from old tuple. */
        rmemcpy1(newp, 0, (char *)htup_old + htup_old->t_hoff, prefixlen);
        newp += prefixlen;

        /* copy new tuple data from WAL record. */
        len = tuplen - (xlhdr.t_hoff - xk_pg_parser_SizeofHeapTupleHeader);
        rmemcpy1(newp, 0, recdata, len);
        recdata += len;
        newp += len;
    }
    else
    {
        /*
        * copy bitmap [+ padding] [+ oid] + data from record.
        */
        rmemcpy1(newp, 0, recdata, tuplen);
        recdata += tuplen;
        newp += tuplen;
    }

    /* copy suffix from old tuple. */
    if (suffixlen > 0)
        rmemcpy1(newp, 0, (char *)htup_old + old_tuple_len - suffixlen, suffixlen);

    htup->t_infomask2 = xlhdr.t_infomask2;
    htup->t_infomask = xlhdr.t_infomask;
    htup->t_hoff = xlhdr.t_hoff;
    xk_pg_parser_HeapTupleHeaderSetXmin(htup, xid);
    xk_pg_parser_HeapTupleHeaderSetCmin(htup, xk_pg_parser_FirstCommandId);
    xk_pg_parser_HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
    /* Make sure there is no forward chain link in t_ctid. */
    htup->t_ctid = target_tid;
    *result_new = recorbuff_new;

    return true;
}

static void reassemble_mutituple_from_wal_data(char *data, size_t len,
                                   xk_pg_parser_ReorderBufferTupleBuf *tup,
                                   xk_pg_parser_xl_multi_insert_tuple *xlhdr,
                                   int16_t dbtype,
                                   char *dbversion)
{
    xk_pg_parser_HeapTupleHeader header;
    xk_pg_parser_ReorderBufferTupleBuf *tuple = tup;
    header = tuple->tuple.t_data;
    xk_pg_parser_ItemPointerSetInvalid(&tuple->tuple.t_self);
    tuple->tuple.t_tableOid = xk_pg_parser_InvalidOid;
    tuple->tuple.t_len = len + xk_pg_parser_SizeofHeapTupleHeader;

    rmemset1(header, 0, 0, xk_pg_parser_SizeofHeapTupleHeader);

    rmemcpy1(((char *)tuple->tuple.t_data) + xk_pg_parser_SizeofHeapTupleHeader,
             0,
             data,
             len);

    header->t_infomask = xlhdr->t_infomask;
    header->t_infomask2 = xlhdr->t_infomask2;
    header->t_hoff = xlhdr->t_hoff;
}
