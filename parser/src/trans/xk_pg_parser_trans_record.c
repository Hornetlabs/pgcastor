#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_xlog/xk_pg_parser_trans_rmgr_xlog.h"
#include "trans/rmgr_xact/xk_pg_parser_trans_rmgr_xact.h"
#include "trans/rmgr_relmap/xk_pg_parser_trans_rmgr_relmap.h"
#include "trans/rmgr_heap_heap2/xk_pg_parser_trans_rmgr_heap_heap2.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_itemptr.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_heaptuple.h"
#include "image/xk_pg_parser_image.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_fmgr.h"
#include "trans/rmgr_standby/xk_pg_parser_trans_rmgr_standby.h"
#include "trans/rmgr_seq/xk_pg_parser_trans_rmgr_seq.h"

/* DEFINE */
#define XK_PG_PARSER_RMGRCNTS 7
#define XK_PG_PARSER_GETTUPLE_RMGRCNTS 2

#define PARSER_MCXT NULL
/* typedef statement */

/* 定义预处理FMGR的函数指针 */
typedef bool (*xk_pg_parser_trans_transrec_rmgrfunc_pre)(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result,
                            int32_t *xk_pg_parser_errno);

/* 定义二次解析时处理FMGR的函数指针 */
typedef bool (*xk_pg_parser_trans_transrec_rmgrfunc_trans)(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno);

/* static function statement */
static bool xk_pg_parser_trans_transrec_decode_checkPreTransParam(xk_pg_parser_translog_pre
                                                                 *xk_pg_parser_pre_data);

static bool deal_invalid_record(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                xk_pg_parser_translog_pre_base **xk_pg_parser_result,
                                int32_t *xk_pg_parser_errno);

typedef struct XK_PG_PARSER_TRANS_RMGR
{
    xk_pg_parser_trans_rmgr_enum                 m_rmgrid;           /* rmgrid值 */
    xk_pg_parser_trans_transrec_rmgrfunc_pre     m_rmgrfunc_pre;     /* 预解析接口rmgr级的处理函数 */
    xk_pg_parser_trans_transrec_rmgrfunc_trans   m_rmgrfunc_trans;    /* 二次解析时函数处理接口 */
} xk_pg_parser_trans_rmgr;

/**
 * @brief        rmgid 分发
 */
static xk_pg_parser_trans_rmgr m_record_rmgr[] =
{
    { XK_PG_PARSER_TRANSLOG_RMGR_XLOG_ID, xk_pg_parser_trans_rmgr_xlog_pre, NULL},
    { XK_PG_PARSER_TRANSLOG_RMGR_XACT_ID, xk_pg_parser_trans_rmgr_xact_pre, NULL},
    { XK_PG_PARSER_TRANSLOG_RMGR_RELMAP_ID, xk_pg_parser_trans_rmgr_relmap_pre, NULL},
    { XK_PG_PARSER_TRANSLOG_RMGR_STANDBY_ID, xk_pg_parser_trans_rmgr_standby_pre, NULL},
    { XK_PG_PARSER_TRANSLOG_RMGR_HEAP2_ID, xk_pg_parser_trans_rmgr_heap2_pre, xk_pg_parser_trans_rmgr_heap2_trans},
    { XK_PG_PARSER_TRANSLOG_RMGR_HEAP_ID, xk_pg_parser_trans_rmgr_heap_pre, xk_pg_parser_trans_rmgr_heap_trans},
    { XK_PG_PARSER_TRANSLOG_RMGR_SEQ_ID, xk_pg_parser_trans_rmgr_seq_pre, NULL}
};

static xk_pg_parser_trans_rmgr m_record_rmgr_get_tuple[] =
{
    { XK_PG_PARSER_TRANSLOG_RMGR_HEAP2_ID, NULL, xk_pg_parser_trans_rmgr_heap2_trans_get_tuple},
    { XK_PG_PARSER_TRANSLOG_RMGR_HEAP_ID, NULL, xk_pg_parser_trans_rmgr_heap_trans_get_tuple}
};

#if 0
/**
 * @brief        rmgid kingbase分发
 */
static xk_pg_parser_trans_rmgr m_record_rmgr_kingbase[] =
{
    { XK_PG_PARSER_TRANSLOG_KINGBASE_RMGR_XLOG_ID, xk_pg_parser_trans_rmgr_xlog_pre, NULL},
    { XK_PG_PARSER_TRANSLOG_KINGBASE_RMGR_XACT_ID, xk_pg_parser_trans_rmgr_xact_pre, NULL},
    { XK_PG_PARSER_TRANSLOG_KINGBASE_RMGR_HEAP2_ID, xk_pg_parser_trans_rmgr_heap2_pre, xk_pg_parser_trans_rmgr_heap2_trans},
    { XK_PG_PARSER_TRANSLOG_KINGBASE_RMGR_HEAP_ID, xk_pg_parser_trans_rmgr_heap_pre, xk_pg_parser_trans_rmgr_heap_trans}
};
#endif

/* 预解析接口 */
//名称加ref in 标记出入参
bool xk_pg_parser_trans_preTrans(xk_pg_parser_translog_pre *xk_pg_parser_pre_data,
                                 xk_pg_parser_translog_pre_base **xk_pg_parser_result,
                                 int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_trans_transrec_decode_XLogReaderState *readstate = NULL;
    int32_t                       index = 0;
    int8_t                        rmgrcnts = XK_PG_PARSER_RMGRCNTS;
    xk_pg_parser_XLogRecord      *record = (xk_pg_parser_XLogRecord *) xk_pg_parser_pre_data->m_record;
    bool                          pre_trans_result = false;
    /* 检查传入的参数放在JNI */
    //todo
    if (!xk_pg_parser_trans_transrec_decode_checkPreTransParam(xk_pg_parser_pre_data))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_PRE_CHECK;
        return false;
    }
    /* 准备recordstate, 分配内存 */
    readstate = xk_pg_parser_trans_transrec_decode_XLogReader_Allocate();

    if (NULL == readstate)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_DECODESTATE;
        return false;
    }
    readstate->xk_pg_parser_errno = xk_pg_parser_errno;
    readstate->pre_trans_data = xk_pg_parser_pre_data;

    /* 预解析时直接解析record, 数据保存在readstate中 */
    if (!xk_pg_parser_trans_transrec_decodeXlogRecord(readstate,
                                                      record,
                                                      xk_pg_parser_pre_data->m_pagesize,
                                                      xk_pg_parser_errno,
                                                      true,
                                                      xk_pg_parser_pre_data->m_dbtype,
                                                      xk_pg_parser_pre_data->m_dbversion))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_PRE_DECODE;
        return false;
    }

    *xk_pg_parser_errno = XK_ERRNO_SUCCESS;

    /* 调用以RMGRID分类的解析函数 */
    for (index = 0; index < rmgrcnts; index++)
    {
        if (m_record_rmgr[index].m_rmgrid != record->xl_rmid)
        {
            continue;
        }
        pre_trans_result = m_record_rmgr[index].m_rmgrfunc_pre(readstate,
                                                           xk_pg_parser_result,
                                                           xk_pg_parser_errno);
        break;
    }
    if (false == pre_trans_result)
    {
        /* 当错误码不为0时, 在rmgr或info处理时发生了错误 */
        if (XK_ERRNO_SUCCESS != *xk_pg_parser_errno)
            return false;
        /* 如果无需解析的record中有全页写, 需要保存 */
        if (!xk_pg_parser_check_fpw(readstate, xk_pg_parser_result, xk_pg_parser_errno, xk_pg_parser_pre_data->m_dbtype))
        {
            if (XK_ERRNO_SUCCESS != *xk_pg_parser_errno)
            {
                xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
                return false;
            }
            /* 错误码为0时代表没有捕捉到有效地的rmgr, 同时没有捕获到全页写, 此条record不需要解析 */
            deal_invalid_record(readstate, xk_pg_parser_result, xk_pg_parser_errno);
        }
    }
    /* 释放readstate */
    if (readstate)
        xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
    return true;
}

static bool xk_pg_parser_trans_transrec_decode_checkPreTransParam(xk_pg_parser_translog_pre *xk_pg_parser_pre_data)
{
    if (XK_PG_PARSER_DEBUG_SILENCE > xk_pg_parser_pre_data->m_debugLevel
        || XK_PG_PARSER_WALLEVEL_REPLICA > xk_pg_parser_pre_data->m_walLevel
        || XK_PG_PARSER_WALLEVEL_LOGICAL < xk_pg_parser_pre_data->m_walLevel
        || NULL == xk_pg_parser_pre_data->m_record)
    {
        return false;
    }

    if (0 < (xk_pg_parser_pre_data->m_pagesize % 2))
    {
        return false;
    }

    return true;
}

static bool deal_invalid_record(xk_pg_parser_trans_transrec_decode_XLogReaderState *readstate,
                                xk_pg_parser_translog_pre_base **xk_pg_parser_result,
                                int32_t *xk_pg_parser_errno)
{
    if (!xk_pg_parser_mcxt_malloc(PARSER_MCXT,
                                 (void **) (xk_pg_parser_result),
                                  sizeof(xk_pg_parser_translog_pre_base)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_00;
        return false;
    }
    (*xk_pg_parser_result)->m_type = XK_PG_PARSER_TRANSLOG_INVALID;
    (*xk_pg_parser_result)->m_originid = readstate->record_origin;
    (*xk_pg_parser_result)->m_xid = readstate->decoded_record->xl_xid;
    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_USELESS;
    return true;
}

/* 二次解析接口 */
bool xk_pg_parser_trans_TransRecord(xk_pg_parser_translog_translog2col *xk_pg_parser_transData,
                                    xk_pg_parser_translog_tbcolbase **xk_pg_parser_trans_result,
                                    int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_trans_transrec_decode_XLogReaderState *readstate = NULL;
    xk_pg_parser_XLogRecord      *record = (xk_pg_parser_XLogRecord *)
                                            xk_pg_parser_transData->m_record;
    bool    result = false;
    int32_t index = 0;
    int8_t  rmgrcnts = XK_PG_PARSER_RMGRCNTS;

    readstate = xk_pg_parser_trans_transrec_decode_XLogReader_Allocate();
    if (NULL == readstate)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_DECODESTATE;
        return false;
    }
    readstate->xk_pg_parser_errno = xk_pg_parser_errno;
    /* 预解析时直接解析record, 数据保存在readstate中 */
    if (!xk_pg_parser_trans_transrec_decodeXlogRecord(readstate,
                                                      record,
                                                      xk_pg_parser_transData->m_pagesize,
                                                      xk_pg_parser_errno,
                                                      false,
                                                      xk_pg_parser_transData->m_dbtype,
                                                      xk_pg_parser_transData->m_dbversion))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_PRE_DECODE;
        xk_pg_parser_log_errlog(xk_pg_parser_transData->m_debugLevel,
                                "ERROR: in trans record, decode failed\n");
        return false;
    }
    readstate->trans_data = xk_pg_parser_transData;
    *xk_pg_parser_errno = XK_ERRNO_SUCCESS;
    xk_pg_parser_log_errlog(xk_pg_parser_transData->m_debugLevel,
                                    "DEBUG: input param:\n"
                                    "m_walLevel: %hhu\n"
                                    "m_debugLevel: %hhu\n"
                                    "m_dbtype: %hd\n"
                                    "m_pagesize: %u\n"
                                    "m_tuplecnt: %u\n"
                                    "m_dbversion: %s\n"
                                    "m_sysdicts:\n"
                                    "    class: %d\n"
                                    "    attribute: %d\n"
                                    "    namespace: %d\n"
                                    "    type: %d\n"
                                    "    range: %d\n"
                                    "    enum: %d\n"
                                    "    proc: %d\n"
                                    "m_dbcharset: %s\n"
                                    "m_tartgetcharset: %s\n",
                                    xk_pg_parser_transData->m_walLevel,
                                    xk_pg_parser_transData->m_debugLevel,
                                    xk_pg_parser_transData->m_dbtype,
                                    xk_pg_parser_transData->m_pagesize,
                                    xk_pg_parser_transData->m_tuplecnt,
                                    xk_pg_parser_transData->m_dbversion ? xk_pg_parser_transData->m_dbversion : "NULL",
                                    xk_pg_parser_transData->m_sysdicts->m_pg_class.m_count,
                                    xk_pg_parser_transData->m_sysdicts->m_pg_attribute.m_count,
                                    xk_pg_parser_transData->m_sysdicts->m_pg_namespace.m_count,
                                    xk_pg_parser_transData->m_sysdicts->m_pg_type.m_count,
                                    xk_pg_parser_transData->m_sysdicts->m_pg_range.m_count,
                                    xk_pg_parser_transData->m_sysdicts->m_pg_enum.m_count,
                                    xk_pg_parser_transData->m_sysdicts->m_pg_proc.m_count,
                                    xk_pg_parser_transData->m_convert->m_dbcharset,
                                    xk_pg_parser_transData->m_convert->m_tartgetcharset);
    /* 调用以RMGRID分类的解析函数 */

    for (index = 0; index < rmgrcnts; index++)
    {
        if (m_record_rmgr[index].m_rmgrid != record->xl_rmid)
        {
            continue;
        }
        if (m_record_rmgr[index].m_rmgrfunc_trans == NULL)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_RMGR_NOT_FIND;
            xk_pg_parser_log_errlog(xk_pg_parser_transData->m_debugLevel, 
                                   "ERROR: RMGR MANAGER,"
                                   "can't find trans func by rmgrid:%u\n", record->xl_rmid);
            return false;
        }
        result = m_record_rmgr[index].m_rmgrfunc_trans(readstate,
                                                    xk_pg_parser_trans_result,
                                                    xk_pg_parser_errno);
        break;
    }

    if (false == result)
    {
        /* 当错误码不为0时, 在rmgr或info处理时发生了错误 */
        if (XK_ERRNO_SUCCESS != *xk_pg_parser_errno)
        {
            xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
                return false;
        }
    }
    /* 释放readstate */
    xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
    return true;
}

bool xk_pg_parser_trans_TransRecord_GetTuple(xk_pg_parser_translog_translog2col *xk_pg_parser_transData,
                                             xk_pg_parser_translog_tbcolbase **xk_pg_parser_trans_result,
                                             int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_trans_transrec_decode_XLogReaderState *readstate = NULL;
    xk_pg_parser_XLogRecord      *record = (xk_pg_parser_XLogRecord *)
                                            xk_pg_parser_transData->m_record;
    bool    result = false;
    int32_t index = 0;
    int8_t  rmgrcnts = XK_PG_PARSER_GETTUPLE_RMGRCNTS;

    readstate = xk_pg_parser_trans_transrec_decode_XLogReader_Allocate();
    if (NULL == readstate)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_DECODESTATE;
        return false;
    }
    readstate->xk_pg_parser_errno = xk_pg_parser_errno;
    /* 预解析时直接解析record, 数据保存在readstate中 */
    if (!xk_pg_parser_trans_transrec_decodeXlogRecord(readstate,
                                                      record,
                                                      xk_pg_parser_transData->m_pagesize,
                                                      xk_pg_parser_errno,
                                                      false,
                                                      xk_pg_parser_transData->m_dbtype,
                                                      xk_pg_parser_transData->m_dbversion))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_PRE_FUNCERR_PRE_DECODE;
        xk_pg_parser_log_errlog(xk_pg_parser_transData->m_debugLevel,
                                "ERROR: in trans record, decode failed\n");
        return false;
    }
    readstate->trans_data = xk_pg_parser_transData;
    *xk_pg_parser_errno = XK_ERRNO_SUCCESS;

    /* 调用以RMGRID分类的解析函数 */

    for (index = 0; index < rmgrcnts; index++)
    {
        if (m_record_rmgr_get_tuple[index].m_rmgrid != record->xl_rmid)
        {
            continue;
        }
        if (m_record_rmgr_get_tuple[index].m_rmgrfunc_trans == NULL)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_TRANS_FUNCERR_RMGR_NOT_FIND;
            xk_pg_parser_log_errlog(xk_pg_parser_transData->m_debugLevel, 
                                   "ERROR: RMGR MANAGER,"
                                   "can't find trans func by rmgrid:%u\n", record->xl_rmid);
            return false;
        }
        result = m_record_rmgr_get_tuple[index].m_rmgrfunc_trans(readstate,
                                                    xk_pg_parser_trans_result,
                                                    xk_pg_parser_errno);
        break;
    }

    if (false == result)
    {
        /* 当错误码不为0时, 在rmgr或info处理时发生了错误 */
        if (XK_ERRNO_SUCCESS != *xk_pg_parser_errno)
        {
            xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
                return false;
        }
    }
    /* 释放readstate */
    xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
    return true;
}

/* DDL解析接口 */
bool xk_pg_parser_trans_DDLtrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                 xk_pg_parser_translog_ddlstmt **xk_pg_parser_ddl_result,
                                 int32_t *xk_pg_parser_errno)
{
    if (!xk_pg_parser_DDL_transRecord2DDL(xk_pg_parser_ddl, xk_pg_parser_ddl_result, xk_pg_parser_errno))
        return false;
    return true;
}

/* external行外存储解析接口 */
bool xk_pg_parser_trans_external_trans(xk_pg_parser_translog_external *xk_pg_parser_exdata,
                                       xk_pg_parser_translog_tbcol_value **xk_pg_parser_trans_result,
                                       int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_value *result = NULL;
    xk_pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);
    if (!xk_pg_parser_mcxt_malloc(PARSER_MCXT,
                                  (void **) &result,
                                  sizeof(xk_pg_parser_translog_tbcol_value)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_EXTERNAL_MEMERR_ALLOC_00;
        xk_pg_parser_log_errlog(xk_pg_parser_exdata->m_debuglevel,
                                "ERROR: EXTERNAL PARSER,"
                                "malloc failed\n");
        return false;
    }
    zicinfo.convertinfo = xk_pg_parser_exdata->m_convertInfo;
    result->m_colName = rstrdup(xk_pg_parser_exdata->m_colName);
    result->m_freeFlag = true;
    result->m_coltype = xk_pg_parser_exdata->m_typeid;
    zicinfo.dbtype = xk_pg_parser_exdata->m_dbtype;
    zicinfo.dbversion = xk_pg_parser_exdata->m_dbversion;
    zicinfo.errorno = xk_pg_parser_errno;
    zicinfo.debuglevel = xk_pg_parser_exdata->m_debuglevel;
    if (!xk_pg_parser_convert_attr_to_str_external_value((xk_pg_parser_Datum)xk_pg_parser_exdata->m_chunkdata,
                                                    xk_pg_parser_exdata->m_typout,
                                                    result,
                                                    &zicinfo))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_EXTERNAL_MEMERR_ALLOC_01;
        xk_pg_parser_log_errlog(xk_pg_parser_exdata->m_debuglevel,
                                "ERROR: EXTERNAL PARSER,"
                                "malloc failed\n");
        return false;
    }
    *xk_pg_parser_trans_result = result;
    return true;
}

void xk_pg_parser_trans_preTrans_free(xk_pg_parser_translog_pre_base *xk_pg_parser_result)
{
    if (xk_pg_parser_result)
    {
        if (XK_PG_PARSER_TRANSLOG_XACT_COMMIT == (xk_pg_parser_result)->m_type
         || XK_PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE == (xk_pg_parser_result)->m_type)
        {
            xk_pg_parser_translog_pre_trans *trans = (xk_pg_parser_translog_pre_trans *)
                                                      xk_pg_parser_result;
            xk_pg_parser_xl_xact_parsed_commit *commit = (xk_pg_parser_xl_xact_parsed_commit *)
                                                          trans->m_transdata;
            if (commit)
            {
                if (commit->subxacts)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, commit->subxacts);
                if (commit->xnodes)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, commit->xnodes);
                if (commit->msgs)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, commit->msgs);
                xk_pg_parser_mcxt_free(PARSER_MCXT, commit);
            }

            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_result);
        }
        else if (XK_PG_PARSER_TRANSLOG_XACT_ABORT == (xk_pg_parser_result)->m_type
         || XK_PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE == (xk_pg_parser_result)->m_type)
        {
            xk_pg_parser_translog_pre_trans *trans = (xk_pg_parser_translog_pre_trans *)
                                                      xk_pg_parser_result;
            xk_pg_parser_xl_xact_parsed_abort *abort = (xk_pg_parser_xl_xact_parsed_abort *)
                                                          trans->m_transdata;

            if (abort)
            {
                if (abort->subxacts)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, abort->subxacts);
                if (abort->xnodes)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, abort->xnodes);
                xk_pg_parser_mcxt_free(PARSER_MCXT, abort);
            }

            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_result);
        }
        else if (XK_PG_PARSER_TRANSLOG_FPW_TUPLE == (xk_pg_parser_result)->m_type)
        {
            int32_t i = 0;
            xk_pg_parser_translog_pre_image_tuple *image = (xk_pg_parser_translog_pre_image_tuple *)
                                                            xk_pg_parser_result;
            for (i = 0; i < (int32_t) image->m_tuplecnt; i++)
                xk_pg_parser_mcxt_free(PARSER_MCXT, image->m_tuples[i].m_tupledata);
            xk_pg_parser_mcxt_free(PARSER_MCXT, image->m_tuples);
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_result);
        }
        else if (XK_PG_PARSER_TRANSLOG_RELMAP == (xk_pg_parser_result)->m_type)
        {
            xk_pg_parser_translog_pre_relmap *relmap = (xk_pg_parser_translog_pre_relmap *)
                                                            xk_pg_parser_result;
            if (relmap->m_count > 0 && relmap->m_mapping)
                xk_pg_parser_mcxt_free(PARSER_MCXT, relmap->m_mapping);
             xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_result);
        }
        else if (XK_PG_PARSER_TRANSLOG_RUNNING_XACTS == (xk_pg_parser_result)->m_type)
        {
            xk_pg_parser_translog_pre_running_xact *rxact = (xk_pg_parser_translog_pre_running_xact *)
                                                             xk_pg_parser_result;
            if (rxact)
            {
                if (rxact->m_standby)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, rxact->m_standby);
                xk_pg_parser_mcxt_free(PARSER_MCXT, rxact);
            }

        }
        else if (XK_PG_PARSER_TRANSLOG_HEAP_TRUNCATE == (xk_pg_parser_result)->m_type)
        {
            xk_pg_parser_translog_pre_heap_truncate *truncate = (xk_pg_parser_translog_pre_heap_truncate *)
                                                                 xk_pg_parser_result;
            if (truncate)
            {
                if (truncate->relids)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, truncate->relids);
                xk_pg_parser_mcxt_free(PARSER_MCXT, truncate);
            }
        }
        else
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_result);

    }
}
static void xk_pg_parser_free_node_tree(xk_pg_parser_nodetree *node)
{
    xk_pg_parser_nodetree *next_node = NULL;
    xk_pg_parser_nodetree *node_tree = node;
    while (node_tree)
    {
        next_node = node_tree->m_next;
        switch (node_tree->m_node_type)
        {
            case XK_PG_PARSER_NODETYPE_CONST:
            {
                xk_pg_parser_node_const *node_const = (xk_pg_parser_node_const *)node_tree->m_node;
                if (node_const->m_char_value)
                    xk_pg_parser_mcxt_free(NULL, node_const->m_char_value);
                xk_pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
            }
            case XK_PG_PARSER_NODETYPE_FUNC:
            {
                xk_pg_parser_node_func *node_func = (xk_pg_parser_node_func *)node_tree->m_node;
                if (node_func->m_funcname)
                    xk_pg_parser_mcxt_free(NULL, node_func->m_funcname);
                xk_pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
            }
            case XK_PG_PARSER_NODETYPE_OP:
            {
                xk_pg_parser_node_op *node_op = (xk_pg_parser_node_op *)node_tree->m_node;
                if (node_op->m_opname)
                    xk_pg_parser_mcxt_free(NULL, node_op->m_opname);
                xk_pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
            }
            default:
                xk_pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
        }
        xk_pg_parser_mcxt_free(NULL, node_tree);
        node_tree = next_node;
    }
}

static void xk_pg_parser_free_value(xk_pg_parser_translog_tbcol_value *value)
{
    if (value)
    {
        if (value->m_value)
        {
            if (value->m_info == INFO_COL_IS_CUSTOM
            || value->m_info == INFO_COL_IS_ARRAY)
            {
                xk_pg_parser_translog_tbcol_valuetype_customer *custom = 
                    (xk_pg_parser_translog_tbcol_valuetype_customer *) value->m_value;
                while (custom)
                {
                    xk_pg_parser_free_value(custom->m_value);
                    xk_pg_parser_mcxt_free(PARSER_MCXT, custom->m_value);
                    custom = custom->m_next;
                }
                xk_pg_parser_mcxt_free(PARSER_MCXT, value->m_value);
            }
            else if (value->m_info == INFO_COL_IS_NODE)
                xk_pg_parser_free_node_tree((xk_pg_parser_nodetree*)value->m_value);
            else
                xk_pg_parser_mcxt_free(PARSER_MCXT, value->m_value);
        }
        if (value->m_freeFlag)
            xk_pg_parser_mcxt_free(PARSER_MCXT, value->m_colName);
    }

}

void xk_pg_parser_free_value_ext(xk_pg_parser_translog_tbcol_value *value)
{
    xk_pg_parser_free_value(value);
}

void xk_pg_parser_trans_external_free(xk_pg_parser_translog_external *ext,
                                      xk_pg_parser_translog_tbcol_value *result)
{
    if (ext)
    {
        if (ext->m_chunkdata)
            xk_pg_parser_mcxt_free(PARSER_MCXT, ext->m_chunkdata);
        xk_pg_parser_mcxt_free(PARSER_MCXT, ext);
    }
    if (result)
    {
        if (result->m_value)
            xk_pg_parser_free_value(result);
        xk_pg_parser_mcxt_free(PARSER_MCXT, result);
    }
}

void xk_pg_parser_trans_ddl_free(xk_pg_parser_translog_systb2ddl *ddl,
                                 xk_pg_parser_translog_ddlstmt *result)
{
    if (ddl)
    {
        xk_pg_parser_translog_systb2dll_record *ddl_record = ddl->m_record;

        while (ddl_record)
        {
            xk_pg_parser_translog_systb2dll_record *ddl_record_temp = ddl_record;
            ddl_record = ddl_record->m_next;
            xk_pg_parser_mcxt_free(PARSER_MCXT, ddl_record_temp);
        }

        if (ddl->m_convert)
            xk_pg_parser_mcxt_free(PARSER_MCXT, ddl->m_convert);

        xk_pg_parser_mcxt_free(PARSER_MCXT, ddl);
    }
    if (result)
    {
        xk_pg_parser_translog_ddlstmt *result_temp = NULL;
        while (result)
        {
            if (result->m_ddlstmt && result->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_CREATE)
            {
                switch (result->m_base.m_ddlinfo)
                {
                    case XK_PG_PARSER_DDLINFO_CREATE_TABLE:
                    {
                        xk_pg_parser_translog_ddlstmt_createtable *create =
                            (xk_pg_parser_translog_ddlstmt_createtable *)result->m_ddlstmt;
                        /* m_cols中的指针内容都是复用的record中的指针, 无需释放, 因此仅释放m_cols */
                        if (create->m_cols)
                        {
                            int index_col = 0;
                            for (index_col = 0; index_col < create->m_colcnt; index_col++)
                            {
                                if (create->m_cols[index_col].m_default)
                                {
                                    xk_pg_parser_free_node_tree(create->m_cols[index_col].m_default);
                                }
                            }
                            xk_pg_parser_mcxt_free(PARSER_MCXT, create->m_cols);
                        }
                        if (create->m_partitionby)
                        {
                            if (create->m_partitionby->m_column)
                                xk_pg_parser_mcxt_free(PARSER_MCXT, create->m_partitionby->m_column);
                            /* 释放m_partitionby->m_colnode */
                            if (create->m_partitionby->m_colnode)
                                xk_pg_parser_free_node_tree(create->m_partitionby->m_colnode);
                            xk_pg_parser_mcxt_free(PARSER_MCXT, create->m_partitionby);
                        }
                        if (create->m_partitionof)
                        {
                            /* 释放m_partitionof->m_partitionof_node */
                            if (create->m_partitionof->m_partitionof_node)
                                xk_pg_parser_free_node_tree(create->m_partitionof->m_partitionof_node);
                            xk_pg_parser_mcxt_free(PARSER_MCXT, create->m_partitionof);
                        }
                        if (create->m_inherits)
                            xk_pg_parser_mcxt_free(PARSER_MCXT, create->m_inherits);
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case XK_PG_PARSER_DDLINFO_CREATE_INDEX:
                    {
                        xk_pg_parser_translog_ddlstmt_index *temp_index =
                            (xk_pg_parser_translog_ddlstmt_index *) result->m_ddlstmt;
                        if (temp_index->m_colnode)
                            xk_pg_parser_free_node_tree(temp_index->m_colnode);
                        if (temp_index->m_column)
                            xk_pg_parser_mcxt_free(PARSER_MCXT, temp_index->m_column);
                        if (temp_index->m_includecols)
                            xk_pg_parser_mcxt_free(PARSER_MCXT, temp_index->m_includecols);
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case XK_PG_PARSER_DDLINFO_CREATE_NAMESPACE:
                    case XK_PG_PARSER_DDLINFO_CREATE_SEQUENCE:
                    {
                        /* 只需要释放子结构体指针 */
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }

                    case XK_PG_PARSER_DDLINFO_CREATE_TYPE:
                    {
                        xk_pg_parser_translog_ddlstmt_type *type_return =
                            (xk_pg_parser_translog_ddlstmt_type *)result->m_ddlstmt;
                        if (type_return->m_typptr)
                        {
                            /* 不同类型的子结构体中的指针均使用传入record的地址, 无需释放, 仅释放子结构体 */
                            xk_pg_parser_mcxt_free(PARSER_MCXT, type_return->m_typptr);
                        }
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    default:
                    {
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                }
            }
            else if (result->m_ddlstmt && result->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_DROP)
            {
                switch (result->m_base.m_ddlinfo)
                {
                    case XK_PG_PARSER_DDLINFO_DROP_NAMESPACE:
                    case XK_PG_PARSER_DDLINFO_DROP_TABLE:
                    case XK_PG_PARSER_DDLINFO_DROP_INDEX:
                    case XK_PG_PARSER_DDLINFO_DROP_SEQUENCE:
                    case XK_PG_PARSER_DDLINFO_DROP_TYPE:
                    {
                        /* 只需要释放子结构体指针 */
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    default:
                    {
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                }
            }
            else if (result->m_ddlstmt && result->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_ALTER)
            {
                switch (result->m_base.m_ddlinfo)
                {
                    case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME:
                    case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL:
                    case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NULL:
                    case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE:
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_RENAME:
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN:
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT:
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE:
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED:
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED:
                    {
                        /* 只需要释放子结构体指针 */
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT:
                    {
                        xk_pg_parser_translog_ddlstmt_default *alter = (xk_pg_parser_translog_ddlstmt_default *)
                                                        result->m_ddlstmt;
                        if (alter->m_default_node)
                            xk_pg_parser_free_node_tree(alter->m_default_node);
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;

                    }

                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN:
                    {
                        xk_pg_parser_translog_ddlstmt_addcolumn *add_column = 
                        (xk_pg_parser_translog_ddlstmt_addcolumn *)result->m_ddlstmt;
                        if (add_column->m_addcolumn)
                            xk_pg_parser_mcxt_free(PARSER_MCXT, add_column->m_addcolumn);
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT:
                    {
                        xk_pg_parser_translog_ddlstmt_tbconstraint *cons_return =
                            (xk_pg_parser_translog_ddlstmt_tbconstraint *)result->m_ddlstmt;
                        if (cons_return->m_constraint_stmt)
                        {
                            if (cons_return->m_type == XK_PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY)
                            {
                                xk_pg_parser_translog_ddlstmt_tbconstraint_key *pkey =
                                    (xk_pg_parser_translog_ddlstmt_tbconstraint_key *)cons_return->m_constraint_stmt;
                                if (pkey->m_concols)
                                    xk_pg_parser_mcxt_free(PARSER_MCXT, pkey->m_concols);
                            }
                            else if (cons_return->m_type == XK_PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY)
                            {
                                xk_pg_parser_translog_ddlstmt_tbconstraint_fkey *fkey =
                                    (xk_pg_parser_translog_ddlstmt_tbconstraint_fkey *)cons_return->m_constraint_stmt;
                                if (fkey->m_concols_position)
                                    xk_pg_parser_mcxt_free(PARSER_MCXT, fkey->m_concols_position);
                                if (fkey->m_fkeycols_position)
                                    xk_pg_parser_mcxt_free(PARSER_MCXT, fkey->m_fkeycols_position);
                            }
                            else if (cons_return->m_type == XK_PG_PARSER_DDL_CONSTRAINT_UNIQUE)
                            {
                                xk_pg_parser_translog_ddlstmt_tbconstraint_key *ukey =
                                    (xk_pg_parser_translog_ddlstmt_tbconstraint_key *)cons_return->m_constraint_stmt;
                                if (ukey->m_concols)
                                    xk_pg_parser_mcxt_free(PARSER_MCXT, ukey->m_concols);
                            }
                            else if (cons_return->m_type == XK_PG_PARSER_DDL_CONSTRAINT_CHECK)
                            {
                                xk_pg_parser_translog_ddlstmt_tbconstraint_check *check = 
                                    (xk_pg_parser_translog_ddlstmt_tbconstraint_check *)cons_return->m_constraint_stmt;
                                if (check->m_check_node)
                                    xk_pg_parser_free_node_tree(check->m_check_node);
                            }
                            xk_pg_parser_mcxt_free(PARSER_MCXT, cons_return->m_constraint_stmt);
                        }
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    default:
                    {
                        xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                }
            }
            else if (result->m_ddlstmt && result->m_base.m_ddltype == XK_PG_PARSER_DDLTYPE_SPECIAL)
            {
                if (result->m_base.m_ddlinfo == XK_PG_PARSER_DDLINFO_TRUNCATE)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                else if (result->m_base.m_ddlinfo == XK_PG_PARSER_DDLINFO_REINDEX)
                    xk_pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
            }
            result_temp = result;
            result = result->m_next;
            xk_pg_parser_mcxt_free(PARSER_MCXT, result_temp);
        }
    }
}

void xk_pg_parser_trans_TransRecord_free(xk_pg_parser_translog_translog2col *xk_pg_parser_trans_pre,
                                    xk_pg_parser_translog_tbcolbase *xk_pg_parser_trans)
{
    if (xk_pg_parser_trans_pre)
    {
        /* convert结构体里的内容在jni接口中释放 */
        if (xk_pg_parser_trans_pre->m_convert)
        {
            if (xk_pg_parser_trans_pre->m_convert->m_dbcharset)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_dbcharset);
            if (xk_pg_parser_trans_pre->m_convert->m_monetary)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_monetary);
            if (xk_pg_parser_trans_pre->m_convert->m_numeric)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_numeric);
            if (xk_pg_parser_trans_pre->m_convert->m_tartgetcharset)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_tartgetcharset);
            if (xk_pg_parser_trans_pre->m_convert->m_tzname)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_tzname);
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert);
        }
        if (xk_pg_parser_trans_pre->m_sysdicts)
        {
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type);
        }
        if (xk_pg_parser_trans_pre->m_record)
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_record);
        if (xk_pg_parser_trans_pre->m_tuples)
        {
            if (xk_pg_parser_trans_pre->m_tuples->m_tupledata)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_tuples->m_tupledata);
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_tuples);
        }
        xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre);
    }

    if (xk_pg_parser_trans)
    {
        int32_t i = 0;
        xk_pg_parser_translog_tbcol_values *trans = (xk_pg_parser_translog_tbcol_values *)
                                                     xk_pg_parser_trans;
        if (trans->m_valueCnt > 0)
        {
            for (i = 0; i < trans->m_valueCnt; i++)
            {
                if (trans->m_new_values)
                    xk_pg_parser_free_value(&trans->m_new_values[i]);
                if (trans->m_old_values)
                    xk_pg_parser_free_value(&trans->m_old_values[i]);
            }
        }
        if (trans->m_new_values)
            xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_new_values);
        if (trans->m_old_values)
            xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_old_values);
        if (trans->m_tuple)
        {
            for (i = 0; i < (int32_t) trans->m_tupleCnt; i++)
                xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple[i].m_tupledata);
            xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple);
        }
        xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans);
    }
}

void xk_pg_parser_trans_TransRecord_Minsert_free(xk_pg_parser_translog_translog2col *xk_pg_parser_trans_pre,
                                    xk_pg_parser_translog_tbcolbase *xk_pg_parser_trans)
{
    if (xk_pg_parser_trans_pre)
    {
        /* convert结构体里的内容在jni接口中释放 */
        if (xk_pg_parser_trans_pre->m_convert)
        {
            if (xk_pg_parser_trans_pre->m_convert->m_dbcharset)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_dbcharset);
            if (xk_pg_parser_trans_pre->m_convert->m_monetary)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_monetary);
            if (xk_pg_parser_trans_pre->m_convert->m_numeric)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_numeric);
            if (xk_pg_parser_trans_pre->m_convert->m_tartgetcharset)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_tartgetcharset);
            if (xk_pg_parser_trans_pre->m_convert->m_tzname)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert->m_tzname);
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_convert);
        }
        if (xk_pg_parser_trans_pre->m_sysdicts)
        {
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range);
            if (xk_pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type)
                xk_pg_parser_mcxt_free(PARSER_MCXT, 
                    xk_pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type);
        }
        if (xk_pg_parser_trans_pre->m_record)
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_record);
        if (xk_pg_parser_trans_pre->m_tuples)
        {
            if (xk_pg_parser_trans_pre->m_tuples->m_tupledata)
                xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_tuples->m_tupledata);
            xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre->m_tuples);
        }
        xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans_pre);
    }

    if (xk_pg_parser_trans)
    {
        int32_t i = 0;
        int32_t j = 0;
        xk_pg_parser_translog_tbcol_nvalues *trans = (xk_pg_parser_translog_tbcol_nvalues *)
                                                     xk_pg_parser_trans;
        if (trans->m_rowCnt > 0)
        {
            for (i = 0; i < trans->m_rowCnt; i++)
            {
                if (trans->m_rows[i].m_new_values)
                {
                    for (j =0; j < trans->m_valueCnt; j++)
                    {
                        xk_pg_parser_free_value(&trans->m_rows[i].m_new_values[j]);
                    }
                    xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_rows[i].m_new_values);
                }
            }
        }
        if (trans->m_rows)
            xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_rows);
        if (trans->m_tuple)
        {
            for (i = 0; i < (int32_t) trans->m_tupleCnt; i++)
                xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple[i].m_tupledata);
            xk_pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple);
        }
        xk_pg_parser_mcxt_free(PARSER_MCXT, xk_pg_parser_trans);
    }
}
