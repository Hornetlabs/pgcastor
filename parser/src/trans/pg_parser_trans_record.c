#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_xlog/pg_parser_trans_rmgr_xlog.h"
#include "trans/rmgr_xact/pg_parser_trans_rmgr_xact.h"
#include "trans/rmgr_relmap/pg_parser_trans_rmgr_relmap.h"
#include "trans/rmgr_heap_heap2/pg_parser_trans_rmgr_heap_heap2.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"
#include "image/pg_parser_image.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"
#include "trans/rmgr_standby/pg_parser_trans_rmgr_standby.h"
#include "trans/rmgr_seq/pg_parser_trans_rmgr_seq.h"

/* DEFINE */
#define PG_PARSER_RMGRCNTS          7
#define PG_PARSER_GETTUPLE_RMGRCNTS 2

#define PARSER_MCXT                 NULL
/* typedef statement */

/* Define function pointer for pre-processing FMGR */
typedef bool (*pg_parser_trans_transrec_rmgrfunc_pre)(pg_parser_XLogReaderState*    state,
                                                      pg_parser_translog_pre_base** result,
                                                      int32_t* pg_parser_errno);

/* Define function pointer for FMGR processing during secondary parsing */
typedef bool (*pg_parser_trans_transrec_rmgrfunc_trans)(pg_parser_XLogReaderState*     state,
                                                        pg_parser_translog_tbcolbase** result,
                                                        int32_t* pg_parser_errno);

/* static function statement */
static bool pg_parser_trans_transrec_decode_checkPreTransParam(
    pg_parser_translog_pre* pg_parser_pre_data);

static bool deal_invalid_record(pg_parser_XLogReaderState*    state,
                                pg_parser_translog_pre_base** pg_parser_result,
                                int32_t*                      pg_parser_errno);

typedef struct PG_PARSER_TRANS_RMGR
{
    pg_parser_trans_rmgr_enum m_rmgrid; /* rmgrid value */
    pg_parser_trans_transrec_rmgrfunc_pre
        m_rmgrfunc_pre; /* rmgr-level handler for pre-parse interface */
    pg_parser_trans_transrec_rmgrfunc_trans
        m_rmgrfunc_trans; /* Function handler interface for secondary parsing */
} pg_parser_trans_rmgr;

/**
 * @brief        rmgid dispatch
 */
static pg_parser_trans_rmgr m_record_rmgr[] = {
    {PG_PARSER_TRANSLOG_RMGR_XLOG_ID, pg_parser_trans_rmgr_xlog_pre, NULL},
    {PG_PARSER_TRANSLOG_RMGR_XACT_ID, pg_parser_trans_rmgr_xact_pre, NULL},
    {PG_PARSER_TRANSLOG_RMGR_RELMAP_ID, pg_parser_trans_rmgr_relmap_pre, NULL},
    {PG_PARSER_TRANSLOG_RMGR_STANDBY_ID, pg_parser_trans_rmgr_standby_pre, NULL},
    {PG_PARSER_TRANSLOG_RMGR_HEAP2_ID,
     pg_parser_trans_rmgr_heap2_pre,
     pg_parser_trans_rmgr_heap2_trans},
    {PG_PARSER_TRANSLOG_RMGR_HEAP_ID,
     pg_parser_trans_rmgr_heap_pre,
     pg_parser_trans_rmgr_heap_trans},
    {PG_PARSER_TRANSLOG_RMGR_SEQ_ID, pg_parser_trans_rmgr_seq_pre, NULL}};

static pg_parser_trans_rmgr m_record_rmgr_get_tuple[] = {
    {PG_PARSER_TRANSLOG_RMGR_HEAP2_ID, NULL, pg_parser_trans_rmgr_heap2_trans_get_tuple},
    {PG_PARSER_TRANSLOG_RMGR_HEAP_ID, NULL, pg_parser_trans_rmgr_heap_trans_get_tuple}};

/* Pre-parse interface */
// Name with ref in marks input/output parameters
bool pg_parser_trans_preTrans(pg_parser_translog_pre*       pg_parser_pre_data,
                              pg_parser_translog_pre_base** pg_parser_result,
                              int32_t*                      pg_parser_errno)
{
    pg_parser_XLogReaderState* readstate = NULL;
    int32_t                    index = 0;
    int8_t                     rmgrcnts = PG_PARSER_RMGRCNTS;
    pg_parser_XLogRecord*      record = (pg_parser_XLogRecord*)pg_parser_pre_data->m_record;
    bool                       pre_trans_result = false;
    /* Check passed-in parameters in JNI */
    // todo
    if (!pg_parser_trans_transrec_decode_checkPreTransParam(pg_parser_pre_data))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_PRE_CHECK;
        return false;
    }
    /* Prepare recordstate, allocate memory */
    readstate = pg_parser_trans_transrec_decode_XLogReader_Allocate();

    if (NULL == readstate)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_DECODESTATE;
        return false;
    }
    readstate->pg_parser_errno = pg_parser_errno;
    readstate->pre_trans_data = pg_parser_pre_data;

    /* Directly parse record during pre-parsing, data saved in readstate */
    if (!pg_parser_trans_transrec_decodeXlogRecord(readstate,
                                                   record,
                                                   pg_parser_pre_data->m_pagesize,
                                                   pg_parser_errno,
                                                   true,
                                                   pg_parser_pre_data->m_dbtype,
                                                   pg_parser_pre_data->m_dbversion))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_PRE_DECODE;
        return false;
    }

    *pg_parser_errno = ERRNO_SUCCESS;

    /* Call parsing function classified by RMGRID */
    for (index = 0; index < rmgrcnts; index++)
    {
        if (m_record_rmgr[index].m_rmgrid != record->xl_rmid)
        {
            continue;
        }
        pre_trans_result =
            m_record_rmgr[index].m_rmgrfunc_pre(readstate, pg_parser_result, pg_parser_errno);
        break;
    }
    if (false == pre_trans_result)
    {
        /* When error code is not 0, an error occurred during rmgr or info processing */
        if (ERRNO_SUCCESS != *pg_parser_errno)
        {
            return false;
        }
        /* If there is full page write in record that does not need parsing, it needs to be saved */
        if (!pg_parser_check_fpw(
                readstate, pg_parser_result, pg_parser_errno, pg_parser_pre_data->m_dbtype))
        {
            if (ERRNO_SUCCESS != *pg_parser_errno)
            {
                pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
                return false;
            }
            /* When error code is 0, it means no effective rmgr was captured and no full page write
             * was captured, this record does not need parsing */
            deal_invalid_record(readstate, pg_parser_result, pg_parser_errno);
        }
    }
    /* Release readstate */
    if (readstate)
    {
        pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
    }
    return true;
}

static bool pg_parser_trans_transrec_decode_checkPreTransParam(
    pg_parser_translog_pre* pg_parser_pre_data)
{
    if (PG_PARSER_DEBUG_SILENCE > pg_parser_pre_data->m_debugLevel ||
        PG_PARSER_WALLEVEL_REPLICA > pg_parser_pre_data->m_walLevel ||
        PG_PARSER_WALLEVEL_LOGICAL < pg_parser_pre_data->m_walLevel ||
        NULL == pg_parser_pre_data->m_record)
    {
        return false;
    }

    if (0 < (pg_parser_pre_data->m_pagesize % 2))
    {
        return false;
    }

    return true;
}

static bool deal_invalid_record(pg_parser_XLogReaderState*    readstate,
                                pg_parser_translog_pre_base** pg_parser_result,
                                int32_t*                      pg_parser_errno)
{
    if (!pg_parser_mcxt_malloc(
            PARSER_MCXT, (void**)(pg_parser_result), sizeof(pg_parser_translog_pre_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_RECORD_00;
        return false;
    }
    (*pg_parser_result)->m_type = PG_PARSER_TRANSLOG_INVALID;
    (*pg_parser_result)->m_originid = readstate->record_origin;
    (*pg_parser_result)->m_xid = readstate->decoded_record->xl_xid;
    *pg_parser_errno = ERRNO_PG_PARSER_PRE_USELESS;
    return true;
}

/* Secondary parsing interface */
bool pg_parser_trans_TransRecord(pg_parser_translog_translog2col* pg_parser_transData,
                                 pg_parser_translog_tbcolbase**   pg_parser_trans_result,
                                 int32_t*                         pg_parser_errno)
{
    pg_parser_XLogReaderState* readstate = NULL;
    pg_parser_XLogRecord*      record = (pg_parser_XLogRecord*)pg_parser_transData->m_record;
    bool                       result = false;
    int32_t                    index = 0;
    int8_t                     rmgrcnts = PG_PARSER_RMGRCNTS;

    readstate = pg_parser_trans_transrec_decode_XLogReader_Allocate();
    if (NULL == readstate)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_DECODESTATE;
        return false;
    }
    readstate->pg_parser_errno = pg_parser_errno;
    /* Directly parse record during pre-parsing, data saved in readstate */
    if (!pg_parser_trans_transrec_decodeXlogRecord(readstate,
                                                   record,
                                                   pg_parser_transData->m_pagesize,
                                                   pg_parser_errno,
                                                   false,
                                                   pg_parser_transData->m_dbtype,
                                                   pg_parser_transData->m_dbversion))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_PRE_DECODE;
        pg_parser_log_errlog(pg_parser_transData->m_debugLevel,
                             "ERROR: in trans record, decode failed\n");
        return false;
    }
    readstate->trans_data = pg_parser_transData;
    *pg_parser_errno = ERRNO_SUCCESS;
    pg_parser_log_errlog(
        pg_parser_transData->m_debugLevel,
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
        pg_parser_transData->m_walLevel,
        pg_parser_transData->m_debugLevel,
        pg_parser_transData->m_dbtype,
        pg_parser_transData->m_pagesize,
        pg_parser_transData->m_tuplecnt,
        pg_parser_transData->m_dbversion ? pg_parser_transData->m_dbversion : "NULL",
        pg_parser_transData->m_sysdicts->m_pg_class.m_count,
        pg_parser_transData->m_sysdicts->m_pg_attribute.m_count,
        pg_parser_transData->m_sysdicts->m_pg_namespace.m_count,
        pg_parser_transData->m_sysdicts->m_pg_type.m_count,
        pg_parser_transData->m_sysdicts->m_pg_range.m_count,
        pg_parser_transData->m_sysdicts->m_pg_enum.m_count,
        pg_parser_transData->m_sysdicts->m_pg_proc.m_count,
        pg_parser_transData->m_convert->m_dbcharset,
        pg_parser_transData->m_convert->m_tartgetcharset);
    /* Call parsing function classified by RMGRID */

    for (index = 0; index < rmgrcnts; index++)
    {
        if (m_record_rmgr[index].m_rmgrid != record->xl_rmid)
        {
            continue;
        }
        if (m_record_rmgr[index].m_rmgrfunc_trans == NULL)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_RMGR_NOT_FIND;
            pg_parser_log_errlog(pg_parser_transData->m_debugLevel,
                                 "ERROR: RMGR MANAGER,"
                                 "can't find trans func by rmgrid:%u\n",
                                 record->xl_rmid);
            return false;
        }
        result = m_record_rmgr[index].m_rmgrfunc_trans(
            readstate, pg_parser_trans_result, pg_parser_errno);
        break;
    }

    if (false == result)
    {
        /* When error code is not 0, an error occurred during rmgr or info processing */
        if (ERRNO_SUCCESS != *pg_parser_errno)
        {
            pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
            return false;
        }
    }
    /* Release readstate */
    pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
    return true;
}

bool pg_parser_trans_TransRecord_GetTuple(pg_parser_translog_translog2col* pg_parser_transData,
                                          pg_parser_translog_tbcolbase**   pg_parser_trans_result,
                                          int32_t*                         pg_parser_errno)
{
    pg_parser_XLogReaderState* readstate = NULL;
    pg_parser_XLogRecord*      record = (pg_parser_XLogRecord*)pg_parser_transData->m_record;
    bool                       result = false;
    int32_t                    index = 0;
    int8_t                     rmgrcnts = PG_PARSER_GETTUPLE_RMGRCNTS;

    readstate = pg_parser_trans_transrec_decode_XLogReader_Allocate();
    if (NULL == readstate)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_DECODESTATE;
        return false;
    }
    readstate->pg_parser_errno = pg_parser_errno;
    /* Directly parse record during pre-parsing, data saved in readstate */
    if (!pg_parser_trans_transrec_decodeXlogRecord(readstate,
                                                   record,
                                                   pg_parser_transData->m_pagesize,
                                                   pg_parser_errno,
                                                   false,
                                                   pg_parser_transData->m_dbtype,
                                                   pg_parser_transData->m_dbversion))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_PRE_FUNCERR_PRE_DECODE;
        pg_parser_log_errlog(pg_parser_transData->m_debugLevel,
                             "ERROR: in trans record, decode failed\n");
        return false;
    }
    readstate->trans_data = pg_parser_transData;
    *pg_parser_errno = ERRNO_SUCCESS;

    /* Call parsing function classified by RMGRID */

    for (index = 0; index < rmgrcnts; index++)
    {
        if (m_record_rmgr_get_tuple[index].m_rmgrid != record->xl_rmid)
        {
            continue;
        }
        if (m_record_rmgr_get_tuple[index].m_rmgrfunc_trans == NULL)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_RMGR_NOT_FIND;
            pg_parser_log_errlog(pg_parser_transData->m_debugLevel,
                                 "ERROR: RMGR MANAGER,"
                                 "can't find trans func by rmgrid:%u\n",
                                 record->xl_rmid);
            return false;
        }
        result = m_record_rmgr_get_tuple[index].m_rmgrfunc_trans(
            readstate, pg_parser_trans_result, pg_parser_errno);
        break;
    }

    if (false == result)
    {
        /* When error code is not 0, an error occurred during rmgr or info processing */
        if (ERRNO_SUCCESS != *pg_parser_errno)
        {
            pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
            return false;
        }
    }
    /* Release readstate */
    pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(readstate);
    return true;
}

/* DDL parsing interface */
bool pg_parser_trans_DDLtrans(pg_parser_translog_systb2ddl* pg_parser_ddl,
                              pg_parser_translog_ddlstmt**  pg_parser_ddl_result,
                              int32_t*                      pg_parser_errno)
{
    if (!pg_parser_DDL_transRecord2DDL(pg_parser_ddl, pg_parser_ddl_result, pg_parser_errno))
    {
        return false;
    }
    return true;
}

/* External out-of-line storage parsing interface */
bool pg_parser_trans_external_trans(pg_parser_translog_external*     pg_parser_exdata,
                                    pg_parser_translog_tbcol_value** pg_parser_trans_result,
                                    int32_t*                         pg_parser_errno)
{
    pg_parser_translog_tbcol_value*         result = NULL;
    pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    PG_PARSER_UNUSED(pg_parser_errno);
    if (!pg_parser_mcxt_malloc(
            PARSER_MCXT, (void**)&result, sizeof(pg_parser_translog_tbcol_value)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_EXTERNAL_MEMERR_ALLOC_00;
        pg_parser_log_errlog(pg_parser_exdata->m_debuglevel,
                             "ERROR: EXTERNAL PARSER,"
                             "malloc failed\n");
        return false;
    }
    zicinfo.convertinfo = pg_parser_exdata->m_convertInfo;
    result->m_colName = rstrdup(pg_parser_exdata->m_colName);
    result->m_freeFlag = true;
    result->m_coltype = pg_parser_exdata->m_typeid;
    zicinfo.dbtype = pg_parser_exdata->m_dbtype;
    zicinfo.dbversion = pg_parser_exdata->m_dbversion;
    zicinfo.errorno = pg_parser_errno;
    zicinfo.debuglevel = pg_parser_exdata->m_debuglevel;
    if (!pg_parser_convert_attr_to_str_external_value(
            (pg_parser_Datum)pg_parser_exdata->m_chunkdata,
            pg_parser_exdata->m_typout,
            result,
            &zicinfo))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_EXTERNAL_MEMERR_ALLOC_01;
        pg_parser_log_errlog(pg_parser_exdata->m_debuglevel,
                             "ERROR: EXTERNAL PARSER,"
                             "malloc failed\n");
        return false;
    }
    *pg_parser_trans_result = result;
    return true;
}

/*
 * tbcol_value complement missing values
 * Parameter description: v1 needs to contain values that need to be complemented for missing values
 *          v2 Use v2 value to complement v1 missing values
 *
 * Return value description:
 *
 */
bool pg_parser_trans_matchmissing(pg_parser_translog_tbcol_value* v1,
                                  pg_parser_translog_tbcol_value* v2,
                                  uint16_t                        valuecnt)
{
    int                             colindex = 0;
    pg_parser_translog_tbcol_value* value1 = NULL;
    pg_parser_translog_tbcol_value* value2 = NULL;

    if (NULL == v1 || NULL == v2)
    {
        return false;
    }

    /* Complement missing values */
    for (colindex = 0; colindex < valuecnt; colindex++)
    {
        value1 = &v1[colindex];
        value2 = &v2[colindex];
        if (INFO_COL_MAY_NULL == value1->m_info)
        {
            value1->m_freeFlag = value2->m_freeFlag;
            value1->m_info = value2->m_info;
            value1->m_coltype = value2->m_coltype;
            value1->m_valueLen = value2->m_valueLen;

            if (0 != value1->m_valueLen)
            {
                value1->m_value = rmalloc0(value1->m_valueLen + 1);
                if (NULL == value1->m_value)
                {
                    elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
                    return false;
                }
                rmemset0(value1->m_value, 0, 0, value1->m_valueLen + 1);
                rmemcpy0(value1->m_value, 0, value2->m_value, value1->m_valueLen);
            }
            else
            {
                value1->m_value = NULL;
            }
        }
    }

    return true;
}

void pg_parser_trans_preTrans_free(pg_parser_translog_pre_base* pg_parser_result)
{
    if (pg_parser_result)
    {
        if (PG_PARSER_TRANSLOG_XACT_COMMIT == (pg_parser_result)->m_type ||
            PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE == (pg_parser_result)->m_type)
        {
            pg_parser_translog_pre_trans* trans = (pg_parser_translog_pre_trans*)pg_parser_result;
            pg_parser_xl_xact_parsed_commit* commit =
                (pg_parser_xl_xact_parsed_commit*)trans->m_transdata;
            if (commit)
            {
                if (commit->subxacts)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, commit->subxacts);
                }
                if (commit->xnodes)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, commit->xnodes);
                }
                if (commit->msgs)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, commit->msgs);
                }
                pg_parser_mcxt_free(PARSER_MCXT, commit);
            }

            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_result);
        }
        else if (PG_PARSER_TRANSLOG_XACT_ABORT == (pg_parser_result)->m_type ||
                 PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE == (pg_parser_result)->m_type)
        {
            pg_parser_translog_pre_trans*   trans = (pg_parser_translog_pre_trans*)pg_parser_result;
            pg_parser_xl_xact_parsed_abort* abort =
                (pg_parser_xl_xact_parsed_abort*)trans->m_transdata;

            if (abort)
            {
                if (abort->subxacts)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, abort->subxacts);
                }
                if (abort->xnodes)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, abort->xnodes);
                }
                pg_parser_mcxt_free(PARSER_MCXT, abort);
            }

            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_result);
        }
        else if (PG_PARSER_TRANSLOG_FPW_TUPLE == (pg_parser_result)->m_type)
        {
            int32_t                             i = 0;
            pg_parser_translog_pre_image_tuple* image =
                (pg_parser_translog_pre_image_tuple*)pg_parser_result;
            for (i = 0; i < (int32_t)image->m_tuplecnt; i++)
            {
                pg_parser_mcxt_free(PARSER_MCXT, image->m_tuples[i].m_tupledata);
            }
            pg_parser_mcxt_free(PARSER_MCXT, image->m_tuples);
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_result);
        }
        else if (PG_PARSER_TRANSLOG_RELMAP == (pg_parser_result)->m_type)
        {
            pg_parser_translog_pre_relmap* relmap =
                (pg_parser_translog_pre_relmap*)pg_parser_result;
            if (relmap->m_count > 0 && relmap->m_mapping)
            {
                pg_parser_mcxt_free(PARSER_MCXT, relmap->m_mapping);
            }
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_result);
        }
        else if (PG_PARSER_TRANSLOG_RUNNING_XACTS == (pg_parser_result)->m_type)
        {
            pg_parser_translog_pre_running_xact* rxact =
                (pg_parser_translog_pre_running_xact*)pg_parser_result;
            if (rxact)
            {
                if (rxact->m_standby)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, rxact->m_standby);
                }
                pg_parser_mcxt_free(PARSER_MCXT, rxact);
            }
        }
        else if (PG_PARSER_TRANSLOG_HEAP_TRUNCATE == (pg_parser_result)->m_type)
        {
            pg_parser_translog_pre_heap_truncate* truncate =
                (pg_parser_translog_pre_heap_truncate*)pg_parser_result;
            if (truncate)
            {
                if (truncate->relids)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, truncate->relids);
                }
                pg_parser_mcxt_free(PARSER_MCXT, truncate);
            }
        }
        else
        {
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_result);
        }
    }
}
static void pg_parser_free_node_tree(pg_parser_nodetree* node)
{
    pg_parser_nodetree* next_node = NULL;
    pg_parser_nodetree* node_tree = node;
    while (node_tree)
    {
        next_node = node_tree->m_next;
        switch (node_tree->m_node_type)
        {
            case PG_PARSER_NODETYPE_CONST:
            {
                pg_parser_node_const* node_const = (pg_parser_node_const*)node_tree->m_node;
                if (node_const->m_char_value)
                {
                    pg_parser_mcxt_free(NULL, node_const->m_char_value);
                }
                pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
            }
            case PG_PARSER_NODETYPE_FUNC:
            {
                pg_parser_node_func* node_func = (pg_parser_node_func*)node_tree->m_node;
                if (node_func->m_funcname)
                {
                    pg_parser_mcxt_free(NULL, node_func->m_funcname);
                }
                pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
            }
            case PG_PARSER_NODETYPE_OP:
            {
                pg_parser_node_op* node_op = (pg_parser_node_op*)node_tree->m_node;
                if (node_op->m_opname)
                {
                    pg_parser_mcxt_free(NULL, node_op->m_opname);
                }
                pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
            }
            default:
                pg_parser_mcxt_free(NULL, node_tree->m_node);
                break;
        }
        pg_parser_mcxt_free(NULL, node_tree);
        node_tree = next_node;
    }
}

static void pg_parser_free_value(pg_parser_translog_tbcol_value* value)
{
    if (value)
    {
        if (value->m_value)
        {
            if (value->m_info == INFO_COL_IS_CUSTOM || value->m_info == INFO_COL_IS_ARRAY)
            {
                pg_parser_translog_tbcol_valuetype_customer* custom =
                    (pg_parser_translog_tbcol_valuetype_customer*)value->m_value;
                while (custom)
                {
                    pg_parser_free_value(custom->m_value);
                    pg_parser_mcxt_free(PARSER_MCXT, custom->m_value);
                    custom = custom->m_next;
                }
                pg_parser_mcxt_free(PARSER_MCXT, value->m_value);
            }
            else if (value->m_info == INFO_COL_IS_NODE)
            {
                pg_parser_free_node_tree((pg_parser_nodetree*)value->m_value);
            }
            else
            {
                pg_parser_mcxt_free(PARSER_MCXT, value->m_value);
            }
        }
        if (value->m_freeFlag)
        {
            pg_parser_mcxt_free(PARSER_MCXT, value->m_colName);
        }
    }
}

void pg_parser_free_value_ext(pg_parser_translog_tbcol_value* value)
{
    pg_parser_free_value(value);
}

void pg_parser_trans_external_free(pg_parser_translog_external*    ext,
                                   pg_parser_translog_tbcol_value* result)
{
    if (ext)
    {
        if (ext->m_chunkdata)
        {
            pg_parser_mcxt_free(PARSER_MCXT, ext->m_chunkdata);
        }
        pg_parser_mcxt_free(PARSER_MCXT, ext);
    }
    if (result)
    {
        if (result->m_value)
        {
            pg_parser_free_value(result);
        }
        pg_parser_mcxt_free(PARSER_MCXT, result);
    }
}

void pg_parser_trans_ddl_free(pg_parser_translog_systb2ddl* ddl, pg_parser_translog_ddlstmt* result)
{
    if (ddl)
    {
        pg_parser_translog_systb2dll_record* ddl_record = ddl->m_record;

        while (ddl_record)
        {
            pg_parser_translog_systb2dll_record* ddl_record_temp = ddl_record;
            ddl_record = ddl_record->m_next;
            pg_parser_mcxt_free(PARSER_MCXT, ddl_record_temp);
        }

        if (ddl->m_convert)
        {
            pg_parser_mcxt_free(PARSER_MCXT, ddl->m_convert);
        }

        pg_parser_mcxt_free(PARSER_MCXT, ddl);
    }
    if (result)
    {
        pg_parser_translog_ddlstmt* result_temp = NULL;
        while (result)
        {
            if (result->m_ddlstmt && result->m_base.m_ddltype == PG_PARSER_DDLTYPE_CREATE)
            {
                switch (result->m_base.m_ddlinfo)
                {
                    case PG_PARSER_DDLINFO_CREATE_TABLE:
                    {
                        pg_parser_translog_ddlstmt_createtable* create =
                            (pg_parser_translog_ddlstmt_createtable*)result->m_ddlstmt;
                        /* Pointers in m_cols are reused from record, no need to release, so only
                         * release m_cols */
                        if (create->m_cols)
                        {
                            int index_col = 0;
                            for (index_col = 0; index_col < create->m_colcnt; index_col++)
                            {
                                if (create->m_cols[index_col].m_default)
                                {
                                    pg_parser_free_node_tree(create->m_cols[index_col].m_default);
                                }
                            }
                            pg_parser_mcxt_free(PARSER_MCXT, create->m_cols);
                        }
                        if (create->m_partitionby)
                        {
                            if (create->m_partitionby->m_column)
                            {
                                pg_parser_mcxt_free(PARSER_MCXT, create->m_partitionby->m_column);
                            }
                            /* Release m_partitionby->m_colnode */
                            if (create->m_partitionby->m_colnode)
                            {
                                pg_parser_free_node_tree(create->m_partitionby->m_colnode);
                            }
                            pg_parser_mcxt_free(PARSER_MCXT, create->m_partitionby);
                        }
                        if (create->m_partitionof)
                        {
                            /* Release m_partitionof->m_partitionof_node */
                            if (create->m_partitionof->m_partitionof_node)
                            {
                                pg_parser_free_node_tree(create->m_partitionof->m_partitionof_node);
                            }
                            pg_parser_mcxt_free(PARSER_MCXT, create->m_partitionof);
                        }
                        if (create->m_inherits)
                        {
                            pg_parser_mcxt_free(PARSER_MCXT, create->m_inherits);
                        }
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case PG_PARSER_DDLINFO_CREATE_INDEX:
                    {
                        pg_parser_translog_ddlstmt_index* temp_index =
                            (pg_parser_translog_ddlstmt_index*)result->m_ddlstmt;
                        if (temp_index->m_colnode)
                        {
                            pg_parser_free_node_tree(temp_index->m_colnode);
                        }
                        if (temp_index->m_column)
                        {
                            pg_parser_mcxt_free(PARSER_MCXT, temp_index->m_column);
                        }
                        if (temp_index->m_includecols)
                        {
                            pg_parser_mcxt_free(PARSER_MCXT, temp_index->m_includecols);
                        }
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case PG_PARSER_DDLINFO_CREATE_NAMESPACE:
                    case PG_PARSER_DDLINFO_CREATE_SEQUENCE:
                    {
                        /* Only need to release sub-structure pointers */
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }

                    case PG_PARSER_DDLINFO_CREATE_TYPE:
                    {
                        pg_parser_translog_ddlstmt_type* type_return =
                            (pg_parser_translog_ddlstmt_type*)result->m_ddlstmt;
                        if (type_return->m_typptr)
                        {
                            /* Pointers in different types of sub-structures use addresses from
                             * passed-in record, no need to release, Only release sub-structure */
                            pg_parser_mcxt_free(PARSER_MCXT, type_return->m_typptr);
                        }
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    default:
                    {
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                }
            }
            else if (result->m_ddlstmt && result->m_base.m_ddltype == PG_PARSER_DDLTYPE_DROP)
            {
                switch (result->m_base.m_ddlinfo)
                {
                    case PG_PARSER_DDLINFO_DROP_NAMESPACE:
                    case PG_PARSER_DDLINFO_DROP_TABLE:
                    case PG_PARSER_DDLINFO_DROP_INDEX:
                    case PG_PARSER_DDLINFO_DROP_SEQUENCE:
                    case PG_PARSER_DDLINFO_DROP_TYPE:
                    {
                        /* Only need to release sub-structure pointers */
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    default:
                    {
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                }
            }
            else if (result->m_ddlstmt && result->m_base.m_ddltype == PG_PARSER_DDLTYPE_ALTER)
            {
                switch (result->m_base.m_ddlinfo)
                {
                    case PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME:
                    case PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL:
                    case PG_PARSER_DDLINFO_ALTER_COLUMN_NULL:
                    case PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE:
                    case PG_PARSER_DDLINFO_ALTER_TABLE_RENAME:
                    case PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN:
                    case PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT:
                    case PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE:
                    case PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED:
                    case PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED:
                    {
                        /* Only need to release sub-structure pointers */
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT:
                    {
                        pg_parser_translog_ddlstmt_default* alter =
                            (pg_parser_translog_ddlstmt_default*)result->m_ddlstmt;
                        if (alter->m_default_node)
                        {
                            pg_parser_free_node_tree(alter->m_default_node);
                        }
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }

                    case PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN:
                    {
                        pg_parser_translog_ddlstmt_addcolumn* add_column =
                            (pg_parser_translog_ddlstmt_addcolumn*)result->m_ddlstmt;
                        if (add_column->m_addcolumn)
                        {
                            pg_parser_mcxt_free(PARSER_MCXT, add_column->m_addcolumn);
                        }
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    case PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT:
                    {
                        pg_parser_translog_ddlstmt_tbconstraint* cons_return =
                            (pg_parser_translog_ddlstmt_tbconstraint*)result->m_ddlstmt;
                        if (cons_return->m_constraint_stmt)
                        {
                            if (cons_return->m_type == PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY)
                            {
                                pg_parser_translog_ddlstmt_tbconstraint_key* pkey =
                                    (pg_parser_translog_ddlstmt_tbconstraint_key*)
                                        cons_return->m_constraint_stmt;
                                if (pkey->m_concols)
                                {
                                    pg_parser_mcxt_free(PARSER_MCXT, pkey->m_concols);
                                }
                            }
                            else if (cons_return->m_type == PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY)
                            {
                                pg_parser_translog_ddlstmt_tbconstraint_fkey* fkey =
                                    (pg_parser_translog_ddlstmt_tbconstraint_fkey*)
                                        cons_return->m_constraint_stmt;
                                if (fkey->m_concols_position)
                                {
                                    pg_parser_mcxt_free(PARSER_MCXT, fkey->m_concols_position);
                                }
                                if (fkey->m_fkeycols_position)
                                {
                                    pg_parser_mcxt_free(PARSER_MCXT, fkey->m_fkeycols_position);
                                }
                            }
                            else if (cons_return->m_type == PG_PARSER_DDL_CONSTRAINT_UNIQUE)
                            {
                                pg_parser_translog_ddlstmt_tbconstraint_key* ukey =
                                    (pg_parser_translog_ddlstmt_tbconstraint_key*)
                                        cons_return->m_constraint_stmt;
                                if (ukey->m_concols)
                                {
                                    pg_parser_mcxt_free(PARSER_MCXT, ukey->m_concols);
                                }
                            }
                            else if (cons_return->m_type == PG_PARSER_DDL_CONSTRAINT_CHECK)
                            {
                                pg_parser_translog_ddlstmt_tbconstraint_check* check =
                                    (pg_parser_translog_ddlstmt_tbconstraint_check*)
                                        cons_return->m_constraint_stmt;
                                if (check->m_check_node)
                                {
                                    pg_parser_free_node_tree(check->m_check_node);
                                }
                            }
                            pg_parser_mcxt_free(PARSER_MCXT, cons_return->m_constraint_stmt);
                        }
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                    default:
                    {
                        pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                        break;
                    }
                }
            }
            else if (result->m_ddlstmt && result->m_base.m_ddltype == PG_PARSER_DDLTYPE_SPECIAL)
            {
                if (result->m_base.m_ddlinfo == PG_PARSER_DDLINFO_TRUNCATE)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                }
                else if (result->m_base.m_ddlinfo == PG_PARSER_DDLINFO_REINDEX)
                {
                    pg_parser_mcxt_free(PARSER_MCXT, result->m_ddlstmt);
                }
            }
            result_temp = result;
            result = result->m_next;
            pg_parser_mcxt_free(PARSER_MCXT, result_temp);
        }
    }
}

void pg_parser_trans_TransRecord_free(pg_parser_translog_translog2col* pg_parser_trans_pre,
                                      pg_parser_translog_tbcolbase*    pg_parser_trans)
{
    if (pg_parser_trans_pre)
    {
        /* Contents in convert structure are released in jni interface */
        if (pg_parser_trans_pre->m_convert)
        {
            if (pg_parser_trans_pre->m_convert->m_dbcharset)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_dbcharset);
            }
            if (pg_parser_trans_pre->m_convert->m_monetary)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_monetary);
            }
            if (pg_parser_trans_pre->m_convert->m_numeric)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_numeric);
            }
            if (pg_parser_trans_pre->m_convert->m_tartgetcharset)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_tartgetcharset);
            }
            if (pg_parser_trans_pre->m_convert->m_tzname)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_tzname);
            }
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert);
        }
        if (pg_parser_trans_pre->m_sysdicts)
        {
            if (pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes)
            {
                pg_parser_mcxt_free(
                    PARSER_MCXT, pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type);
            }
        }
        if (pg_parser_trans_pre->m_record)
        {
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_record);
        }
        if (pg_parser_trans_pre->m_tuples)
        {
            if (pg_parser_trans_pre->m_tuples->m_tupledata)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_tuples->m_tupledata);
            }
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_tuples);
        }
        pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre);
    }

    if (pg_parser_trans)
    {
        int32_t                          i = 0;
        pg_parser_translog_tbcol_values* trans = (pg_parser_translog_tbcol_values*)pg_parser_trans;
        if (trans->m_valueCnt > 0)
        {
            for (i = 0; i < trans->m_valueCnt; i++)
            {
                if (trans->m_new_values)
                {
                    pg_parser_free_value(&trans->m_new_values[i]);
                }
                if (trans->m_old_values)
                {
                    pg_parser_free_value(&trans->m_old_values[i]);
                }
            }
        }
        if (trans->m_new_values)
        {
            pg_parser_mcxt_free(PARSER_MCXT, trans->m_new_values);
        }
        if (trans->m_old_values)
        {
            pg_parser_mcxt_free(PARSER_MCXT, trans->m_old_values);
        }
        if (trans->m_tuple)
        {
            for (i = 0; i < (int32_t)trans->m_tupleCnt; i++)
            {
                pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple[i].m_tupledata);
            }
            pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple);
        }
        pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans);
    }
}

void pg_parser_trans_TransRecord_Minsert_free(pg_parser_translog_translog2col* pg_parser_trans_pre,
                                              pg_parser_translog_tbcolbase*    pg_parser_trans)
{
    if (pg_parser_trans_pre)
    {
        /* Contents in convert structure are released in jni interface */
        if (pg_parser_trans_pre->m_convert)
        {
            if (pg_parser_trans_pre->m_convert->m_dbcharset)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_dbcharset);
            }
            if (pg_parser_trans_pre->m_convert->m_monetary)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_monetary);
            }
            if (pg_parser_trans_pre->m_convert->m_numeric)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_numeric);
            }
            if (pg_parser_trans_pre->m_convert->m_tartgetcharset)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_tartgetcharset);
            }
            if (pg_parser_trans_pre->m_convert->m_tzname)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert->m_tzname);
            }
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_convert);
        }
        if (pg_parser_trans_pre->m_sysdicts)
        {
            if (pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes)
            {
                pg_parser_mcxt_free(
                    PARSER_MCXT, pg_parser_trans_pre->m_sysdicts->m_pg_attribute.m_pg_attributes);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_class.m_pg_class);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_enum.m_pg_enum);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_namespace.m_pg_namespace);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_proc.m_pg_proc);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_range.m_pg_range);
            }
            if (pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type)
            {
                pg_parser_mcxt_free(PARSER_MCXT,
                                    pg_parser_trans_pre->m_sysdicts->m_pg_type.m_pg_type);
            }
        }
        if (pg_parser_trans_pre->m_record)
        {
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_record);
        }
        if (pg_parser_trans_pre->m_tuples)
        {
            if (pg_parser_trans_pre->m_tuples->m_tupledata)
            {
                pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_tuples->m_tupledata);
            }
            pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre->m_tuples);
        }
        pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans_pre);
    }

    if (pg_parser_trans)
    {
        int32_t                           i = 0;
        int32_t                           j = 0;
        pg_parser_translog_tbcol_nvalues* trans =
            (pg_parser_translog_tbcol_nvalues*)pg_parser_trans;
        if (trans->m_rowCnt > 0)
        {
            for (i = 0; i < trans->m_rowCnt; i++)
            {
                if (trans->m_rows[i].m_new_values)
                {
                    for (j = 0; j < trans->m_valueCnt; j++)
                    {
                        pg_parser_free_value(&trans->m_rows[i].m_new_values[j]);
                    }
                    pg_parser_mcxt_free(PARSER_MCXT, trans->m_rows[i].m_new_values);
                }
            }
        }
        if (trans->m_rows)
        {
            pg_parser_mcxt_free(PARSER_MCXT, trans->m_rows);
        }
        if (trans->m_tuple)
        {
            for (i = 0; i < (int32_t)trans->m_tupleCnt; i++)
            {
                pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple[i].m_tupledata);
            }
            pg_parser_mcxt_free(PARSER_MCXT, trans->m_tuple);
        }
        pg_parser_mcxt_free(PARSER_MCXT, pg_parser_trans);
    }
}
