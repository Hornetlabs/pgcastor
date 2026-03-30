#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/rmgr_heap_heap2/pg_parser_trans_rmgr_heap_heap2.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"
#include "image/pg_parser_image.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define PG_PARSER_RMGR_HEAP_INFOCNT            4
#define PG_PARSER_RMGR_HEAP2_INFOCNT           1

#define PG_PARSER_RMGR_HEAP_GET_TUPLE_INFOCNT  4
#define PG_PARSER_RMGR_HEAP2_GET_TUPLE_INFOCNT 1

#define TRANS_RMGR_HEAP_MCXT                   NULL

typedef bool (*pg_parser_trans_transrec_rmgr_info_func_trans)(pg_parser_XLogReaderState*     state,
                                                              pg_parser_translog_tbcolbase** result,
                                                              int32_t*                       pg_parser_errno);

typedef struct PG_PARSER_TRANS_RMGR_HEAP
{
    pg_parser_trans_rmgr_heap_info                m_infoid;         /* info value */
    pg_parser_trans_transrec_rmgr_info_func_trans m_infofunc_trans; /* info-level handler for secondary parsing */
} pg_parser_trans_rmgr_heap;

typedef struct PG_PARSER_TRANS_RMGR_HEAP2
{
    pg_parser_trans_rmgr_heap2_info               m_infoid;         /* info value */
    pg_parser_trans_transrec_rmgr_info_func_trans m_infofunc_trans; /* info-level handler for secondary parsing */
} pg_parser_trans_rmgr_heap2;

typedef struct PG_PARSER_TRANS_RMGR_HEAP_GET_TUPLE
{
    pg_parser_trans_rmgr_heap_info                m_infoid;         /* info value */
    pg_parser_trans_transrec_rmgr_info_func_trans m_infofunc_trans; /* info-level handler for secondary parsing */
} pg_parser_trans_rmgr_heap_get_tuple;

typedef struct PG_PARSER_TRANS_RMGR_HEAP2_GET_TUPLE
{
    pg_parser_trans_rmgr_heap2_info               m_infoid;         /* info value */
    pg_parser_trans_transrec_rmgr_info_func_trans m_infofunc_trans; /* info-level handler for secondary parsing */
} pg_parser_trans_rmgr_heap2_get_tuple;

/* static statement */
static bool pg_parser_trans_rmgr_heap_insert_trans(pg_parser_XLogReaderState*     state,
                                                   pg_parser_translog_tbcolbase** result,
                                                   int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap2_minsert_trans(pg_parser_XLogReaderState*     state,
                                                     pg_parser_translog_tbcolbase** result,
                                                     int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_delete_trans(pg_parser_XLogReaderState*     state,
                                                   pg_parser_translog_tbcolbase** result,
                                                   int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_update_trans(pg_parser_XLogReaderState*     state,
                                                   pg_parser_translog_tbcolbase** result,
                                                   int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_insert_get_tuple(pg_parser_XLogReaderState*     state,
                                                       pg_parser_translog_tbcolbase** result,
                                                       int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_delete_get_tuple(pg_parser_XLogReaderState*     state,
                                                       pg_parser_translog_tbcolbase** result,
                                                       int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap2_minsert_get_tuple(pg_parser_XLogReaderState*     state,
                                                         pg_parser_translog_tbcolbase** result,
                                                         int32_t*                       pg_parser_errno);

static bool pg_parser_trans_rmgr_heap_update_get_tuple(pg_parser_XLogReaderState*     state,
                                                       pg_parser_translog_tbcolbase** result,
                                                       int32_t*                       pg_parser_errno);

static bool reassemble_tuplenew_from_wal_data(char*                             data,
                                              size_t                            len,
                                              pg_parser_ReorderBufferTupleBuf** result_new,
                                              pg_parser_xl_heap_update*         xlrec,
                                              uint32_t                          blknum_new,
                                              pg_parser_TransactionId           xid,
                                              pg_parser_HeapTupleHeader         htup_old,
                                              int32_t                           old_tuple_len,
                                              int16_t                           dbtype,
                                              char*                             dbversion);

static void reassemble_mutituple_from_wal_data(char*                            data,
                                               size_t                           len,
                                               pg_parser_ReorderBufferTupleBuf* tuple,
                                               pg_parser_xl_multi_insert_tuple* xlhdr,
                                               int16_t                          dbtype,
                                               char*                            dbversion);

static pg_parser_trans_rmgr_heap2 m_record_rmgr_heap2_info[] = {
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT, pg_parser_trans_rmgr_heap2_minsert_trans}
};

static pg_parser_trans_rmgr_heap m_record_rmgr_heap_info[] = {
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT,     pg_parser_trans_rmgr_heap_insert_trans},
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE,     pg_parser_trans_rmgr_heap_delete_trans},
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE,     pg_parser_trans_rmgr_heap_update_trans},
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE, pg_parser_trans_rmgr_heap_update_trans}
};

static pg_parser_trans_rmgr_heap2_get_tuple m_record_rmgr_heap2_get_tuple_info[] = {
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT, pg_parser_trans_rmgr_heap2_minsert_get_tuple}
};

static pg_parser_trans_rmgr_heap_get_tuple m_record_rmgr_heap_get_tuple_info[] = {
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT,     pg_parser_trans_rmgr_heap_insert_get_tuple},
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE,     pg_parser_trans_rmgr_heap_delete_get_tuple},
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE,     pg_parser_trans_rmgr_heap_update_get_tuple},
    {PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE, pg_parser_trans_rmgr_heap_update_get_tuple}
};

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
            return PG_PARSER_TRANSLOG_TABLETYPE_DICT;
        default:
            return PG_PARSER_TRANSLOG_TABLETYPE_SYS;
    }
}

bool pg_parser_trans_rmgr_heap_trans(pg_parser_XLogReaderState*     state,
                                     pg_parser_translog_tbcolbase** result,
                                     int32_t*                       pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t  infocnts = PG_PARSER_RMGR_HEAP_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap_info[index].m_infofunc_trans(state, result, pg_parser_errno);
        break;
    }
    /* Return false when not found */
    return false;
}

bool pg_parser_trans_rmgr_heap2_trans(pg_parser_XLogReaderState*     state,
                                      pg_parser_translog_tbcolbase** result,
                                      int32_t*                       pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t  infocnts = PG_PARSER_RMGR_HEAP2_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap2_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap2_info[index].m_infofunc_trans(state, result, pg_parser_errno);
        break;
    }
    /* Return false when not found */
    return false;
}

bool pg_parser_trans_rmgr_heap_trans_get_tuple(pg_parser_XLogReaderState*     state,
                                               pg_parser_translog_tbcolbase** result,
                                               int32_t*                       pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t  infocnts = PG_PARSER_RMGR_HEAP_GET_TUPLE_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap_get_tuple_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap_get_tuple_info[index].m_infofunc_trans(state, result, pg_parser_errno);
        break;
    }
    /* Return false when not found */
    return false;
}

bool pg_parser_trans_rmgr_heap2_trans_get_tuple(pg_parser_XLogReaderState*     state,
                                                pg_parser_translog_tbcolbase** result,
                                                int32_t*                       pg_parser_errno)
{
    uint8_t info = state->decoded_record->xl_info;
    int8_t  infocnts = PG_PARSER_RMGR_HEAP2_GET_TUPLE_INFOCNT;
    int32_t index = 0;
    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;
    for (index = 0; index < infocnts; index++)
    {
        if (m_record_rmgr_heap2_get_tuple_info[index].m_infoid != info)
        {
            continue;
        }
        return m_record_rmgr_heap2_get_tuple_info[index].m_infofunc_trans(state, result, pg_parser_errno);
        break;
    }
    /* Return false when not found */
    return false;
}

/* insert */
static bool pg_parser_trans_rmgr_heap_insert_trans(pg_parser_XLogReaderState*     state,
                                                   pg_parser_translog_tbcolbase** result,
                                                   int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_values*        insert_record = NULL;
    pg_parser_xl_heap_insert*               xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf*        tuple = NULL;
    pg_parser_sysdict_tableInfo             tbinfo = {'\0'};
    pg_parser_TupleDesc                     tupdesc = NULL;
    pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t                                 natt = 0;
    uint32_t                                relfilenode = 0;
    size_t                                  datalen = 0;
    size_t                                  tuplelen = 0;
    char*                                   page = NULL;
    char*                                   tupledata = NULL;
    int16_t                                 dbtype = state->trans_data->m_dbtype;
    char*                                   dbversion = state->trans_data->m_dbversion;
    bool                                    have_oid = false;
    uint32_t                                tuple_oid = pg_parser_InvalidOid;

    /* Check validity of input/output parameters */
    if (!state || !result || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap insert], invalid param\n");
        return false;
    }

    xlrec = (pg_parser_xl_heap_insert*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)(&insert_record), sizeof(pg_parser_translog_tbcol_values)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_00;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap insert], MALLOC failed\n");
        return false;
    }

    insert_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
    insert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if (!pg_parser_sysdict_getTableInfo(relfilenode, state->trans_data->m_sysdicts, &tbinfo))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_TBINFO;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap insert], get table info failed\n");
        return false;
    }
    insert_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    insert_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    insert_record->m_relid = tbinfo.oid;
    insert_record->m_valueCnt = (uint16_t)tbinfo.natts;
    natt = tbinfo.natts;
    if (state->trans_data->m_iscatalog)
    {
        insert_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    }
    else
    {
        insert_record->m_base.m_tabletype = PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    }
    /* Replica mode and system table */
    if (true)
    {
        void* temp_tuple = NULL;
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_01;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make full image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            insert_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(state->trans_data->m_dbtype,
                                                                                      state->trans_data->m_dbversion,
                                                                                      state->trans_data->m_pagesize,
                                                                                      page,
                                                                                      &insert_record->m_tupleCnt,
                                                                                      state->blocks[0].blkno,
                                                                                      state->trans_data->m_debugLevel);
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap insert], get %u tuples from image\n",
                                 insert_record->m_tupleCnt);
            if (!insert_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make return tuple failed\n");
                return false;
            }
            /* Set return type */
            insert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            insert_record->m_relfilenode = relfilenode;

            temp_tuple = (void*)pg_parser_assemble_tuple(dbtype,
                                                         state->trans_data->m_dbversion,
                                                         state->trans_data->m_pagesize,
                                                         page,
                                                         xlrec->offnum);
            if (!temp_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make tuple failed\n");
                return false;
            }
            tuple = (pg_parser_ReorderBufferTupleBuf*)temp_tuple;
            /* Full page write data extracted, tuple assembled, release page */
            if (page)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            uint32_t tuplen = 0;
            tupledata = pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE2;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], get tuple from block failed\n");
                return false;
            }
            tuplelen = datalen - pg_parser_SizeOfHeapHeader;
            temp_tuple = (void*)pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
            if (!temp_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE2;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make tuple from block failed\n");
                return false;
            }
            pg_parser_reassemble_tuple_from_wal_data(tupledata,
                                                     datalen,
                                                     (pg_parser_ReorderBufferTupleBuf*)temp_tuple,
                                                     state->decoded_record->xl_xid,
                                                     pg_parser_InvalidTransactionId,
                                                     dbtype,
                                                     dbversion);

            tuple = (pg_parser_ReorderBufferTupleBuf*)temp_tuple;
            tuplen = tuple->tuple.t_len;

            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(insert_record->m_tuple),
                                       sizeof(pg_parser_translog_tuplecache)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_02;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            insert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* Process tuple data for return */
            insert_record->m_tuple->m_pageno = state->blocks[0].blkno;
            insert_record->m_tupleCnt = 1;
            insert_record->m_relfilenode = relfilenode;
            insert_record->m_tuple->m_tuplelen = tuplen;
            insert_record->m_tuple->m_itemoffnum = xlrec->offnum;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap insert], return tuple "
                                 "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                 insert_record->m_tuple->m_itemoffnum,
                                 insert_record->m_tuple->m_pageno,
                                 insert_record->m_tuple->m_tuplelen);
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&(insert_record->m_tuple->m_tupledata), tuplen))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_03;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }

            rmemcpy0(insert_record->m_tuple->m_tupledata, 0, tuple->tuple.t_data, tuple->tuple.t_len);
        }
    }
    tupdesc = pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_MAKE_DESC;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap insert],"
                             "make DESC failed \n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&insert_record->m_new_values,
                               insert_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_04;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap insert],"
                             "MALLOC col value failed \n");
        return false;
    }
    pg_parser_log_errlog(state->trans_data->m_debugLevel, "DEBUG: tupdesc->natts:%d\n", tupdesc->natts);
    zicinfo.convertinfo = state->trans_data->m_convert;

    /* Extract column values from tuple and convert to readable information */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        pg_parser_Datum                 origval = (pg_parser_Datum)0;
        pg_parser_translog_tbcol_value* colvalue = &insert_record->m_new_values[natt];
        bool                            isnull = true;
        bool                            ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }

        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "DEBUG: column name: %s\n",
                             colvalue->m_colName ? colvalue->m_colName : "NULL");

        origval = pg_parser_heap_getattr(&tuple->tuple, natt + 1, tupdesc, &isnull, &ismissing, dbtype, dbversion);

        colvalue->m_coltype = tupdesc->attrs[natt].atttypid;

        if (!strcmp(insert_record->m_base.m_schemaname, PG_TOAST_NAME))
        {
            zicinfo.istoast = true;
        }
        else
        {
            zicinfo.istoast = false;
        }

        zicinfo.dbtype = dbtype;
        zicinfo.dbversion = dbversion;
        zicinfo.errorno = pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (isnull)
        {
            pg_parser_log_errlog(state->trans_data->m_debugLevel, "DEBUG: IS NULL\n");
            colvalue->m_info = INFO_COL_IS_NULL;
            continue;
        }
        if (ismissing)
        {
            pg_parser_log_errlog(state->trans_data->m_debugLevel, "DEBUG: IS MISSING\n");
            colvalue->m_info = INFO_COL_MAY_NULL;
            continue;
        }
        else if (!pg_parser_convert_attr_to_str_value(origval, state->trans_data->m_sysdicts, colvalue, &zicinfo))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_CONVERT_ATTR_TO_CHAR;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap insert],"
                                 "convert attr to char failed \n");
            return false;
        }
    }
    if (have_oid)
    {
        char* result = NULL;
        if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&result, 12))
        {
            return (pg_parser_Datum)0;
        }
        snprintf(result, 12, "%u", tuple_oid);
        insert_record->m_new_values[tupdesc->natts].m_info = INFO_NOTHING;
        insert_record->m_new_values[tupdesc->natts].m_freeFlag = true;
        insert_record->m_new_values[tupdesc->natts].m_value = result;
        insert_record->m_new_values[tupdesc->natts].m_valueLen = strlen(result);
        insert_record->m_new_values[tupdesc->natts].m_coltype = PG_SYSDICT_OIDOID;
        insert_record->m_new_values[tupdesc->natts].m_colName = pg_parser_mcxt_strndup("oid", 3);
    }

    *result = (pg_parser_translog_tbcolbase*)insert_record;
    /* Release tuple, desc, tbinfo.attr */
    if (tuple)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }
    if (tupdesc)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    }
    if (tbinfo.pgattr)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    }
    if (zicinfo.zicdata)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    }
    return true;
}

/* insert */
static bool pg_parser_trans_rmgr_heap_insert_get_tuple(pg_parser_XLogReaderState*     state,
                                                       pg_parser_translog_tbcolbase** result,
                                                       int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_values* insert_record = NULL;
    pg_parser_xl_heap_insert*        xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf* tuple = NULL;
    uint32_t                         relfilenode = 0;
    size_t                           datalen = 0;
    size_t                           tuplelen = 0;
    char*                            page = NULL;
    char*                            tupledata = NULL;
    int16_t                          dbtype = state->trans_data->m_dbtype;
    char*                            dbversion = state->trans_data->m_dbversion;

    xlrec = (pg_parser_xl_heap_insert*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)(&insert_record), sizeof(pg_parser_translog_tbcol_values)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_00;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap insert], MALLOC failed\n");
        return false;
    }

    insert_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_INSERT;
    insert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    /* Replica mode and system table */
    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        void* temp_tuple = NULL;
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_01;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make full image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            insert_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(state->trans_data->m_dbtype,
                                                                                      state->trans_data->m_dbversion,
                                                                                      state->trans_data->m_pagesize,
                                                                                      page,
                                                                                      &insert_record->m_tupleCnt,
                                                                                      state->blocks[0].blkno,
                                                                                      state->trans_data->m_debugLevel);
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap insert], get %u tuples from image\n",
                                 insert_record->m_tupleCnt);
            if (!insert_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make return tuple failed\n");
                return false;
            }
            /* Set return type */
            insert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            insert_record->m_relfilenode = relfilenode;

            temp_tuple = (void*)pg_parser_assemble_tuple(dbtype,
                                                         state->trans_data->m_dbversion,
                                                         state->trans_data->m_pagesize,
                                                         page,
                                                         xlrec->offnum);
            if (!temp_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make tuple failed\n");
                return false;
            }

            tuple = (pg_parser_ReorderBufferTupleBuf*)temp_tuple;

            /* Full page write data extracted, tuple assembled, release page */
            if (page)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            uint32_t tuplen = 0;
            tupledata = pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TUPLE2;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], get tuple from block failed\n");
                return false;
            }

            tuplelen = datalen - pg_parser_SizeOfHeapHeader;

            temp_tuple = (void*)pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
            if (!temp_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_INSERT_GET_TEMP_TUPLE2;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], make tuple from block failed\n");
                return false;
            }
            pg_parser_reassemble_tuple_from_wal_data(tupledata,
                                                     datalen,
                                                     (pg_parser_ReorderBufferTupleBuf*)temp_tuple,
                                                     state->decoded_record->xl_xid,
                                                     pg_parser_InvalidTransactionId,
                                                     dbtype,
                                                     dbversion);

            tuple = (pg_parser_ReorderBufferTupleBuf*)temp_tuple;
            tuplen = tuple->tuple.t_len;

            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(insert_record->m_tuple),
                                       sizeof(pg_parser_translog_tuplecache)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_02;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }
            insert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* Process tuple data for return */
            insert_record->m_tuple->m_pageno = state->blocks[0].blkno;
            insert_record->m_tupleCnt = 1;
            insert_record->m_relfilenode = relfilenode;
            insert_record->m_tuple->m_tuplelen = tuplen;
            insert_record->m_tuple->m_itemoffnum = xlrec->offnum;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap insert], return tuple "
                                 "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                 insert_record->m_tuple->m_itemoffnum,
                                 insert_record->m_tuple->m_pageno,
                                 insert_record->m_tuple->m_tuplelen);
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&(insert_record->m_tuple->m_tupledata), tuplen))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_03;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap insert], MALLOC failed\n");
                return false;
            }

            rmemcpy0(insert_record->m_tuple->m_tupledata, 0, tuple->tuple.t_data, tuple->tuple.t_len);
        }
    }

    *result = (pg_parser_translog_tbcolbase*)insert_record;
    /* Release tuple, desc, tbinfo.attr */
    if (tuple)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }

    return true;
}

/* multi insert */
static bool pg_parser_trans_rmgr_heap2_minsert_trans(pg_parser_XLogReaderState*     state,
                                                     pg_parser_translog_tbcolbase** result,
                                                     int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_nvalues*       minsert_record = NULL;
    pg_parser_xl_heap_multi_insert*         xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf**       tuple = NULL;
    pg_parser_sysdict_tableInfo             tbinfo = {'\0'};
    pg_parser_TupleDesc                     tupdesc = NULL;
    pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t                                 natt = 0;
    uint32_t                                relfilenode = 0;
    size_t                                  datalen = 0;
    char*                                   page = NULL;
    char*                                   data = NULL;
    bool                                    isinit = false;
    int32_t                                 i = 0;
    uint16_t                                off_num = 0;
    int16_t                                 dbtype = state->trans_data->m_dbtype;
    char*                                   dbversion = state->trans_data->m_dbversion;

    /* Check validity of input/output parameters */
    if (!state || !result || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MINSERT_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], invalid param\n");
        return false;
    }

    xlrec = (pg_parser_xl_heap_multi_insert*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)(&minsert_record),
                               sizeof(pg_parser_translog_tbcol_nvalues)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_05;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    minsert_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT;
    minsert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if (!pg_parser_sysdict_getTableInfo(relfilenode, state->trans_data->m_sysdicts, &tbinfo))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_TBINFO;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], get table info failed\n");
        return false;
    }
    minsert_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    minsert_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    minsert_record->m_relid = tbinfo.oid;
    minsert_record->m_valueCnt = (uint16_t)tbinfo.natts;
    minsert_record->m_rowCnt = xlrec->ntuples;
    natt = tbinfo.natts;

    if (PG_PARSER_XLOG_HEAP_INIT_PAGE & pg_parser_XLogRecGetInfo(state))
    {
        isinit = true;
    }

    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&tuple,
                               sizeof(pg_parser_ReorderBufferTupleBuf*) * xlrec->ntuples))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_07;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    if (state->trans_data->m_iscatalog)
    {
        minsert_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    }
    else
    {
        minsert_record->m_base.m_tabletype = PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    }
    /* Replica mode and system table */
    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_08;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert], get image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            minsert_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(state->trans_data->m_dbtype,
                                                                                       state->trans_data->m_dbversion,
                                                                                       state->trans_data->m_pagesize,
                                                                                       page,
                                                                                       &minsert_record->m_tupleCnt,
                                                                                       state->blocks[0].blkno,
                                                                                       state->trans_data->m_debugLevel);
            if (!minsert_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert],"
                                     "get tuple from image failed\n");
                return false;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap nulti insert], get %u tuples from image\n",
                                 minsert_record->m_tupleCnt);
            /* Set return type */
            minsert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            minsert_record->m_relfilenode = relfilenode;

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                {
                    off_num = i + 1;
                }
                else
                {
                    off_num = xlrec->offsets[i];
                }
                tuple[i] = pg_parser_assemble_tuple(dbtype,
                                                    state->trans_data->m_dbversion,
                                                    state->trans_data->m_pagesize,
                                                    page,
                                                    off_num);
                if (!tuple[i])
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 multi_insert],"
                                         "get tuple failed\n");
                    return false;
                }
            }

            /* Full page write data extracted, tuple assembled, release page */
            if (page)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            data = pg_parser_XLogRecGetBlockData(state, 0, &datalen);

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                pg_parser_xl_multi_insert_tuple* xlhdr = NULL;
                xlhdr = (pg_parser_xl_multi_insert_tuple*)PG_PARSER_SHORTALIGN(data);
                data = ((char*)xlhdr) + pg_parser_SizeOfMultiInsertTuple;
                datalen = xlhdr->datalen;
                tuple[i] = pg_parser_heaptuple_get_tuple_space(datalen, dbtype, dbversion);
                if (!tuple[i])
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 multi_insert],"
                                         "get tuple failed\n");
                    return false;
                }
                reassemble_mutituple_from_wal_data(data, datalen, tuple[i], xlhdr, dbtype, dbversion);
                data += datalen;
            }

            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(minsert_record->m_tuple),
                                       sizeof(pg_parser_translog_tuplecache) * minsert_record->m_rowCnt))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_09;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            minsert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* Process tuple data for return */
            minsert_record->m_tupleCnt = minsert_record->m_rowCnt;
            minsert_record->m_relfilenode = relfilenode;
            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                {
                    off_num = i + 1;
                }
                else
                {
                    off_num = xlrec->offsets[i];
                }
                minsert_record->m_tuple[i].m_pageno = state->blocks[0].blkno;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;

                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                           (void**)&(minsert_record->m_tuple[i].m_tupledata),
                                           tuple[i]->tuple.t_len))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0B;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                    return false;
                }
                rmemcpy0(minsert_record->m_tuple[i].m_tupledata, 0, tuple[i]->tuple.t_data, tuple[i]->tuple.t_len);
            }
            if (PG_PARSER_DEBUG_SILENCE < state->trans_data->m_debugLevel)
            {
                for (i = 0; i < (int32_t)minsert_record->m_tupleCnt; i++)
                {
                    printf(
                        "DEBUG: trans record is [heap multi insert], return tuple "
                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                        minsert_record->m_tuple[i].m_itemoffnum,
                        minsert_record->m_tuple[i].m_pageno,
                        minsert_record->m_tuple[i].m_tuplelen);
                }
            }
        }
    }
    else
    {
        /* Logical mode */
        if (!(xlrec->flags & PG_PARSER_TRANS_XLH_INSERT_CONTAINS_NEW_TUPLE))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_LOGICAL_NEW_FLAG;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap2 multi_insert],"
                                 "logical mode but don't have new tuple \n");
            return false;
        }
        data = pg_parser_XLogRecGetBlockData(state, 0, &datalen);
        if (!data)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_LOGICAL_DATA;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap2 multi_insert],"
                                 "logical mode don't have data \n");
            return false;
        }
        for (i = 0; i < minsert_record->m_rowCnt; i++)
        {
            pg_parser_xl_multi_insert_tuple* xlhdr = NULL;
            xlhdr = (pg_parser_xl_multi_insert_tuple*)PG_PARSER_SHORTALIGN(data);
            data = ((char*)xlhdr) + pg_parser_SizeOfMultiInsertTuple;
            datalen = xlhdr->datalen;

            tuple[i] = pg_parser_heaptuple_get_tuple_space(datalen, dbtype, dbversion);
            if (!tuple[i])
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert],"
                                     "get tuple failed\n");
                return false;
            }
            reassemble_mutituple_from_wal_data(data, datalen, tuple[i], xlhdr, dbtype, dbversion);

            data += datalen;
        }
        /* Set return type, logical mode does not need to return page/tuple information */
        minsert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
    }
    tupdesc = pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_MAKE_DESC;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert],"
                             "get tuple failed\n");
        return false;
    }

    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&minsert_record->m_rows,
                               minsert_record->m_rowCnt * sizeof(pg_parser_translog_tbcol_rows)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0C;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    for (i = 0; i < minsert_record->m_rowCnt; i++)
    {
        if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                   (void**)&minsert_record->m_rows[i].m_new_values,
                                   minsert_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0D;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
            return false;
        }
        zicinfo.convertinfo = state->trans_data->m_convert;
        /* Extract column values from tuple and convert to readable information */
        for (natt = 0; natt < tupdesc->natts; natt++)
        {
            pg_parser_Datum                 origval = (pg_parser_Datum)0;
            pg_parser_translog_tbcol_value* colvalue = &minsert_record->m_rows[i].m_new_values[natt];
            bool                            isnull = true;
            bool                            ismissing = false;
            colvalue->m_info = INFO_NOTHING;
            if (tupdesc->attrs[natt].attisdropped)
            {
                colvalue->m_info = INFO_COL_IS_DROPED;
                continue;
            }
            /* Assign column name, reuse memory address from sysdict, no need to release */
            colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
            colvalue->m_freeFlag = true;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: column name: %s\n",
                                 colvalue->m_colName ? colvalue->m_colName : "NULL");

            origval =
                pg_parser_heap_getattr(&tuple[i]->tuple, natt + 1, tupdesc, &isnull, &ismissing, dbtype, dbversion);

            colvalue->m_coltype = tupdesc->attrs[natt].atttypid;

            zicinfo.dbtype = dbtype;
            zicinfo.dbversion = dbversion;
            zicinfo.errorno = pg_parser_errno;
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
            else if (!pg_parser_convert_attr_to_str_value(origval, state->trans_data->m_sysdicts, colvalue, &zicinfo))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_CONVERT_ATTR_TO_CHAR;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert],"
                                     "convert attr to str failed\n");
                return false;
            }
        }
    }

    *result = (pg_parser_translog_tbcolbase*)minsert_record;
    /* Release tuple, desc, tbinfo.pgattr */
    if (tuple)
    {
        for (i = 0; i < minsert_record->m_rowCnt; i++)
        {
            pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple[i]);
        }
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }

    if (tupdesc)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    }
    if (tbinfo.pgattr)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    }
    if (zicinfo.zicdata)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    }

    return true;
}

/* multi insert */
static bool pg_parser_trans_rmgr_heap2_minsert_get_tuple(pg_parser_XLogReaderState*     state,
                                                         pg_parser_translog_tbcolbase** result,
                                                         int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_nvalues* minsert_record = NULL;
    pg_parser_xl_heap_multi_insert*   xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf** tuple = NULL;
    uint32_t                          relfilenode = 0;
    size_t                            datalen = 0;
    char*                             page = NULL;
    char*                             data = NULL;
    bool                              isinit = false;
    int32_t                           i = 0;
    uint16_t                          off_num = 0;
    int16_t                           dbtype = state->trans_data->m_dbtype;
    char*                             dbversion = state->trans_data->m_dbversion;

    xlrec = (pg_parser_xl_heap_multi_insert*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)(&minsert_record),
                               sizeof(pg_parser_translog_tbcol_nvalues)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_05;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    minsert_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT;
    minsert_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    minsert_record->m_rowCnt = xlrec->ntuples;

    if (PG_PARSER_XLOG_HEAP_INIT_PAGE & pg_parser_XLogRecGetInfo(state))
    {
        isinit = true;
    }

    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&tuple,
                               sizeof(pg_parser_ReorderBufferTupleBuf*) * xlrec->ntuples))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_07;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
        return false;
    }

    /* Replica mode and system table */
    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_08;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert], get image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            minsert_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(state->trans_data->m_dbtype,
                                                                                       state->trans_data->m_dbversion,
                                                                                       state->trans_data->m_pagesize,
                                                                                       page,
                                                                                       &minsert_record->m_tupleCnt,
                                                                                       state->blocks[0].blkno,
                                                                                       state->trans_data->m_debugLevel);
            if (!minsert_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert],"
                                     "get tuple from image failed\n");
                return false;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap nulti insert], get %u tuples from image\n",
                                 minsert_record->m_tupleCnt);
            /* Set return type */
            minsert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            minsert_record->m_relfilenode = relfilenode;

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                {
                    off_num = i + 1;
                }
                else
                {
                    off_num = xlrec->offsets[i];
                }
                tuple[i] = pg_parser_assemble_tuple(dbtype,
                                                    state->trans_data->m_dbversion,
                                                    state->trans_data->m_pagesize,
                                                    page,
                                                    off_num);
                if (!tuple[i])
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 multi_insert],"
                                         "get tuple failed\n");
                    return false;
                }
            }

            /* Full page write data extracted, tuple assembled, release page */
            if (page)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        else
        {
            data = pg_parser_XLogRecGetBlockData(state, 0, &datalen);

            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                pg_parser_xl_multi_insert_tuple* xlhdr = NULL;
                xlhdr = (pg_parser_xl_multi_insert_tuple*)PG_PARSER_SHORTALIGN(data);
                data = ((char*)xlhdr) + pg_parser_SizeOfMultiInsertTuple;
                datalen = xlhdr->datalen;
                tuple[i] = pg_parser_heaptuple_get_tuple_space(datalen, dbtype, dbversion);
                if (!tuple[i])
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP2_MULTI_INSERT_GET_TEMP_TUPLE;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 multi_insert],"
                                         "get tuple failed\n");
                    return false;
                }
                reassemble_mutituple_from_wal_data(data, datalen, tuple[i], xlhdr, dbtype, dbversion);
                data += datalen;
            }

            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(minsert_record->m_tuple),
                                       sizeof(pg_parser_translog_tuplecache) * minsert_record->m_rowCnt))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_09;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                return false;
            }
            minsert_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;

            /* Process tuple data for return */
            minsert_record->m_tupleCnt = minsert_record->m_rowCnt;
            minsert_record->m_relfilenode = relfilenode;
            for (i = 0; i < minsert_record->m_rowCnt; i++)
            {
                if (isinit)
                {
                    off_num = i + 1;
                }
                else
                {
                    off_num = xlrec->offsets[i];
                }

                minsert_record->m_tuple[i].m_pageno = state->blocks[0].blkno;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                minsert_record->m_tuple[i].m_tuplelen = tuple[i]->tuple.t_len;
                minsert_record->m_tuple[i].m_itemoffnum = off_num;
                if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                           (void**)&(minsert_record->m_tuple[i].m_tupledata),
                                           tuple[i]->tuple.t_len))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0B;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 multi_insert], MALLOC failed\n");
                    return false;
                }
                rmemcpy0(minsert_record->m_tuple[i].m_tupledata, 0, tuple[i]->tuple.t_data, tuple[i]->tuple.t_len);
            }
            if (PG_PARSER_DEBUG_SILENCE < state->trans_data->m_debugLevel)
            {
                for (i = 0; i < (int32_t)minsert_record->m_tupleCnt; i++)
                {
                    printf(
                        "DEBUG: trans record is [heap multi insert], return tuple "
                        "itemoff[%u], pageno[%u], tuplen[%u]\n",
                        minsert_record->m_tuple[i].m_itemoffnum,
                        minsert_record->m_tuple[i].m_pageno,
                        minsert_record->m_tuple[i].m_tuplelen);
                }
            }
        }
    }

    *result = (pg_parser_translog_tbcolbase*)minsert_record;
    /* Release tuple, desc, tbinfo.pgattr */
    if (tuple)
    {
        for (i = 0; i < minsert_record->m_rowCnt; i++)
        {
            pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple[i]);
        }
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }

    return true;
}

static bool pg_parser_trans_rmgr_heap_delete_trans(pg_parser_XLogReaderState*     state,
                                                   pg_parser_translog_tbcolbase** result,
                                                   int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_values*        delete_record = NULL;
    pg_parser_xl_heap_delete*               xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf*        tuple = NULL;
    pg_parser_sysdict_tableInfo             tbinfo = {'\0'};
    pg_parser_TupleDesc                     tupdesc = NULL;
    pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t                                 natt = 0;
    uint32_t                                relfilenode = 0;
    size_t                                  datalen = 0;
    size_t                                  tuplelen = 0;
    char*                                   page = NULL;
    int16_t                                 dbtype = state->trans_data->m_dbtype;
    char*                                   dbversion = state->trans_data->m_dbversion;
    bool                                    have_oid = false;
    uint32_t                                tuple_oid = pg_parser_InvalidOid;

    /* Check validity of input/output parameters */
    if (!state || !result || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap2 delete], invalid param\n");
        return false;
    }

    xlrec = (pg_parser_xl_heap_delete*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)(&delete_record), sizeof(pg_parser_translog_tbcol_values)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0E;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap2 delete], MALLOC failed\n");
        return false;
    }

    delete_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_DELETE;
    delete_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if (!pg_parser_sysdict_getTableInfo(relfilenode, state->trans_data->m_sysdicts, &tbinfo))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TBINFO;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 delete],"
                             "get tbinfo failed\n");
        return false;
    }

    delete_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    delete_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    delete_record->m_relid = tbinfo.oid;
    delete_record->m_valueCnt = (uint16_t)tbinfo.natts;
    natt = tbinfo.natts;

    tupdesc = pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_DESC;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap2 delete],"
                             "make desc failed\n");
        return false;
    }

    /* Replica mode and system table */
    if (state->trans_data->m_iscatalog)
    {
        delete_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    }
    else
    {
        delete_record->m_base.m_tabletype = PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    }
    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0F;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "get image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            delete_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(state->trans_data->m_dbtype,
                                                                                      state->trans_data->m_dbversion,
                                                                                      state->trans_data->m_pagesize,
                                                                                      page,
                                                                                      &delete_record->m_tupleCnt,
                                                                                      state->blocks[0].blkno,
                                                                                      state->trans_data->m_debugLevel);
            if (!delete_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "get tuple failed\n");
                return false;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap delete], get %u tuples from image\n",
                                 delete_record->m_tupleCnt);

            /* Set return type */
            delete_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            delete_record->m_relfilenode = relfilenode;

            tuple = pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                             state->trans_data->m_dbversion,
                                             state->trans_data->m_pagesize,
                                             page,
                                             xlrec->offnum);
            if (!tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPEL_ALLOC;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "malloc tuple failed\n");
                return false;
            }

            /* Full page write data extracted, tuple assembled, release page */
            if (page)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }
        }
        /* Without full page write, we need to use the passed-in tuple data */
        else
        {
            void*                            temp_tuple = NULL;
            void*                            tuphdr = NULL;
            pg_parser_translog_translog2col* trans_data = state->trans_data;
            pg_parser_translog_tuplecache*   current_tuplecache = NULL;

            /* Check passed-in page data */
            if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
            {
                if (!strcmp(tbinfo.scname, "pg_toast") && !strncmp(tbinfo.tbname, "pg_toast", 8))
                {
                    int index_colnum = 0;
                    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                               (void**)&delete_record->m_old_values,
                                               delete_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
                    {
                        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_10;
                        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                             "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                        return false;
                    }
                    for (index_colnum = 0; index_colnum < delete_record->m_valueCnt; index_colnum++)
                    {
                        pg_parser_translog_tbcol_value* colvalue = &delete_record->m_old_values[index_colnum];
                        colvalue->m_colName = rstrdup(tbinfo.pgattr[index_colnum]->attname.data);
                        colvalue->m_freeFlag = true;
                        colvalue->m_coltype = tupdesc->attrs[index_colnum].atttypid;
                        colvalue->m_info = INFO_COL_MAY_NULL;
                    }
                    *result = (pg_parser_translog_tbcolbase*)delete_record;
                    /* Release tuple, desc, tbinfo.pgattr */
                    if (tuple)
                    {
                        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
                    }
                    if (tupdesc)
                    {
                        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
                    }
                    if (tbinfo.pgattr)
                    {
                        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
                    }
                    if (zicinfo.zicdata)
                    {
                        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
                    }
                    return true;
                }
                else
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE_IN;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap2 delete],"
                                         "tuple input check failed\n");
                    return false;
                }
            }
            delete_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
            current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                   trans_data->m_tuplecnt,
                                                                   xlrec->offnum,
                                                                   state->blocks[0].blkno);
            if (!current_tuplecache)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE_IN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "tuple input check failed\n");
                return false;
            }
            tuphdr = (void*)current_tuplecache->m_tupledata;
            tuplelen = (size_t)current_tuplecache->m_tuplelen;
            temp_tuple = (void*)pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
            if (!temp_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TEMP_ALLOC;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "malloc temp tuple failed\n");
                return false;
            }
            pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr,
                                                              tuplelen,
                                                              (pg_parser_ReorderBufferTupleBuf*)temp_tuple,
                                                              dbtype,
                                                              dbversion);

            tuple = (pg_parser_ReorderBufferTupleBuf*)temp_tuple;
        }
    }
    else
    {
        /* Logical mode, delete statement stores old tuple after maindata in logical mode */
        if (!(xlrec->flags & PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD))
        {
            /* Filter toast in logical mode, do not use goto here */
            int index_colnum = 0;
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&delete_record->m_old_values,
                                       delete_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_10;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                return false;
            }
            for (index_colnum = 0; index_colnum < delete_record->m_valueCnt; index_colnum++)
            {
                pg_parser_translog_tbcol_value* colvalue = &delete_record->m_old_values[index_colnum];
                colvalue->m_colName = rstrdup(tbinfo.pgattr[index_colnum]->attname.data);
                colvalue->m_freeFlag = true;
                colvalue->m_coltype = tupdesc->attrs[index_colnum].atttypid;
                colvalue->m_info = INFO_COL_MAY_NULL;
            }
            *result = (pg_parser_translog_tbcolbase*)delete_record;
            /* Release tuple, desc, tbinfo.pgattr */
            if (tuple)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
            }
            if (tupdesc)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
            }
            if (tbinfo.pgattr)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
            }
            if (zicinfo.zicdata)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
            }
            return true;
        }
        datalen = pg_parser_XLogRecGetDataLen(state) - pg_parser_SizeOfHeapDelete;
        if (0 == datalen)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_LOGICAL_DATA;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap2 delete],"
                                 "logical mode check data failed\n");
            return false;
        }

        tuplelen = datalen - pg_parser_SizeOfHeapHeader;
        tuple = pg_parser_heaptuple_get_tuple_space(tuplelen, dbtype, dbversion);
        if (!tuple)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPEL_ALLOC;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap2 delete],"
                                 "malloc tuple failed\n");
            return false;
        }
        pg_parser_DecodeXLogTuple((char*)xlrec + pg_parser_SizeOfHeapDelete, datalen, tuple, dbtype, dbversion);

        /* Set return type, logical mode does not need to return page/tuple information */
        delete_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
    }
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&delete_record->m_old_values,
                               delete_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_10;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap2 delete], MALLOC failed\n");
        return false;
    }
    zicinfo.convertinfo = state->trans_data->m_convert;
    /* Extract column values from tuple and convert to readable information */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        pg_parser_Datum                 origval = (pg_parser_Datum)0;
        pg_parser_translog_tbcol_value* colvalue = &delete_record->m_old_values[natt];
        bool                            isnull = true;
        bool                            ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }

        if (!strcmp(delete_record->m_base.m_schemaname, PG_TOAST_NAME))
        {
            zicinfo.istoast = true;
        }
        else
        {
            zicinfo.istoast = false;
        }

        /* Assign column name, reuse memory address from sysdict, no need to release */
        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "DEBUG: column name: %s\n",
                             colvalue->m_colName ? colvalue->m_colName : "NULL");

        origval = pg_parser_heap_getattr(&tuple->tuple, natt + 1, tupdesc, &isnull, &ismissing, dbtype, dbversion);

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
        zicinfo.errorno = pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (!pg_parser_convert_attr_to_str_value(origval, state->trans_data->m_sysdicts, colvalue, &zicinfo))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_ATTR_TO_STR;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap2 delete],"
                                 "convert attr to str failed\n");
            return false;
        }
    }

    if (have_oid)
    {
        char* result = NULL;
        if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&result, 12))
        {
            return (pg_parser_Datum)0;
        }
        snprintf(result, 12, "%u", tuple_oid);
        delete_record->m_old_values[tupdesc->natts].m_info = INFO_NOTHING;
        delete_record->m_old_values[tupdesc->natts].m_freeFlag = true;
        delete_record->m_old_values[tupdesc->natts].m_value = result;
        delete_record->m_old_values[tupdesc->natts].m_valueLen = strlen(result);
        delete_record->m_old_values[tupdesc->natts].m_coltype = PG_SYSDICT_OIDOID;
        delete_record->m_old_values[tupdesc->natts].m_colName = pg_parser_mcxt_strndup("oid", 3);
    }

    *result = (pg_parser_translog_tbcolbase*)delete_record;
    /* Release tuple, desc, tbinfo.pgattr */
    if (tuple)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple);
    }
    if (tupdesc)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    }
    if (tbinfo.pgattr)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    }
    if (zicinfo.zicdata)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    }
    return true;
}

static bool pg_parser_trans_rmgr_heap_delete_get_tuple(pg_parser_XLogReaderState*     state,
                                                       pg_parser_translog_tbcolbase** result,
                                                       int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_values* delete_record = NULL;
    uint32_t                         relfilenode = 0;
    char*                            page = NULL;

    /* Check validity of input/output parameters */
    if (!state || !result || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap2 delete], invalid param\n");
        return false;
    }

    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)(&delete_record), sizeof(pg_parser_translog_tbcol_values)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0E;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap2 delete], MALLOC failed\n");
        return false;
    }

    delete_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_DELETE;
    delete_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    /* Replica mode and system table */
    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_0F;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page, state->trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "get image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            delete_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(state->trans_data->m_dbtype,
                                                                                      state->trans_data->m_dbversion,
                                                                                      state->trans_data->m_pagesize,
                                                                                      page,
                                                                                      &delete_record->m_tupleCnt,
                                                                                      state->blocks[0].blkno,
                                                                                      state->trans_data->m_debugLevel);
            if (!delete_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_DELETE_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap2 delete],"
                                     "get tuple failed\n");
                return false;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap delete], get %u tuples from image\n",
                                 delete_record->m_tupleCnt);

            /* Set return type */
            delete_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            delete_record->m_relfilenode = relfilenode;

            /* Full page write data extracted, tuple assembled, release page */
            if (page)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
                page = NULL;
            }

            *result = (pg_parser_translog_tbcolbase*)delete_record;
        }
        else
        {
            pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, delete_record);
        }
    }
    else
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, delete_record);
    }

    return true;
}

static bool pg_parser_trans_rmgr_heap_update_trans(pg_parser_XLogReaderState*     state,
                                                   pg_parser_translog_tbcolbase** result,
                                                   int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_values*        update_record = NULL;
    pg_parser_xl_heap_update*               xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf*        tuple_new = NULL;
    pg_parser_ReorderBufferTupleBuf*        tuple_old = NULL;
    pg_parser_sysdict_tableInfo             tbinfo = {'\0'};
    pg_parser_TupleDesc                     tupdesc = NULL;
    pg_parser_translog_convertinfo_with_zic zicinfo = {'\0'};
    int32_t                                 natt = 0;
    uint32_t                                relfilenode = 0;
    size_t                                  datalen = 0;
    size_t                                  tuplelen_new = 0;
    size_t                                  tuplelen_old = 0;
    char*                                   page_new = NULL;
    char*                                   page_old = NULL;
    char*                                   tupledata_new = NULL;
    bool                                    use_logical = false;
    int16_t                                 dbtype = state->trans_data->m_dbtype;
    char*                                   dbversion = state->trans_data->m_dbversion;
    bool                                    have_oid = false;
    bool                                    have_old_tuple_data = true;
    uint32_t                                tuple_oid = pg_parser_InvalidOid;

    /* Check validity of input/output parameters */
    if (!state || !result || !pg_parser_errno)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_CHECK;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap update], invalid param\n");
        return false;
    }

    xlrec = (pg_parser_xl_heap_update*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)(&update_record), sizeof(pg_parser_translog_tbcol_values)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_11;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }

    update_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_UPDATE;
    update_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;
    if (!pg_parser_sysdict_getTableInfo(relfilenode, state->trans_data->m_sysdicts, &tbinfo))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TBINFO;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap update],"
                             "MALLOC failed\n");
        return false;
    }

    update_record->m_base.m_tbname = rstrdup(tbinfo.tbname);
    update_record->m_base.m_schemaname = rstrdup(tbinfo.scname);
    update_record->m_relid = tbinfo.oid;
    update_record->m_valueCnt = tbinfo.natts;
    natt = tbinfo.natts;
    /* Replica mode and system table */
    if (state->trans_data->m_iscatalog)
    {
        update_record->m_base.m_tabletype = check_table_type(tbinfo.oid);
    }
    else
    {
        update_record->m_base.m_tabletype = PG_PARSER_TRANSLOG_TABLETYPE_NORMAL;
    }
    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            pg_parser_translog_translog2col* trans_data = state->trans_data;
            void*                            tuphdr_old = NULL;
            void*                            temp_tuple_new = NULL;

            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page_new, trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_12;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page_new, trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "make image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            update_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(dbtype,
                                                                                      state->trans_data->m_dbversion,
                                                                                      state->trans_data->m_pagesize,
                                                                                      page_new,
                                                                                      &update_record->m_tupleCnt,
                                                                                      state->blocks[0].blkno,
                                                                                      state->trans_data->m_debugLevel);
            if (!update_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "make return tuple failed\n");
                return false;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap update], get %u tuples from image\n",
                                 update_record->m_tupleCnt);
            /* Set return type */
            update_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            update_record->m_relfilenode = relfilenode;

            /* Check if old and new tuples are in the same page block */
            if (0 == state->max_block_id)
            {
                /* When in the same page block */
                void* temp_tuple_old = NULL;
                page_old = page_new;
                temp_tuple_old = (void*)pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                                 state->trans_data->m_dbversion,
                                                                 state->trans_data->m_pagesize,
                                                                 page_old,
                                                                 xlrec->old_offnum);
                tuple_old = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old;
            }
            else
            {
                pg_parser_translog_tuplecache* current_tuplecache = NULL;
                void*                          temp_tuple_old = NULL;

                /* When not in same page block, use passed-in tuple, check passed-in page data */
                if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap update],"
                                         "input tuple check failed\n");
                    return false;
                }
                current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                       trans_data->m_tuplecnt,
                                                                       xlrec->old_offnum,
                                                                       state->blocks[1].blkno);
                if (!current_tuplecache)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap update],"
                                         "off:%u, block:%u,"
                                         " input tuple check failed\n",
                                         xlrec->old_offnum,
                                         state->blocks[0].blkno);
                    return false;
                }
                tuphdr_old = (void*)current_tuplecache->m_tupledata;
                tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
                temp_tuple_old = pg_parser_heaptuple_get_tuple_space(tuplelen_old, dbtype, dbversion);
                if (!temp_tuple_old)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap update],"
                                         "get old tuple failed\n");
                    return false;
                }
                pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                                  tuplelen_old,
                                                                  (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old,
                                                                  dbtype,
                                                                  dbversion);
                tuple_old = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old;
            }
            temp_tuple_new = (void*)pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                             state->trans_data->m_dbversion,
                                                             state->trans_data->m_pagesize,
                                                             page_new,
                                                             xlrec->new_offnum);
            if (!temp_tuple_new)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "get new tuple failed\n");
                return false;
            }
            tuple_new = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_new;
            /* Full page write data extracted, tuple assembled, release page */
            if (page_new)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page_new);
                page_new = NULL;
            }
        }
        /* Without full page write, we need to use passed-in page data for old tuple */
        else
        {
            void*                            tuphdr_old = NULL;
            void*                            temp_tuple_old = NULL;
            void*                            temp_tuple_new = NULL;
            size_t                           temp_len = 0;
            uint32_t                         temp_tuplen = 0;
            pg_parser_translog_translog2col* trans_data = state->trans_data;

            pg_parser_translog_tuplecache*   current_tuplecache = NULL;
            /* Use passed-in tuple, check passed-in page data */
            if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "input tuple check failed\n");
                return false;
            }
            if (0 == state->max_block_id)
            {
                current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                       trans_data->m_tuplecnt,
                                                                       xlrec->old_offnum,
                                                                       state->blocks[0].blkno);
            }
            else
            {
                current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                       trans_data->m_tuplecnt,
                                                                       xlrec->old_offnum,
                                                                       state->blocks[1].blkno);
            }
            if (!current_tuplecache)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "input tuple check failed\n");
                return false;
            }
            tuphdr_old = (void*)current_tuplecache->m_tupledata;
            tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
            temp_len = tuplelen_old - pg_parser_SizeOfHeapHeader;
            temp_tuple_old = (void*)pg_parser_heaptuple_get_tuple_space(temp_len, dbtype, dbversion);
            if (!temp_tuple_old)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "get old tuple failed\n");
                return false;
            }
            pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                              tuplelen_old,
                                                              (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old,
                                                              dbtype,
                                                              dbversion);
            tuple_old = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old;
            /* Combine old data, assemble new tuple from record */
            tupledata_new = pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata_new)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "get new tuple failed\n");
                return false;
            }
            if (!reassemble_tuplenew_from_wal_data(tupledata_new,
                                                   datalen,
                                                   (pg_parser_ReorderBufferTupleBuf**)(&temp_tuple_new),
                                                   xlrec,
                                                   state->blocks[0].blkno,
                                                   pg_parser_InvalidTransactionId,
                                                   tuphdr_old,
                                                   tuplelen_old,
                                                   dbtype,
                                                   dbversion))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_MAKE_NEW_TUPLE_FROM_OLD;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "make new tuple from old tuple failed\n");
                return false;
            }
            tuple_new = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_new;
            /* Set return type, update returns new tuple, old tuple does not need to return */
            update_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(update_record->m_tuple),
                                       sizeof(pg_parser_translog_tuplecache)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_13;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* Process tuple data for return */
            update_record->m_tuple->m_pageno = state->blocks[0].blkno;
            update_record->m_tupleCnt = 1;
            update_record->m_relfilenode = relfilenode;
            temp_tuplen = tuple_new->tuple.t_len;
            update_record->m_tuple->m_tuplelen = temp_tuplen;
            update_record->m_tuple->m_itemoffnum = xlrec->new_offnum;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap insert], return tuple "
                                 "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                 update_record->m_tuple->m_itemoffnum,
                                 update_record->m_tuple->m_pageno,
                                 update_record->m_tuple->m_tuplelen);
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(update_record->m_tuple->m_tupledata),
                                       temp_tuplen))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_14;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }

            rmemcpy0(update_record->m_tuple->m_tupledata, 0, tuple_new->tuple.t_data, tuple_new->tuple.t_len);
        }
    }
    else
    {
        /* Logical mode, update statement stores old tuple after maindata in logical mode */
        use_logical = true;
        datalen = pg_parser_XLogRecGetDataLen(state) - pg_parser_SizeOfHeapUpdate;

        have_old_tuple_data = ((xlrec->flags & PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD) || datalen > 0) ? true : false;
        /* Only process data when old data flag is present */
        if (have_old_tuple_data)
        {
            /* Extract old tuple from record */
            tuplelen_old = datalen - pg_parser_SizeOfHeapHeader;
            tuple_old = pg_parser_heaptuple_get_tuple_space(tuplelen_old, dbtype, dbversion);
            if (!tuple_old)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_LOGICAL_MALLOC_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "logical malloc tuple failed\n");
                return false;
            }
            pg_parser_DecodeXLogTuple((char*)xlrec + pg_parser_SizeOfHeapUpdate, datalen, tuple_old, dbtype, dbversion);
        }

        /* Extract new tuple from record */
        datalen = 0;
        tupledata_new = pg_parser_XLogRecGetBlockData(state, 0, &datalen);
        if (!tupledata_new)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_LOGICAL_GET_NEW_TUPLE_DATA;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap update],"
                                 "logical get new tuple data failed\n");
            return false;
        }
        tuplelen_new = datalen - pg_parser_SizeOfHeapHeader;
        tuple_new = pg_parser_heaptuple_get_tuple_space(tuplelen_new, dbtype, dbversion);
        if (!tuple_new)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_LOGICAL_MALLOC_TUPLE;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap update],"
                                 "logical malloc tuple failed\n");
            return false;
        }
        pg_parser_reassemble_tuple_from_wal_data(tupledata_new,
                                                 datalen,
                                                 tuple_new,
                                                 state->decoded_record->xl_xid,
                                                 pg_parser_InvalidTransactionId,
                                                 dbtype,
                                                 dbversion);

        /* Set return type, logical mode does not need to return page/tuple information */
        update_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_DATA;
    }
    tupdesc = pg_parser_get_desc(&tbinfo);
    if (!tupdesc)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_DESC;
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "ERROR: trans record is [heap update],"
                             "get desc failed\n");
        return false;
    }

    /* Allocate memory for old columns */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&update_record->m_old_values,
                               update_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_15;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }

    /* Allocate memory for new columns */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                               (void**)&update_record->m_new_values,
                               update_record->m_valueCnt * sizeof(pg_parser_translog_tbcol_value)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_16;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }
    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                         "DEBUG: tuple block[%u] old off[%u], new off[%u]\n",
                         state->blocks[0].blkno,
                         xlrec->old_offnum,
                         xlrec->new_offnum);

    zicinfo.convertinfo = state->trans_data->m_convert;
    /* Extract old column values from tuple and convert to readable information */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        pg_parser_Datum                 origval = (pg_parser_Datum)0;
        pg_parser_translog_tbcol_value* colvalue = &update_record->m_old_values[natt];
        bool                            isnull = true;
        bool                            ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }
        /* Assign column name, reuse memory address from sysdict, no need to release */
        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "DEBUG: column name: %s\n",
                             colvalue->m_colName ? colvalue->m_colName : "NULL");
        if (!have_old_tuple_data)
        {
            colvalue->m_info = INFO_COL_MAY_NULL;
            continue;
        }

        origval = pg_parser_heap_getattr(&tuple_old->tuple, natt + 1, tupdesc, &isnull, &ismissing, dbtype, dbversion);

        if (isnull)
        {
            if (use_logical)
            {
                colvalue->m_info = INFO_COL_MAY_NULL;
            }
            else
            {
                colvalue->m_info = INFO_COL_IS_NULL;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel, "DEBUG: column value is null\n");
            continue;
        }
        if (ismissing)
        {
            colvalue->m_info = INFO_COL_MAY_NULL;
            pg_parser_log_errlog(state->trans_data->m_debugLevel, "DEBUG: column value is null\n");
            continue;
        }
        zicinfo.dbtype = dbtype;
        zicinfo.dbversion = dbversion;
        colvalue->m_coltype = tupdesc->attrs[natt].atttypid;
        zicinfo.errorno = pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (!pg_parser_convert_attr_to_str_value(origval, state->trans_data->m_sysdicts, colvalue, &zicinfo))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_OLD_VALUE_ATTR_TO_STR;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap update],"
                                 "old value convert attr to str failed\n");
            return false;
        }
    }

    /* Extract new column values from tuple and convert to readable information */
    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        pg_parser_Datum                 origval = (pg_parser_Datum)0;
        pg_parser_translog_tbcol_value* colvalue = &update_record->m_new_values[natt];
        bool                            isnull = true;
        bool                            ismissing = false;
        colvalue->m_info = INFO_NOTHING;
        colvalue->m_freeFlag = true;
        if (tupdesc->attrs[natt].attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }
        /* Assign column name, reuse memory address from sysdict, no need to release */
        colvalue->m_colName = rstrdup(tbinfo.pgattr[natt]->attname.data);
        pg_parser_log_errlog(state->trans_data->m_debugLevel,
                             "DEBUG: column name: %s\n",
                             colvalue->m_colName ? colvalue->m_colName : "NULL");

        origval = pg_parser_heap_getattr(&tuple_new->tuple, natt + 1, tupdesc, &isnull, &ismissing, dbtype, dbversion);
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
        zicinfo.errorno = pg_parser_errno;
        zicinfo.debuglevel = state->trans_data->m_debugLevel;
        if (!pg_parser_convert_attr_to_str_value(origval, state->trans_data->m_sysdicts, colvalue, &zicinfo))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_NEW_VALUE_ATTR_TO_STR;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "ERROR: trans record is [heap update],"
                                 "new value convert attr to str failed\n");
            return false;
        }
    }

    if (have_oid)
    {
        char* result = NULL;
        if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&result, 12))
        {
            return (pg_parser_Datum)0;
        }
        snprintf(result, 12, "%u", tuple_oid);
        update_record->m_new_values[tupdesc->natts].m_info = INFO_NOTHING;
        update_record->m_new_values[tupdesc->natts].m_freeFlag = true;
        update_record->m_new_values[tupdesc->natts].m_value = result;
        update_record->m_new_values[tupdesc->natts].m_valueLen = strlen(result);
        update_record->m_new_values[tupdesc->natts].m_coltype = PG_SYSDICT_OIDOID;
        update_record->m_new_values[tupdesc->natts].m_colName = pg_parser_mcxt_strndup("oid", 3);

        update_record->m_old_values[tupdesc->natts].m_info = INFO_NOTHING;
        update_record->m_old_values[tupdesc->natts].m_freeFlag = true;
        update_record->m_old_values[tupdesc->natts].m_value = pg_parser_mcxt_strndup(result, 12);
        update_record->m_old_values[tupdesc->natts].m_valueLen = strlen(result);
        update_record->m_old_values[tupdesc->natts].m_coltype = PG_SYSDICT_OIDOID;
        update_record->m_old_values[tupdesc->natts].m_colName = pg_parser_mcxt_strndup("oid", 3);
    }

    *result = (pg_parser_translog_tbcolbase*)update_record;
    /* Release used tuple_new, tuple_old, desc, tbinfo.pgattr */
    if (tuple_new)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_new);
    }
    if (tuple_old)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_old);
    }
    if (tupdesc)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tupdesc);
    }
    if (tbinfo.pgattr)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tbinfo.pgattr);
    }
    if (zicinfo.zicdata)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, zicinfo.zicdata);
    }
    return true;
}

static bool pg_parser_trans_rmgr_heap_update_get_tuple(pg_parser_XLogReaderState*     state,
                                                       pg_parser_translog_tbcolbase** result,
                                                       int32_t*                       pg_parser_errno)
{
    pg_parser_translog_tbcol_values* update_record = NULL;
    pg_parser_xl_heap_update*        xlrec = NULL;
    pg_parser_ReorderBufferTupleBuf* tuple_new = NULL;
    pg_parser_ReorderBufferTupleBuf* tuple_old = NULL;
    uint32_t                         relfilenode = 0;
    size_t                           datalen = 0;
    size_t                           tuplelen_old = 0;
    char*                            page_new = NULL;
    char*                            page_old = NULL;
    char*                            tupledata_new = NULL;
    int16_t                          dbtype = state->trans_data->m_dbtype;
    char*                            dbversion = state->trans_data->m_dbversion;

    xlrec = (pg_parser_xl_heap_update*)pg_parser_XLogRecGetData(state);
    /* Allocate memory for return value */
    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)(&update_record), sizeof(pg_parser_translog_tbcol_values)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_11;
        pg_parser_log_errlog(state->trans_data->m_debugLevel, "ERROR: trans record is [heap update], MALLOC failed\n");
        return false;
    }

    update_record->m_base.m_dmltype = PG_PARSER_TRANSLOG_DMLTYPE_UPDATE;
    update_record->m_base.m_originid = state->record_origin;
    relfilenode = state->blocks[0].rnode.relNode;

    if (PG_PARSER_WALLEVEL_REPLICA == state->trans_data->m_walLevel || state->trans_data->m_iscatalog)
    {
        /* Has full page write, extract tuple */
        if (pg_parser_XLogRecHasBlockImage(state, 0))
        {
            pg_parser_translog_translog2col* trans_data = state->trans_data;
            void*                            tuphdr_old = NULL;
            void*                            temp_tuple_new = NULL;

            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page_new, trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_12;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* Assemble complete page */
            if (!pg_parser_image_get_block_image(state, 0, page_new, trans_data->m_pagesize))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_IMAGE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "make image failed\n");
                return false;
            }
            /* Process full page write data for return, extract tuple from full page write */
            update_record->m_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(dbtype,
                                                                                      state->trans_data->m_dbversion,
                                                                                      state->trans_data->m_pagesize,
                                                                                      page_new,
                                                                                      &update_record->m_tupleCnt,
                                                                                      state->blocks[0].blkno,
                                                                                      state->trans_data->m_debugLevel);
            if (!update_record->m_tuple)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "make return tuple failed\n");
                return false;
            }
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap update], get %u tuples from image\n",
                                 update_record->m_tupleCnt);
            /* Set return type */
            update_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            update_record->m_relfilenode = relfilenode;

            /* Check if old and new tuples are in the same page block */
            if (0 == state->max_block_id)
            {
                /* When in the same page block */
                void* temp_tuple_old = NULL;
                page_old = page_new;
                temp_tuple_old = (void*)pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                                 state->trans_data->m_dbversion,
                                                                 state->trans_data->m_pagesize,
                                                                 page_old,
                                                                 xlrec->old_offnum);

                tuple_old = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old;
            }
            else
            {
                pg_parser_translog_tuplecache* current_tuplecache = NULL;
                void*                          temp_tuple_old = NULL;

                /* When not in same page block, use passed-in tuple, check passed-in page data */
                if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap update],"
                                         "input tuple check failed\n");
                    return false;
                }
                current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                       trans_data->m_tuplecnt,
                                                                       xlrec->old_offnum,
                                                                       state->blocks[1].blkno);
                if (!current_tuplecache)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap update],"
                                         "off:%u, block:%u,"
                                         " input tuple check failed\n",
                                         xlrec->old_offnum,
                                         state->blocks[0].blkno);
                    return false;
                }
                tuphdr_old = (void*)current_tuplecache->m_tupledata;
                tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
                temp_tuple_old = pg_parser_heaptuple_get_tuple_space(tuplelen_old, dbtype, dbversion);
                if (!temp_tuple_old)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                    pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                         "ERROR: trans record is [heap update],"
                                         "get old tuple failed\n");
                    return false;
                }
                pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                                  tuplelen_old,
                                                                  (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old,
                                                                  dbtype,
                                                                  dbversion);

                tuple_old = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old;
            }
            temp_tuple_new = (void*)pg_parser_assemble_tuple(state->trans_data->m_dbtype,
                                                             state->trans_data->m_dbversion,
                                                             state->trans_data->m_pagesize,
                                                             page_new,
                                                             xlrec->new_offnum);
            if (!temp_tuple_new)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "get new tuple failed\n");
                return false;
            }

            tuple_new = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_new;

            /* Full page write data extracted, tuple assembled, release page */
            if (page_new)
            {
                pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page_new);
                page_new = NULL;
            }
        }
        /* Without full page write, we need to use passed-in page data for old tuple */
        else
        {
            void*                            tuphdr_old = NULL;
            void*                            temp_tuple_old = NULL;
            void*                            temp_tuple_new = NULL;
            size_t                           temp_len = 0;
            uint32_t                         temp_tuplen = 0;
            pg_parser_translog_translog2col* trans_data = state->trans_data;

            pg_parser_translog_tuplecache*   current_tuplecache = NULL;
            /* Use passed-in tuple, check passed-in page data */
            if (!(trans_data->m_tuplecnt) || !(trans_data->m_tuples))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "input tuple check failed\n");
                return false;
            }
            if (0 == state->max_block_id)
            {
                current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                       trans_data->m_tuplecnt,
                                                                       xlrec->old_offnum,
                                                                       state->blocks[0].blkno);
            }
            else
            {
                current_tuplecache = pg_parser_image_getTupleFromCache(trans_data->m_tuples,
                                                                       trans_data->m_tuplecnt,
                                                                       xlrec->old_offnum,
                                                                       state->blocks[1].blkno);
            }
            if (!current_tuplecache)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_TUPLE_RETURN;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "input tuple check failed\n");
                return false;
            }
            tuphdr_old = (void*)current_tuplecache->m_tupledata;
            tuplelen_old = (size_t)current_tuplecache->m_tuplelen;
            temp_len = tuplelen_old - pg_parser_SizeOfHeapHeader;
            temp_tuple_old = (void*)pg_parser_heaptuple_get_tuple_space(temp_len, dbtype, dbversion);
            if (!temp_tuple_old)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_OLD_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "get old tuple failed\n");
                return false;
            }
            pg_parser_reassemble_tuple_from_heap_tuple_header(tuphdr_old,
                                                              tuplelen_old,
                                                              (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old,
                                                              dbtype,
                                                              dbversion);
            tuple_old = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_old;
            /* Combine old data, assemble new tuple from record */
            tupledata_new = pg_parser_XLogRecGetBlockData(state, 0, &datalen);
            if (!tupledata_new)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_GET_NEW_TUPLE;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "get new tuple failed\n");
                return false;
            }
            if (!reassemble_tuplenew_from_wal_data(tupledata_new,
                                                   datalen,
                                                   (pg_parser_ReorderBufferTupleBuf**)(&temp_tuple_new),
                                                   xlrec,
                                                   state->blocks[0].blkno,
                                                   pg_parser_InvalidTransactionId,
                                                   tuphdr_old,
                                                   tuplelen_old,
                                                   dbtype,
                                                   dbversion))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_FUNCERR_HEAP_UPDATE_MAKE_NEW_TUPLE_FROM_OLD;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update],"
                                     "make new tuple from old tuple failed\n");
                return false;
            }
            tuple_new = (pg_parser_ReorderBufferTupleBuf*)temp_tuple_new;
            /* Set return type, update returns new tuple, old tuple does not need to return */
            update_record->m_base.m_type |= PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE;
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(update_record->m_tuple),
                                       sizeof(pg_parser_translog_tuplecache)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_13;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            /* Process tuple data for return */
            update_record->m_tuple->m_pageno = state->blocks[0].blkno;
            update_record->m_tupleCnt = 1;
            update_record->m_relfilenode = relfilenode;
            temp_tuplen = tuple_new->tuple.t_len;
            update_record->m_tuple->m_tuplelen = temp_tuplen;
            update_record->m_tuple->m_itemoffnum = xlrec->new_offnum;
            pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                 "DEBUG: trans record is [heap insert], return tuple "
                                 "itemoff[%u], pageno[%u], tuplen[%u]\n",
                                 update_record->m_tuple->m_itemoffnum,
                                 update_record->m_tuple->m_pageno,
                                 update_record->m_tuple->m_tuplelen);
            if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT,
                                       (void**)&(update_record->m_tuple->m_tupledata),
                                       temp_tuplen))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_14;
                pg_parser_log_errlog(state->trans_data->m_debugLevel,
                                     "ERROR: trans record is [heap update], MALLOC failed\n");
                return false;
            }
            rmemcpy0(update_record->m_tuple->m_tupledata, 0, tuple_new->tuple.t_data, tuple_new->tuple.t_len);
        }
    }

    *result = (pg_parser_translog_tbcolbase*)update_record;
    /* Release used tuple_new, tuple_old, desc, tbinfo.pgattr */
    if (tuple_new)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_new);
    }
    if (tuple_old)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, tuple_old);
    }

    return true;
}

bool pg_parser_check_fpw(pg_parser_XLogReaderState*    readstate,
                         pg_parser_translog_pre_base** pg_parser_result,
                         int32_t*                      pg_parser_errno,
                         int16_t                       dbtype)
{
    pg_parser_translog_pre_image_tuple* pre_tuple = NULL;
    uint8_t                             info = readstate->decoded_record->xl_info;
    int32_t                             blcknum = 0;
    bool                                hasimage = false;

    info &= ~PG_PARSER_TRANS_TRANSREC_XLR_INFO_MASK;
    info &= PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK;

    PG_PARSER_UNUSED(pg_parser_errno);
    PG_PARSER_UNUSED(dbtype);

    if (!(PG_PARSER_TRANSLOG_RMGR_HEAP_ID == readstate->decoded_record->xl_rmid ||
          PG_PARSER_TRANSLOG_RMGR_HEAP2_ID == readstate->decoded_record->xl_rmid ||
          PG_PARSER_TRANSLOG_RMGR_XLOG_ID == readstate->decoded_record->xl_rmid))
    {
        return false;
    }

    for (blcknum = 0; blcknum < readstate->max_block_id + 1; blcknum++)
    {
        if (pg_parser_XLogRecHasBlockImage(readstate, blcknum) && readstate->blocks[blcknum].in_use)
        {
            hasimage = true;
            break;
        }
    }
    if (!hasimage)
    {
        return false;
    }

    if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&pre_tuple, sizeof(pg_parser_translog_pre_image_tuple)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_18;
        pg_parser_log_errlog(readstate->trans_data->m_debugLevel, "ERROR: check FPW, MALLOC failed\n");
        return false;
    }

    for (blcknum = 0; blcknum < readstate->max_block_id + 1; blcknum++)
    {
        char*                          page = NULL;
        uint32_t                       pageno = 0;
        pg_parser_translog_tuplecache* temp_tuple = NULL;
        uint32_t                       block_tuple_cnt = 0;

        if (!pg_parser_XLogRecHasBlockImage(readstate, blcknum) || !readstate->blocks[blcknum].in_use)
        {
            continue;
        }

        if (!pg_parser_mcxt_malloc(TRANS_RMGR_HEAP_MCXT, (void**)&page, readstate->pre_trans_data->m_pagesize))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_TRANS_MEMERR_ALLOC_17;
            pg_parser_log_errlog(readstate->trans_data->m_debugLevel, "ERROR: check FPW, MALLOC failed\n");
            return false;
        }
        /* Assemble complete page */
        if (!pg_parser_image_get_block_image(readstate, blcknum, page, readstate->pre_trans_data->m_pagesize))
        {
            pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
            *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_IMAGE_FPW;
            return false;
        }

        pageno = readstate->blocks[blcknum].blkno;

        /* Extract tuple from page */
        temp_tuple = pg_parser_image_get_tuple_from_image_with_dbtype(readstate->pre_trans_data->m_dbtype,
                                                                      readstate->pre_trans_data->m_dbversion,
                                                                      readstate->pre_trans_data->m_pagesize,
                                                                      page,
                                                                      &block_tuple_cnt,
                                                                      pageno,
                                                                      readstate->pre_trans_data->m_debugLevel);
        if (!temp_tuple)
        {
            /* If no valid tuple found in this page, do not report error and continue */
            pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
            pg_parser_log_errlog(readstate->pre_trans_data->m_debugLevel,
                                 "DEBUG: pre trans record is [xlog/heap/heap2 FPW],"
                                 "try get tuples from image, but get NULL\n");
            continue;
        }
        pg_parser_log_errlog(readstate->pre_trans_data->m_debugLevel,
                             "DEBUG: trans record is [xlog/heap/heap2 FPW], get %u tuples from image\n",
                             block_tuple_cnt);
        if (pre_tuple->m_tuples)
        {
            pg_parser_mcxt_realloc(TRANS_RMGR_HEAP_MCXT,
                                   (void**)&pre_tuple->m_tuples,
                                   (pre_tuple->m_tuplecnt + block_tuple_cnt) * sizeof(pg_parser_translog_tuplecache));
            rmemcpy1((char*)(pre_tuple->m_tuples),
                     pre_tuple->m_tuplecnt * sizeof(pg_parser_translog_tuplecache),
                     temp_tuple,
                     block_tuple_cnt * sizeof(pg_parser_translog_tuplecache));
            pre_tuple->m_tuplecnt += block_tuple_cnt;
            pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, temp_tuple);
        }
        else
        {
            pre_tuple->m_tuplecnt = block_tuple_cnt;
            pre_tuple->m_tuples = temp_tuple;
        }
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, page);
    }

    /* If no valid tuple found in this page, do not report error and return false */
    if (!pre_tuple->m_tuples)
    {
        pg_parser_mcxt_free(TRANS_RMGR_HEAP_MCXT, pre_tuple);
        return false;
    }

    /* Process relfilenode information for tuple data to return */
    pre_tuple->m_relfilenode = readstate->blocks[0].rnode.relNode;
    pre_tuple->m_dboid = readstate->blocks[0].rnode.dbNode;
    pre_tuple->m_tbspcoid = readstate->blocks[0].rnode.spcNode;

    pre_tuple->m_transid = readstate->decoded_record->xl_xid;
    /* Set return type */
    pre_tuple->m_base.m_type = PG_PARSER_TRANSLOG_FPW_TUPLE;
    pre_tuple->m_base.m_xid = pg_parser_XLogRecGetXid(readstate);
    pre_tuple->m_base.m_originid = readstate->record_origin;
    *pg_parser_result = (pg_parser_translog_pre_base*)pre_tuple;

    return true;
}

static bool reassemble_tuplenew_from_wal_data(char*                             data,
                                              size_t                            len,
                                              pg_parser_ReorderBufferTupleBuf** result_new,
                                              pg_parser_xl_heap_update*         xlrec,
                                              uint32_t                          blknum_new,
                                              pg_parser_TransactionId           xid,
                                              pg_parser_HeapTupleHeader         htup_old_in,
                                              int32_t                           old_tuple_len,
                                              int16_t                           dbtype,
                                              char*                             dbversion)
{
    pg_parser_xl_heap_header         xlhdr;
    pg_parser_HeapTupleHeader        htup; /* Pointer for convenient coding when assembling new tuple. */
    pg_parser_ReorderBufferTupleBuf* recorbuff_new = NULL;
    pg_parser_HeapTupleHeader        htup_old = htup_old_in;
    char*                            newp = NULL;
    char *                           recdata, *recdata_end;
    int                              suffixlen = 0;
    int                              prefixlen = 0;
    int                              tuplen = 0; /* Record the length of user data in update wal record. */
    int                              reass_tuple_len = 0;
    pg_parser_ItemPointerData        target_tid;

    recdata = data;
    recdata_end = data + len;

    if (xlrec->flags & PG_PARSER_TRANS_XLH_UPDATE_PREFIX_FROM_OLD)
    {
        rmemcpy1(&prefixlen, 0, recdata, sizeof(uint16_t));
        recdata += sizeof(uint16_t);
    }
    if (xlrec->flags & PG_PARSER_TRANS_XLH_UPDATE_SUFFIX_FROM_OLD)
    {
        rmemcpy1(&suffixlen, 0, recdata, sizeof(uint16_t));
        recdata += sizeof(uint16_t);
    }
    rmemcpy1((char*)&xlhdr, 0, recdata, pg_parser_SizeOfHeapHeader);
    recdata += pg_parser_SizeOfHeapHeader;

    tuplen = recdata_end - recdata;
    reass_tuple_len = pg_parser_SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;

    recorbuff_new = pg_parser_heaptuple_get_tuple_space(tuplen + prefixlen + suffixlen, dbtype, dbversion);
    if (!recorbuff_new)
    {
        return false;
    }
    recorbuff_new->tuple.t_len = reass_tuple_len;

    htup = recorbuff_new->tuple.t_data;

    pg_parser_ItemPointerSetInvalid(&recorbuff_new->tuple.t_self);
    pg_parser_ItemPointerSet(&target_tid, blknum_new, xlrec->new_offnum);

    /* we can only figure this out after reassembling the transactions. */
    recorbuff_new->tuple.t_tableOid = pg_parser_InvalidOid;

    htup = (pg_parser_HeapTupleHeader)recorbuff_new->tuple.t_data;

    newp = (char*)htup + pg_parser_SizeofHeapTupleHeader;

    if (prefixlen > 0)
    {
        int len;

        /* Copy padding before actual data. */
        len = xlhdr.t_hoff - pg_parser_SizeofHeapTupleHeader;
        rmemcpy1(newp, 0, recdata, len);
        recdata += len;
        newp += len;

        /* copy prefix from old tuple. */
        rmemcpy1(newp, 0, (char*)htup_old + htup_old->t_hoff, prefixlen);
        newp += prefixlen;

        /* copy new tuple data from WAL record. */
        len = tuplen - (xlhdr.t_hoff - pg_parser_SizeofHeapTupleHeader);
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
    {
        rmemcpy1(newp, 0, (char*)htup_old + old_tuple_len - suffixlen, suffixlen);
    }

    htup->t_infomask2 = xlhdr.t_infomask2;
    htup->t_infomask = xlhdr.t_infomask;
    htup->t_hoff = xlhdr.t_hoff;
    pg_parser_HeapTupleHeaderSetXmin(htup, xid);
    pg_parser_HeapTupleHeaderSetCmin(htup, pg_parser_FirstCommandId);
    pg_parser_HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
    /* Make sure there is no forward chain link in t_ctid. */
    htup->t_ctid = target_tid;
    *result_new = recorbuff_new;

    return true;
}

static void reassemble_mutituple_from_wal_data(char*                            data,
                                               size_t                           len,
                                               pg_parser_ReorderBufferTupleBuf* tup,
                                               pg_parser_xl_multi_insert_tuple* xlhdr,
                                               int16_t                          dbtype,
                                               char*                            dbversion)
{
    pg_parser_HeapTupleHeader        header;
    pg_parser_ReorderBufferTupleBuf* tuple = tup;
    header = tuple->tuple.t_data;
    pg_parser_ItemPointerSetInvalid(&tuple->tuple.t_self);
    tuple->tuple.t_tableOid = pg_parser_InvalidOid;
    tuple->tuple.t_len = len + pg_parser_SizeofHeapTupleHeader;

    rmemset1(header, 0, 0, pg_parser_SizeofHeapTupleHeader);

    rmemcpy1(((char*)tuple->tuple.t_data) + pg_parser_SizeofHeapTupleHeader, 0, data, len);

    header->t_infomask = xlhdr->t_infomask;
    header->t_infomask2 = xlhdr->t_infomask2;
    header->t_hoff = xlhdr->t_hoff;
}
