/**
 * @file pg_parser_thirdparty_tupleparser_record.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-03
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define PGFUNC_RECORD_MCXT                           NULL

#define pg_parser_HeapTupleHeaderGetTypeId(tup)      ((tup)->t_choice.t_datum.datum_typeid)

#define pg_parser_HeapTupleHeaderGetDatumLength(tup) PG_PARSER_VARSIZE(tup)

pg_parser_Datum record_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool                                         is_toast = false;
    bool                                         need_free = false;
    pg_parser_HeapTupleHeader                    rec = NULL;
    uint32_t                                     tupType;
    pg_parser_TupleDesc                          tupdesc;
    pg_parser_HeapTupleData                      tuple;
    int                                          ncolumns;
    int                                          i;
    pg_parser_Datum*                             values;
    bool*                                        nulls;

    pg_parser_sysdict_tableInfo                  tbinfo = {'\0'};
    pg_parser_translog_tbcol_valuetype_customer* custom = NULL;
    pg_parser_translog_tbcol_valuetype_customer* current_custom = NULL;

    rec = (pg_parser_HeapTupleHeader)pg_parser_detoast_datum((struct pg_parser_varlena*)attr,
                                                             &is_toast,
                                                             &need_free,
                                                             info->zicinfo->dbtype,
                                                             info->zicinfo->dbversion);
    if (is_toast)
    {
        if (info != NULL)
        {
            info->valueinfo = INFO_COL_IS_TOAST;
        }
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum)rec;
    }

    info->valueinfo = INFO_COL_IS_CUSTOM;

    /* Extract type info from the tuple itself */
    tupType = pg_parser_HeapTupleHeaderGetTypeId(rec);
    if (!pg_parser_sysdict_getTableInfo_byoid(get_typrelid_by_typid(info->sysdicts, tupType), info->sysdicts, &tbinfo))
    {
        return (pg_parser_Datum)0;
    }
    tupdesc = pg_parser_get_desc(&tbinfo);
    ncolumns = tupdesc->natts;

    /* Build a temporary HeapTuple control structure */
    tuple.t_len = pg_parser_HeapTupleHeaderGetDatumLength(rec);
    pg_parser_ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = pg_parser_InvalidOid;
    tuple.t_data = rec;

    if (!pg_parser_mcxt_malloc(PGFUNC_RECORD_MCXT,
                               (void**)&custom,
                               ncolumns * sizeof(pg_parser_translog_tbcol_valuetype_customer)))
    {
        return (pg_parser_Datum)0;
    }

    if (!pg_parser_mcxt_malloc(PGFUNC_RECORD_MCXT, (void**)&values, ncolumns * sizeof(pg_parser_Datum)))
    {
        return (pg_parser_Datum)0;
    }
    if (!pg_parser_mcxt_malloc(PGFUNC_RECORD_MCXT, (void**)&nulls, ncolumns * sizeof(bool)))
    {
        return (pg_parser_Datum)0;
    }

    /* Break down the tuple into fields */
    pg_parser_heap_deform_tuple(&tuple, tupdesc, values, nulls);
    current_custom = custom;
    for (i = 0; i < ncolumns; i++)
    {
        pg_sysdict_Form_pg_attribute    att = pg_parser_TupleDescAttr(tupdesc, i);
        pg_parser_translog_tbcol_value* colvalue = NULL;
        pg_parser_Datum                 attr;

        if (!pg_parser_mcxt_malloc(PGFUNC_RECORD_MCXT, (void**)&colvalue, sizeof(pg_parser_translog_tbcol_value)))
        {
            return (pg_parser_Datum)0;
        }

        colvalue->m_colName = att->attname.data;
        colvalue->m_coltype = att->atttypid;

        /* Ignore dropped columns in datatype */
        if (att->attisdropped)
        {
            colvalue->m_info = INFO_COL_IS_DROPED;
            continue;
        }

        if (nulls[i])
        {
            /* emit nothing... */
            colvalue->m_info = INFO_COL_IS_NULL;
            continue;
        }

        attr = values[i];

        if (!pg_parser_convert_attr_to_str_value(attr, info->sysdicts, colvalue, info->zicinfo))
        {
            return (pg_parser_Datum)0;
        }
        current_custom->m_value = colvalue;
        if (i < ncolumns - 1)
        {
            current_custom->m_next = &custom[i + 1];
            current_custom = current_custom->m_next;
        }
    }
    if (values)
    {
        pg_parser_mcxt_free(PGFUNC_RECORD_MCXT, values);
    }
    if (nulls)
    {
        pg_parser_mcxt_free(PGFUNC_RECORD_MCXT, nulls);
    }
    if (tupdesc)
    {
        pg_parser_mcxt_free(PGFUNC_RECORD_MCXT, tupdesc);
    }
    /* ReleaseTupleDesc(tupdesc); */
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_RECORD_MCXT, rec);
    }
    if (tbinfo.pgattr)
    {
        pg_parser_mcxt_free(PGFUNC_RECORD_MCXT, tbinfo.pgattr);
    }
    return (pg_parser_Datum)custom;
}