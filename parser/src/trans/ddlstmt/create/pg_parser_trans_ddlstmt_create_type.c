#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_CREATE_TYPE_MCXT NULL

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_create_type(
    pg_parser_translog_systb2ddl* pg_parser_ddl, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno, char typtype);

/*
 * Create type:
 * 1. Entry is inserting a tuple into pg_type;
 * 2. Exit is inserting relnatts attribute values into pg_attribute.
 * This process contains entry for create table, create view
 */
pg_parser_translog_ddlstmt* pg_parser_DDL_create_type(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;
    char*                       temp_str = NULL;
    char*                       typtype_temp = NULL;

    if (!ddlstate->m_type_item)
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_TYPE1;
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    typtype_temp =
        PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typtype", ddlstate->m_type_item->m_new_values,
                                           ddlstate->m_type_item->m_valueCnt, typtype_temp);
    switch (typtype_temp[0])
    {
        /* The first record inserting into pg_type encountered is of type c, composite type */
        case PG_SYSDICT_TYPTYPE_COMPOSITE:
        {
            /* Current record is insert operation */
            if (IS_INSERT(current_record->m_record))
            {
                /* Current record operates on pg_class */
                if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS,
                                               pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                {
                    char* temp_relkind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "relkind", current_record->m_record->m_new_values,
                        current_record->m_record->m_valueCnt, temp_relkind);
                    if (!temp_relkind)
                    {
                        // errcode
                        return false;
                    }
                    /* Current record in pg_class has relkind r or p, represents this is a table
                     * record */
                    if (PG_SYSDICT_RELKIND_RELATION == temp_relkind[0])
                    {
                        /* Switch from create type flow to create normal table flow */
                        char* temp_ispartition = NULL;
                        pg_parser_ddl_init_ddlstate(ddlstate);

                        temp_ispartition = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "relispartition", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_ispartition);
                        /* Actually there is no partition table here, logic for determining if it is
                         * partition table child is in create table in pg_depend */
                        if ('t' == temp_ispartition[0])
                        {
                            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                                 "DEBUG, DDL PARSER: in create type, stmt type "
                                                 "change to create table partition of \n");
                            ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB;
                        }
                        else
                        {
                            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                                 "DEBUG, DDL PARSER: in create type, stmt type "
                                                 "change to create table \n");
                            ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_CREATE;
                        }
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record,
                                                             true))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (PG_SYSDICT_RELKIND_PARTITIONED_TABLE == temp_relkind[0])
                    {
                        /* Switch from create type flow to create partition table flow */
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                             "DEBUG, DDL PARSER: in create type, stmt type change "
                                             "to create table partition by \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_CREATE_PARTITION;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record,
                                                             true))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    /* View and materialized view */
                    else if (PG_SYSDICT_RELKIND_VIEW == temp_relkind[0] ||
                             PG_SYSDICT_RELKIND_MATVIEW == temp_relkind[0])
                    {
                        /* Switch from create type flow to create view flow */
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                             "DEBUG, DDL PARSER: in create type, stmt type change "
                                             "to create view \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_VIEW_CREATE;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record,
                                                             true))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (PG_SYSDICT_RELKIND_TOASTVALUE == temp_relkind[0])
                    {
                        /* Out-of-line storage processing, for filtering */
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        pg_parser_log_errlog(
                            pg_parser_ddl->m_debugLevel,
                            "DEBUG, DDL PARSER: in create type, stmt type change to skip toast \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_TOAST_ESCAPE;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record,
                                                             true))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (PG_SYSDICT_RELKIND_SEQUENCE == temp_relkind[0])
                    {
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                             "DEBUG, DDL PARSER: in create type, stmt type change "
                                             "to create sequence \n");
                        ddlstate->m_ddlKind = PG_PARSER_DDL_SEQUENCE_CREATE;
                        if (!pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record,
                                                             true))
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (PG_SYSDICT_RELKIND_COMPOSITE_TYPE == temp_relkind[0])
                    {
                        /* Create composite type, capture column value count of composite type */
                        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "relnatts", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_str);
                        ddlstate->m_type_record_natts = strtoul(temp_str, NULL, 10);
                        pg_parser_log_errlog(
                            pg_parser_ddl->m_debugLevel,
                            "DEBUG, DDL PARSER: create type, type is composite, get some info \n");
                    }
                    else
                    {
                        /*
                         * Under current parsing logic, program will not run here
                         * This is a safety mechanism to prevent parsing errors caused by unknown
                         * DDL statements Therefore when encountering this situation, treat current
                         * record as first record and reprocess DDL
                         */
                        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                             "WARNING: pg_parser_DDL_create_type JUMP UP DDL\n");
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        pg_parser_ddl_firstTransDDL(pg_parser_ddl, current_record, ddlstate,
                                                    pg_parser_errno);
                    }
                }
                /* Column value processing for composite type */
                else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                                    SYS_ATTRIBUTE, pg_parser_ddl->m_dbtype,
                                                    pg_parser_ddl->m_dbversion))
                {
                    if (!pg_parser_ddl_get_attribute_info(ddlstate, current_record->m_record, true))
                    {
                        *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
                        return NULL;
                    }
                    ddlstate->m_type_current_natts++;
                    if (ddlstate->m_type_record_natts == ddlstate->m_type_current_natts)
                    {
                        result = pg_parser_ddl_assemble_create_type(
                            pg_parser_ddl, ddlstate, pg_parser_errno, PG_SYSDICT_TYPTYPE_COMPOSITE);
                    }
                }
            }
            break;
        }
        /* First record's typtype = e, represents this is creating enum type*/
        case PG_SYSDICT_TYPTYPE_ENUM:
        {
            if (IS_INSERT(current_record->m_record))
            {
                if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ENUM,
                                               pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                {
                    ddlstate->m_enumlist =
                        pg_parser_list_lappend(ddlstate->m_enumlist, current_record->m_record);
                }

                else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                                    SYS_DEPEND, pg_parser_ddl->m_dbtype,
                                                    pg_parser_ddl->m_dbversion))
                {
                    char* temp_typarray = NULL;
                    char* temp_objid = NULL;
                    char* temp_classid = NULL;

                    temp_typarray = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "typarray", ddlstate->m_type_item->m_new_values,
                        ddlstate->m_type_item->m_valueCnt, temp_typarray);
                    temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "objid", current_record->m_record->m_new_values,
                        current_record->m_record->m_valueCnt, temp_objid);
                    temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "classid", current_record->m_record->m_new_values,
                        current_record->m_record->m_valueCnt, temp_classid);

                    if (!strcmp(temp_typarray, temp_objid) &&
                        !strcmp(TypeRelationIdChar, temp_classid))
                    {
                        result = pg_parser_ddl_assemble_create_type(
                            pg_parser_ddl, ddlstate, pg_parser_errno, PG_SYSDICT_TYPTYPE_ENUM);
                    }
                }
            }
            break;
        }
        /* range type */
        case PG_SYSDICT_TYPTYPE_RANGE:
        {
            /*Exclude statements other than INSERT*/
            if (IS_INSERT(current_record->m_record))
            {
                /* Capture data written to pg_range table */
                if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_RANGE,
                                               pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                {
                    ddlstate->m_type_sub_item = current_record->m_record;
                }
                /*
                 * Process of creating pg_range automatically creates two functions
                 * The pg_depend exit we need to capture is after the second function
                 * Therefore set flag here
                 */
                else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                                    SYS_PROC, pg_parser_ddl->m_dbtype,
                                                    pg_parser_ddl->m_dbversion))
                {
                    ddlstate->m_type_current_natts++;
                }
                /* Encountered pg_depend statement, check if reached exit */
                else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                                    SYS_DEPEND, pg_parser_ddl->m_dbtype,
                                                    pg_parser_ddl->m_dbversion))
                {
                    /* Quick exclusion */
                    if (2 == ddlstate->m_type_current_natts)
                    {
                        char* temp_refobjid = NULL;
                        char* temp_classid = NULL;
                        char* temp_refclassid = NULL;
                        temp_refobjid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "refobjid", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_refobjid);
                        temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "classid", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_classid);
                        temp_refclassid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "refclassid", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_refclassid);
                        if (!temp_refobjid || !temp_classid || !temp_refclassid)
                        {
                            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_37;
                            return NULL;
                        }
                        if (!strcmp(ddlstate->m_reloid_char, temp_refobjid) &&
                            !strcmp(ProcedureRelationIdChar, temp_classid) &&
                            !strcmp(TypeRelationIdChar, temp_refclassid))
                        {
                            result = pg_parser_ddl_assemble_create_type(
                                pg_parser_ddl, ddlstate, pg_parser_errno, PG_SYSDICT_TYPTYPE_RANGE);
                        }
                    }
                }
            }
            break;
        }
        case PG_SYSDICT_TYPTYPE_PSEUDO:
        {
            /* range type starts as p pseudo type, so we wait for update operation here */
            if (IS_UPDATE(current_record->m_record))
            {
                if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_TYPE,
                                               pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                {
                    char* oid_old = NULL;
                    char* oid_new = NULL;
                    bool  typischange = NULL;
                    oid_old = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "oid", ddlstate->m_type_item->m_new_values,
                        ddlstate->m_type_item->m_valueCnt, oid_old);
                    oid_new = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "oid", current_record->m_record->m_new_values,
                        current_record->m_record->m_valueCnt, oid_new);
                    typischange = pg_parser_ddl_checkChangeColumn(
                        "typtype", current_record->m_record->m_new_values,
                        current_record->m_record->m_old_values,
                        current_record->m_record->m_valueCnt, pg_parser_errno);
                    if (!strcmp(oid_old, oid_new) && typischange)
                    {
                        char* temp_oid = NULL;
                        char* temp_nspoid = NULL;
                        temp_oid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "oid", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_oid);
                        temp_nspoid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "typnamespace", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, temp_nspoid);
                        pg_parser_ddl_init_ddlstate(ddlstate);
                        ddlstate->m_ddlKind = PG_PARSER_DDL_TYPE_CREATE;

                        ddlstate->m_reloid_char = temp_oid;
                        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
                        ddlstate->m_nspname_oid_char = temp_nspoid;
                        ddlstate->m_type_domain = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "typbasetype", current_record->m_record->m_new_values,
                            current_record->m_record->m_valueCnt, ddlstate->m_type_domain);
                        ddlstate->m_type_item = current_record->m_record;
                        ddlstate->m_inddl = true;
                    }
                }
            }
            /* If empty type is created, we need to capture insert of empty type in pg_depend */
            if (IS_INSERT(current_record->m_record))
            {
                char* temp_typisdefined = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                    "typisdefined", ddlstate->m_type_item->m_new_values,
                    ddlstate->m_type_item->m_valueCnt, temp_typisdefined);
                if ('f' == temp_typisdefined[0] &&
                    pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                               SYS_DEPEND, pg_parser_ddl->m_dbtype,
                                               pg_parser_ddl->m_dbversion))
                {
                    pg_parser_translog_systb2dll_record* next = current_record->m_next;
                    /* Check if next statement is update pg_type */
                    if (next &&
                        next->m_record->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_UPDATE &&
                        pg_parser_check_table_name(next->m_record->m_base.m_tbname, SYS_TYPE,
                                                   pg_parser_ddl->m_dbtype,
                                                   pg_parser_ddl->m_dbversion))
                    {
                        char* oid_old = NULL;
                        char* oid_new = NULL;
                        bool  typischange = NULL;
                        oid_old = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                            "oid", ddlstate->m_type_item->m_new_values,
                            ddlstate->m_type_item->m_valueCnt, oid_old);
                        oid_new =
                            PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid", next->m_record->m_new_values,
                                                               next->m_record->m_valueCnt, oid_new);
                        typischange = pg_parser_ddl_checkChangeColumn(
                            "typtype", next->m_record->m_new_values, next->m_record->m_old_values,
                            next->m_record->m_valueCnt, pg_parser_errno);
                        if (!strcmp(oid_old, oid_new) && typischange)
                        {
                            /* This is a create range statement */
                            break;
                        }
                    }
                    result = pg_parser_ddl_assemble_create_type(
                        pg_parser_ddl, ddlstate, pg_parser_errno, PG_SYSDICT_TYPTYPE_NULL);
                }
            }
            break;
        }
#if 0
        case PG_SYSDICT_TYPTYPE_PSEUDO:
        {
            if (IS_UPDATE(mchange))
            {
                if (TypeRelationId == mte->reloid &&
                    TYPTYPE_PSEUDO == ddldata->type_item.typtype)
                {
                    Form_pg_type fpt = NULL;
                    int search_tabid = WAL2SQL_IMPTSYSCLASS_PGTYPE;

                    fpt = (Form_pg_type)get_tuple_from_change(true, mchange);
                    ddldata->ddlKind = DDLNO_TYPE_CREATE;
                    ddldata->reloid = fpt->oid;
                    ddldata->type_item.typtype = fpt->typtype;
                    ddldata->type_item.typein = fpt->typinput;
                    ddldata->type_item.typeout = fpt->typoutput;
                    ddldata->type_item.typarray = fpt->typarray;
                    adj_dict(search_tabid, WAL2SQL_ADJTYPE_UPDATE,
                             &mchange->data.tp.newtuple->tuple, wds);
                }
            }
            else if (IS_INSERT(mchange))
            {
                if (DependRelationId == mte->reloid)
                {
                    Form_pg_depend fpd = NULL;

                    fpd = (Form_pg_depend)get_tuple_from_change(true, mchange);
                    if (ddldata->reloid == fpd->objid &&
                        TypeRelationId == fpd->classid)
                        assemble_create_type(mte, ddldata, wds);
                }
            }
            break;
        }
#endif
        /* Create domain type */
        case PG_SYSDICT_TYPTYPE_DOMAIN:
        {
            /*Exclude statements other than INSERT*/
            if (IS_INSERT(current_record->m_record))
            {
                /* Capture exit pg_depend */
                if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname,
                                               SYS_DEPEND, pg_parser_ddl->m_dbtype,
                                               pg_parser_ddl->m_dbversion))
                {
                    char* temp_refobjid = NULL;
                    char* temp_classid = NULL;
                    temp_refobjid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "refobjid", current_record->m_record->m_new_values,
                        current_record->m_record->m_valueCnt, temp_refobjid);
                    temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                        "classid", current_record->m_record->m_new_values,
                        current_record->m_record->m_valueCnt, temp_classid);

                    if (!strcmp(ddlstate->m_reloid_char, temp_refobjid) &&
                        !strcmp(TypeRelationIdChar, temp_classid))
                    {
                        result = pg_parser_ddl_assemble_create_type(
                            pg_parser_ddl, ddlstate, pg_parser_errno, PG_SYSDICT_TYPTYPE_DOMAIN);
                    }
                }
            }
            break;
        }
        default:
        {
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                 "WARNING: unsupport CREATE TYPE type\n");
            pg_parser_ddl_init_ddlstate(ddlstate);
            break;
        }
    }
    return result;
}

/* Process return value of create type */
static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_create_type(
    pg_parser_translog_systb2ddl* pg_parser_ddl, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno, char typtype)
{
    pg_parser_translog_ddlstmt*      result = NULL;
    pg_parser_translog_ddlstmt_type* type_return = NULL;
    char*                            temp_str = NULL;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    // todo free
    if (!pg_parser_mcxt_malloc(DDL_CREATE_TYPE_MCXT, (void**)&result,
                               sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_38;
        return NULL;
    }
    // todo free
    if (!pg_parser_mcxt_malloc(DDL_CREATE_TYPE_MCXT, (void**)&type_return,
                               sizeof(pg_parser_translog_ddlstmt_type)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_39;
        return NULL;
    }

    temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typowner", ddlstate->m_type_item->m_new_values,
                                                  ddlstate->m_type_item->m_valueCnt, temp_str);
    type_return->m_owner = (uint32_t)strtoul(temp_str, NULL, 10);

    if (PG_SYSDICT_TYPTYPE_COMPOSITE == typtype)
    {
        pg_parser_translog_ddlstmt_typcol* typcol = NULL;
        int32_t                            i = 0;
        pg_parser_ListCell*                cell = NULL;
        pg_parser_translog_tbcol_values*   typatt = NULL;

        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: in create type, type is composite type \n");
        type_return->m_typtype = PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_COMPOSITE;
        type_return->m_type_name = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "typname", ddlstate->m_type_item->m_new_values, ddlstate->m_type_item->m_valueCnt,
            type_return->m_type_name);

        /* Append subtype information stored in dlstate->m_ddlList to return value */

        type_return->m_typvalcnt = ddlstate->m_type_record_natts;
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        // todo free
        if (!pg_parser_mcxt_malloc(
                DDL_CREATE_TYPE_MCXT, (void**)&typcol,
                sizeof(pg_parser_translog_ddlstmt_typcol) * type_return->m_typvalcnt))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3A;
            return NULL;
        }
        i = 0;
        pg_parser_foreach(cell, ddlstate->m_attList)
        {
            typatt = (pg_parser_translog_tbcol_values*)pg_parser_lfirst(cell);
            typcol[i].m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                "attname", typatt->m_new_values, typatt->m_valueCnt, typcol[i].m_colname);
            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid", typatt->m_new_values,
                                                          typatt->m_valueCnt, temp_str);
            typcol[i].m_coltypid = strtoul(temp_str, NULL, 10);
            i++;
        }
        type_return->m_typptr = (void*)typcol;
    }
    else if (PG_SYSDICT_TYPTYPE_ENUM == typtype)
    {
        pg_parser_translog_ddlstmt_valuebase* enumvalue = NULL;
        int32_t                               i = 0;
        pg_parser_ListCell*                   cell = NULL;
        pg_parser_translog_tbcol_values*      temp_value = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: in create type, type is enum type \n");
        type_return->m_typtype = PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_ENUM;
        type_return->m_type_name = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "typname", ddlstate->m_type_item->m_new_values, ddlstate->m_type_item->m_valueCnt,
            type_return->m_type_name);

        /* Append enum value name information stored in dlstate->m_enumlist to return value */

        type_return->m_typvalcnt = ddlstate->m_enumlist->length;
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

        // todo free
        if (!pg_parser_mcxt_malloc(
                DDL_CREATE_TYPE_MCXT, (void**)&enumvalue,
                sizeof(pg_parser_translog_ddlstmt_valuebase) * type_return->m_typvalcnt))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3B;
            return NULL;
        }
        i = 0;
        pg_parser_foreach(cell, ddlstate->m_enumlist)
        {
            temp_value = (pg_parser_translog_tbcol_values*)pg_parser_lfirst(cell);
            enumvalue[i].m_value =
                PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("enumlabel", temp_value->m_new_values,
                                                   temp_value->m_valueCnt, enumvalue[i].m_value);
            enumvalue[i].m_valuelen = strlen(enumvalue[i].m_value);
            i++;
        }
        type_return->m_typptr = (void*)enumvalue;
    }
    else if (PG_SYSDICT_TYPTYPE_RANGE == typtype)
    {
        pg_parser_translog_ddlstmt_typrange* rangedef = NULL;
        char*                                temp_collation = NULL;
        char*                                temp_subtype = NULL;
        char*                                temp_subtype_opclass = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: in create type, type is range type \n");
        type_return->m_typtype = PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_RANGE;
        type_return->m_type_name = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "typname", ddlstate->m_type_item->m_new_values, ddlstate->m_type_item->m_valueCnt,
            type_return->m_type_name);

        /* Build return value */
        type_return->m_typvalcnt = 1;
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

        // todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TYPE_MCXT, (void**)&rangedef,
                                   sizeof(pg_parser_translog_ddlstmt_typrange)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3C;
            return NULL;
        }
        if (!ddlstate->m_type_sub_item)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_TYPE2;
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        temp_subtype = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "rngsubtype", ddlstate->m_type_sub_item->m_new_values,
            ddlstate->m_type_sub_item->m_valueCnt, temp_subtype);
        temp_subtype_opclass = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "rngsubopc", ddlstate->m_type_sub_item->m_new_values,
            ddlstate->m_type_sub_item->m_valueCnt, temp_subtype_opclass);
        temp_collation = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "rngcollation", ddlstate->m_type_sub_item->m_new_values,
            ddlstate->m_type_sub_item->m_valueCnt, temp_collation);
        if (!temp_subtype || !temp_subtype_opclass || !temp_collation)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3D;
            return NULL;
        }
        rangedef->m_collation = strtoul(temp_collation, NULL, 10);
        rangedef->m_subtype = strtoul(temp_subtype, NULL, 10);
        rangedef->m_subtype_opclass = strtoul(temp_subtype_opclass, NULL, 10);

        type_return->m_typptr = (void*)rangedef;
    }
    else if (PG_SYSDICT_TYPTYPE_DOMAIN == typtype)
    {
        uint32_t* domaintyp = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: in create type, type is domain type \n");
        type_return->m_typtype = PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_DOMAIN;
        type_return->m_type_name = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "typname", ddlstate->m_type_item->m_new_values, ddlstate->m_type_item->m_valueCnt,
            type_return->m_type_name);
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        type_return->m_typvalcnt = 1;

        if (!pg_parser_mcxt_malloc(DDL_CREATE_TYPE_MCXT, (void**)&domaintyp, sizeof(uint32_t)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3E;
            return NULL;
        }
        *domaintyp = strtoul(ddlstate->m_type_domain, NULL, 10);
        type_return->m_typptr = (void*)domaintyp;
    }
    else if (PG_SYSDICT_TYPTYPE_NULL == typtype)
    {
        /* Empty type processing */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: in create type, type is NULL type \n");
        type_return->m_typtype = PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_NULL;
        type_return->m_type_name = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
            "typname", ddlstate->m_type_item->m_new_values, ddlstate->m_type_item->m_valueCnt,
            type_return->m_type_name);
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        type_return->m_typvalcnt = 1;
    }
    else
    {
        pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }

    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_CREATE;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_CREATE_TYPE;
    result->m_ddlstmt = (void*)type_return;
    result->m_next = NULL;

    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: create type end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);

    return result;
}
