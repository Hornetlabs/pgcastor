#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"

void pg_parser_ddl_insertRecordTrans(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                     pg_parser_translog_systb2dll_record* current_record,
                                     pg_parser_ddlstate*                  ddlstate,
                                     int32_t*                             pg_parser_errno)
{
    pg_parser_translog_tbcol_values* record_trans = current_record->m_record;
    /* TODO Capture all DDL statement information.
     * In current mode, if DDL parsing exception is encountered, it must be because of
     * current_record->m_tbtypejudgment statement else if branch call, The main reason is that not
     * all DDL types are fully covered.
     */

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                   SYS_CLASS,
                                   pg_parser_ddl->m_dbtype,
                                   pg_parser_ddl->m_dbversion))
    {
        /*
         * Whenever we execute a CREATE operation in the database, a record is inserted into
         * pg_class table, and this is the starting point for parsing all these DDL statements, The
         * main RELKIND types are as follows (see src/backend/catalog/pg_class_d.h for details):
         *        RELKIND_RELATION    'r'        ordinary table
         *        RELKIND_INDEX       'i'        secondary index
         *        RELKIND_SEQUENCE    'S'        sequence object
         *        RELKIND_VIEW        'v'        view
         *        RELKIND_MATVIEW     'm'        materialized view
         */
        char* relkind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(
            "relkind", record_trans->m_new_values, record_trans->m_valueCnt, relkind);

        if (PG_SYSDICT_RELKIND_TOASTVALUE == relkind[0])
        {
            /*
             * If namespace is PG_TOAST_NAMESPACE, it indicates the following DDL statements are:
             *    1. Create table named pg_toast.pg_toast_oid;
             *    2. Create index named pg_toast.pg_toast_oid_index;
             * These two do not need to be obtained and synchronized.
             */
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                 "DEBUG, DDL PARSER: capture toast, ignore begin \n");
            ddlstate->m_ddlKind = PG_PARSER_DDL_TOAST_ESCAPE;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        /* todo rewrite (this will never be reached) */
        else if (PG_SYSDICT_RELKIND_RELATION == relkind[0])
        {
            /*
             * If DDL statement is CREATE TABLE, then corresponding pg_class type is 'r'
             */
            ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_CREATE;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        /* todo rewrite After modifying capture create type, this will never be reached */
        else if (PG_SYSDICT_RELKIND_SEQUENCE == relkind[0])
        {
            /*
             * If the specified type is serial when creating table, then PG will store in WAL log in
             * the following two steps:
             * 1. First create a sequence with naming rule tableName_columnName_seq;
             * 2. Create table and set corresponding columnName value to
             * nextval('tableName_columnName_seq')； Therefore, when parsing DDL statements with
             * serial type, this statement needs to be split into two.
             */
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                 "DEBUG, DDL PARSER: capture create sequence begin \n");
            ddlstate->m_ddlKind = PG_PARSER_DDL_SEQUENCE_CREATE;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        else if (PG_SYSDICT_RELKIND_INDEX == relkind[0] ||
                 PG_SYSDICT_RELKIND_PARTITIONED_INDEX == relkind[0])
        {
            /*
             * If a field is specified as primary key when creating table, then PG will store in WAL
             * log in the following two steps:
             * 1. First create table;
             * 2. Create index with naming rule tableName_pkey;
             * Therefore, when parsing DDL statements with PRIMARY KEY (column_name), it needs to be
             * split into two.
             */
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                 "DEBUG, DDL PARSER: capture create index begin \n");
            ddlstate->m_ddlKind = PG_PARSER_DDL_INDEX_CREATE;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        else if (PG_SYSDICT_RELKIND_VIEW == relkind[0])
        {
            /*
             * When creating view, PG will store in WAL log in the following two steps:
             * 1. Insert view information into pg_class;
             * 2. Record all fields referenced by the view.
             */
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                 "DEBUG, DDL PARSER: capture create view begin \n");
            ddlstate->m_ddlKind = PG_PARSER_DDL_VIEW_CREATE;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        else
        {
            /*
             * Failed to capture entry.
             */
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                                 "DEBUG, DDL PARSER: no ddl stmt capture \n");
            ddlstate->m_inddl = false;
        }
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_ATTRIBUTE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Add field to table, only one column can be added at a time, so the next statement of this
         * record must be an update class operation text/varchar will insert two additional records
         * in WAL log, respectively:
         *     1. Create pg_toast.pg_toast_OID_index index;
         *     2. Create pg_toast.pg_toast_OID table;
         * These two records do not need parsing, skip.
         */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture alter table add column begin \n");
        if (!pg_parser_ddl_get_attribute_info(ddlstate, record_trans, true))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
            return;
        }

        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_ALTER_ADD_COLUMN;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_ATTRDEF,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * When creating table with default values, PG records in WAL log in the following order:
         *     1. Insert pg_attrdef;
         *     2. Update corresponding field in pg_attribute;
         *     3. Insert a record into pg_depend table;
         *     (objid = pg_attrdef.oid, classid = AttrDefaultRelationId)
         */
        uint32_t m_reloid;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture alter table add default begin \n");
        ddlstate->m_reloid_char =
            PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("adrelid",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_reloid_char);
        m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_ALTER_COLUMN_DEFAULT;
        ddlstate->m_reloid = m_reloid;
        ddlstate->m_att_def = record_trans;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_NAMESPACE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Insert a record into pg_namespace table, regarded as create schema (create namespace).
         * The order of records in WAL log is as follows:
         *     1. Insert pg_namespace;
         *  2. Commit; (No operation to insert pg_depend.)
         */
        char* temp_str = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture create schema begin \n");
        ddlstate->m_ddlKind = PG_PARSER_DDL_NAMESPACE_CREATE;
        ddlstate->m_nspname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(
            "nspname", record_trans->m_new_values, record_trans->m_valueCnt, ddlstate->m_nspname);
        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(
            "nspowner", record_trans->m_new_values, record_trans->m_valueCnt, temp_str);
        ddlstate->m_owner = (uint32_t)strtoul(temp_str, NULL, 10);
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_CONSTRAINT,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Insert a record into pg_constraint table, regarded as creating a new foreign key.
         */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture alter table add foreign key begin \n");
        ddlstate->m_reloid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(
            "oid", record_trans->m_new_values, record_trans->m_valueCnt, ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT_FOREIGN;
        ddlstate->m_constraint = record_trans;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_DATABASE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Insert a record into pg_database table, regarded as creating a new database, no parsing
         * needed, skip
         */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture create database begin \n");
        ddlstate->m_ddlKind = PG_PARSER_DDL_DATABASE_CREATE;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_TYPE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Insert a record into pg_type table, regarded as creating a type create type
         * We need to get namespace name and oid here
         */
        char* temp_oid = NULL;
        char* temp_nspoid = NULL;
        temp_oid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(
            "oid", record_trans->m_new_values, record_trans->m_valueCnt, temp_oid);
        temp_nspoid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN(
            "typnamespace", record_trans->m_new_values, record_trans->m_valueCnt, temp_nspoid);
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture create type begin \n");
        ddlstate->m_ddlKind = PG_PARSER_DDL_TYPE_CREATE;

        ddlstate->m_reloid_char = temp_oid;
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);

        ddlstate->m_nspname_oid_char = temp_nspoid;

        ddlstate->m_type_domain =
            PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("typbasetype",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_type_domain);
        ddlstate->m_type_item = record_trans;
        ddlstate->m_inddl = true;
    }
}
