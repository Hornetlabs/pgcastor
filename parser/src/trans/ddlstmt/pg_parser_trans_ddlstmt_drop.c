#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"

void pg_parser_ddl_deleteRecordTrans(pg_parser_translog_systb2ddl*        pg_parser_ddl,
                                     pg_parser_translog_systb2dll_record* current_record,
                                     pg_parser_ddlstate*                  ddlstate,
                                     int32_t*                             pg_parser_errno)
{
    pg_parser_translog_tbcol_values* record_trans = current_record->m_record;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);

    if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                   SYS_CLASS,
                                   pg_parser_ddl->m_dbtype,
                                   pg_parser_ddl->m_dbversion))
    {
        char* relkind = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relkind",
                                                                     record_trans->m_old_values,
                                                                     record_trans->m_valueCnt,
                                                                     relkind);
        /*
         * Whenever we execute a DROP statement, a tuple is deleted from pg_class table,
         * and this is the starting point for parsing DROP statement.
         */
        if (PG_SYSDICT_RELKIND_RELATION == relkind[0] || PG_SYSDICT_RELKIND_PARTITIONED_TABLE == relkind[0])
        {
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: capture drop table begin \n");
            ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_DROP;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }

            ddlstate->m_inddl = true;
        }
        else if (PG_SYSDICT_RELKIND_SEQUENCE == relkind[0])
        {
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: capture drop sequence begin \n");
            ddlstate->m_ddlKind = PG_PARSER_DDL_SEQUENCE_DROP;
            if (!pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }

            ddlstate->m_inddl = true;
        }
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_INDEX,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Delete index.
         *    1. alter table drop constraint;(primary key, unique key)
         *     2. drop index;
         */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: capture drop index begin \n");
        ddlstate->m_ddlKind = PG_PARSER_DDL_INDEX_DROP;
        ddlstate->m_index = record_trans;

        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_NAMESPACE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Delete namespace.
         *     1. drop schema(drop namespace);
         */

        ddlstate->m_ddlKind = PG_PARSER_DDL_NAMESPACE_DROP;
        ddlstate->m_nspname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("nspname",
                                                                           record_trans->m_old_values,
                                                                           record_trans->m_valueCnt,
                                                                           ddlstate->m_nspname);
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_ATTRDEF,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Delete field default value.
         *     1. alter table alter column drop default;
         */
        uint32_t m_reloid;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture alter table drop default begin \n");
        ddlstate->m_reloid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("adrelid",
                                                                               record_trans->m_old_values,
                                                                               record_trans->m_valueCnt,
                                                                               ddlstate->m_reloid_char);
        m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_ALTER_COLUMN_DROP_DEFAULT;
        ddlstate->m_reloid = m_reloid;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_DATABASE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Delete database.
         *    drop database;
         */
        ddlstate->m_ddlKind = PG_PARSER_DDL_DATABASE_DROP;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_TYPE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * Start deleting type
         *    drop type trigger_name;
         */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: capture drop type begin \n");
        ddlstate->m_reloid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("oid",
                                                                               record_trans->m_old_values,
                                                                               record_trans->m_valueCnt,
                                                                               ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
        ddlstate->m_nspname_oid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("typnamespace",
                                                                                    record_trans->m_old_values,
                                                                                    record_trans->m_valueCnt,
                                                                                    ddlstate->m_nspname_oid_char);
        ddlstate->m_ddlKind = PG_PARSER_DDL_TYPE_DROP;
        ddlstate->m_type_item = record_trans;
        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_REWRITE,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * drop view
         * Entry is deleting a record from pg_rewrite
         * Exit is deleting a record from pg_class
         */
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: capture drop view begin \n");
        ddlstate->m_ddlKind = PG_PARSER_DDL_VIEW_DROP;
        ddlstate->m_reloid_char = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("ev_class",
                                                                               record_trans->m_old_values,
                                                                               record_trans->m_valueCnt,
                                                                               ddlstate->m_reloid_char);

        ddlstate->m_inddl = true;
    }
    else if (pg_parser_check_table_name(record_trans->m_base.m_tbname,
                                        SYS_CONSTRAINT,
                                        pg_parser_ddl->m_dbtype,
                                        pg_parser_ddl->m_dbversion))
    {
        /*
         * alter table drop constraint (foreign key constraint)
         */
        char* temp_reloid = NULL;
        char* temp_str = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                             "DEBUG, DDL PARSER: capture alter table drop constraint begin \n");
        ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT;
        ddlstate->m_relname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("conname",
                                                                           current_record->m_record->m_old_values,
                                                                           current_record->m_record->m_valueCnt,
                                                                           ddlstate->m_relname);
        temp_reloid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("conrelid",
                                                                   current_record->m_record->m_old_values,
                                                                   current_record->m_record->m_valueCnt,
                                                                   temp_reloid);
        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("conislocal",
                                                                current_record->m_record->m_old_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_str);
        ddlstate->m_cons_is_local = temp_str[0] == 't' ? true : false;
        ddlstate->m_reloid = strtoul(temp_reloid, NULL, 10);
        ddlstate->m_nspname_oid_char =
            PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("connamespace",
                                                         current_record->m_record->m_old_values,
                                                         current_record->m_record->m_valueCnt,
                                                         ddlstate->m_nspname_oid_char);

        ddlstate->m_inddl = true;
    }
}