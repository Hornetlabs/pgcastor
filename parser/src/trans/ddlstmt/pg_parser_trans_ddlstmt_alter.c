#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"


void xk_pg_parser_ddl_updateRecordTrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                               xk_pg_parser_translog_systb2dll_record *current_record,
                                               xk_pg_parser_ddlstate *ddlstate,
                                               int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *record_trans = current_record->m_record;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 捕获更新 pg_class 表中数据的入口。
         */
        char *relkind = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel,
                                "debug: debug flag 1\n");
        relkind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relkind",
                                                               record_trans->m_old_values,
                                                               record_trans->m_valueCnt,
                                                               relkind);
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel,
                                "debug: debug flag 2\n");
        if (XK_PG_SYSDICT_RELKIND_RELATION == relkind[0]
         || XK_PG_SYSDICT_RELKIND_PARTITIONED_TABLE == relkind[0])
        {

            if (xk_pg_parser_ddl_checkChangeColumn("relfilenode",
                                                    record_trans->m_new_values,
                                                    record_trans->m_old_values,
                                                    record_trans->m_valueCnt,
                                                    xk_pg_parser_errno))
            {
                /*
                 * 截断表的解析入口。
                 *     truncate table;
                 */
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture truncate table begin \n");
                if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return;
                }
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_TRUNCATE;
                ddlstate->m_inddl = true;                
            }
            else if (xk_pg_parser_ddl_checkChangeColumn("relname",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno))
            {
                /*
                 * 表重命名的解析入口。
                 *     alter table rename;
                 */
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table rename table begin \n");
                ddlstate->m_relname_new = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relname",
                                                               record_trans->m_new_values,
                                                               record_trans->m_valueCnt,
                                                               ddlstate->m_relname_new);
                if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return;
                }

                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_TABLE_RENAME;
                ddlstate->m_inddl = true;
            }
            else if (xk_pg_parser_ddl_checkChangeColumn("relnamespace",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno))
            {
                /*
                 * 更新 schema 的解析入口。
                 *     alter table set schema;
                 */
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table alter schema begin \n");
                if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
                {
                    *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                    return;
                }
                ddlstate->m_nspoid_new = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relnamespace",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_nspoid_new);
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_NAMESPACE;
                ddlstate->m_inddl = true;
            }
        }
        else if (XK_PG_SYSDICT_RELKIND_INDEX == relkind[0])
        {
            bool change_relfilenode = xk_pg_parser_ddl_checkChangeColumn("relfilenode",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);
            if (change_relfilenode)
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture reindex begin \n");
                /* 如果更新pg_class的kind为i的relfile node, 这是reindex*/
                ddlstate->m_nspname_oid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relnamespace",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_nspname_oid_char);
                ddlstate->m_relname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relname",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_relname);
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_REINDEX;
                ddlstate->m_inddl = true;
            }
        }
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        bool change_attname = xk_pg_parser_ddl_checkChangeColumn("attname",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);
        bool change_atttypid = xk_pg_parser_ddl_checkChangeColumn("atttypid",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);
        bool change_attstattarget = xk_pg_parser_ddl_checkChangeColumn("attstattarget",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);
        bool change_attisdropped = xk_pg_parser_ddl_checkChangeColumn("attisdropped",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);
        bool change_attnotnull = xk_pg_parser_ddl_checkChangeColumn("attnotnull",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);
        bool change_atttypmod = xk_pg_parser_ddl_checkChangeColumn("atttypmod",
                                                         record_trans->m_new_values,
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         xk_pg_parser_errno);

        if (change_attname && change_atttypid && change_attstattarget && change_attisdropped)
        {
            /*
             * 删除列的解析入口。
             *     alter table drop column;
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table drop column begin \n");
            ddlstate->m_att = record_trans;
            ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("attrelid",
                                                               record_trans->m_old_values,
                                                               record_trans->m_valueCnt,
                                                               ddlstate->m_reloid_char);
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_DROP_COLUMN;
            ddlstate->m_inddl = true;
        }
        else if (change_atttypid || change_atttypmod)
        {
            /*
             * 改变列类型的解析入口。
             *    alter table alter column type;
             */
            if (!change_atttypid && change_atttypmod)
            {
                xk_pg_parser_translog_systb2dll_record *next_record = current_record->m_next;
                char * temp_str = NULL;
                char *find_relfilenode = 0;
                bool find_pg_temp = false;
                bool find_pg_temp_change = false;
                while (next_record)
                {
                    xk_pg_parser_translog_tbcol_values *temp_record_trans = next_record->m_record;
                    if (IS_INSERT(temp_record_trans)
                     && xk_pg_parser_check_table_name(temp_record_trans->m_base.m_tbname,
                                                            SYS_CLASS,
                                                            xk_pg_parser_ddl->m_dbtype,
                                                            xk_pg_parser_ddl->m_dbversion))

                    {
                        temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relname",
                                                               temp_record_trans->m_new_values,
                                                               temp_record_trans->m_valueCnt,
                                                               temp_str);
                        if ( temp_str && !strncmp(temp_str, "pg_temp", 7))
                        {
                            find_pg_temp = true;
                            find_relfilenode = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relfilenode",
                                                               temp_record_trans->m_new_values,
                                                               temp_record_trans->m_valueCnt,
                                                               temp_str);
                        }

                    }
                    else if (IS_UPDATE(temp_record_trans))
                    {
                        if (xk_pg_parser_check_table_name(temp_record_trans->m_base.m_tbname,
                                                            SYS_ATTRIBUTE,
                                                            xk_pg_parser_ddl->m_dbtype,
                                                            xk_pg_parser_ddl->m_dbversion))
                        {
                            bool temp_change_atttypmod = xk_pg_parser_ddl_checkChangeColumn("atttypmod",
                                                            temp_record_trans->m_new_values,
                                                            temp_record_trans->m_old_values,
                                                            temp_record_trans->m_valueCnt,
                                                            xk_pg_parser_errno);
                            bool temp_change_atttypid = xk_pg_parser_ddl_checkChangeColumn("atttypid",
                                                            temp_record_trans->m_new_values,
                                                            temp_record_trans->m_old_values,
                                                            temp_record_trans->m_valueCnt,
                                                            xk_pg_parser_errno);
                            if (temp_change_atttypmod || temp_change_atttypid)
                            {
                                find_pg_temp = false;
                                find_pg_temp_change = false;
                                break;
                            }
                        }
                        if (xk_pg_parser_check_table_name(temp_record_trans->m_base.m_tbname,
                                                            SYS_CLASS,
                                                            xk_pg_parser_ddl->m_dbtype,
                                                            xk_pg_parser_ddl->m_dbversion))
                        {
                            char *temp_old_relfilenode = NULL;
                            char *temp_new_relfilenode = NULL;
                            temp_old_relfilenode = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relfilenode",
                                                               temp_record_trans->m_old_values,
                                                               temp_record_trans->m_valueCnt,
                                                               temp_str);
                            temp_new_relfilenode = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relfilenode",
                                                               temp_record_trans->m_new_values,
                                                               temp_record_trans->m_valueCnt,
                                                               temp_str);
                            if ( temp_new_relfilenode && temp_old_relfilenode && find_relfilenode
                             && !strcmp(temp_new_relfilenode, find_relfilenode)
                             &&  strcmp(temp_old_relfilenode, find_relfilenode))
                            {
                                find_pg_temp_change = true;
                                break;
                            }
                        }
                    }
                    next_record = next_record->m_next;
                }
                if (find_pg_temp && find_pg_temp_change)
                {
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                        "DEBUG, DDL PARSER: capture alter table alter column type begin \n");
                    ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE;
                    ddlstate->m_att = record_trans;
                    ddlstate->m_inddl = true;
                }
                else
                {
                    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                    "DEBUG, DDL PARSER: capture alter table alter column type short begin \n");
                    ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE_SHORT;
                    ddlstate->m_att = record_trans;
                    ddlstate->m_inddl = true;
                }
            }
            else
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                    "DEBUG, DDL PARSER: capture alter table alter column type begin \n");
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_TYPE;
                ddlstate->m_att = record_trans;
                ddlstate->m_inddl = true;
            }
        }
        else if (change_attname)
        {
            /*
             * 列重命名的解析入口。
             *     alter table alter column rename;
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table rename column begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_RENAME;
            ddlstate->m_att = record_trans;
            ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("attrelid",
                                                               record_trans->m_new_values,
                                                               record_trans->m_valueCnt,
                                                               ddlstate->m_reloid_char);
            ddlstate->m_inddl = true;
        }
        else if (change_attnotnull)
        {
            /*
             * 设置列非空的解析入口。
             *    alter table alter column set not null;
             */
            char *isnull_new = NULL;
            char *isnull_old = NULL;
            isnull_new = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("attnotnull",
                                                                record_trans->m_new_values,
                                                                record_trans->m_valueCnt,
                                                                isnull_new);
            isnull_old = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("attnotnull",
                                                                      record_trans->m_old_values,
                                                                      record_trans->m_valueCnt,
                                                                      isnull_old);
            if (('t' == isnull_new[0] && 'f' == isnull_old[0])
             || ('T' == isnull_new[0] && 'F' == isnull_old[0]))
             {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table alter column set not null begin \n");
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_NOTNULL;
             }

            else
            {
                xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table alter column set allow null begin \n");
                ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_NULL;
            }
            ddlstate->m_att = record_trans;
            ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("attrelid",
                                                                record_trans->m_new_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_reloid_char);
            ddlstate->m_inddl = true;
        }
    }
}