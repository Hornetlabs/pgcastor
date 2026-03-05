#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"

void xk_pg_parser_ddl_deleteRecordTrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                               xk_pg_parser_translog_systb2dll_record *current_record,
                                               xk_pg_parser_ddlstate *ddlstate,
                                               int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *record_trans = current_record->m_record;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        char *relkind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relkind",
                                                               record_trans->m_old_values,
                                                               record_trans->m_valueCnt,
                                                               relkind);
        /*
         * 每当我们执行 DROP 语句时，都会删除 pg_class 表中的一条 tuple，
         * 而这就是解析 DROP 语句的起点。
         */
        if (XK_PG_SYSDICT_RELKIND_RELATION == relkind[0]
         || XK_PG_SYSDICT_RELKIND_PARTITIONED_TABLE == relkind[0])
        {
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop table begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_DROP;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }

            ddlstate->m_inddl = true;
        }
        else if (XK_PG_SYSDICT_RELKIND_SEQUENCE == relkind[0])
        {
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop sequence begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_SEQUENCE_DROP;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, false))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }

            ddlstate->m_inddl = true;
        }
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_INDEX, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 删除索引。
         *    1. alter table drop constraint;(primary key, unique key)
         *     2. drop index;
         */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop index begin \n");
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_INDEX_DROP;
        ddlstate->m_index = record_trans;

        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_NAMESPACE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 删除命名空间。
         *     1. drop schema(drop namespace);
         */

        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_NAMESPACE_DROP;
        ddlstate->m_nspname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("nspname",
                                                                   record_trans->m_old_values,
                                                                   record_trans->m_valueCnt,
                                                                   ddlstate->m_nspname);
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_ATTRDEF, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 删除字段默认值。
         *     1. alter table alter column drop default;
         */
        uint32_t m_reloid;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table drop default begin \n");
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("adrelid",
                                                         record_trans->m_old_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_reloid_char);
        m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_DROP_DEFAULT;
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid = m_reloid;
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.oldtuple->tuple)->t_data);
        ddldata->m_att_def.att_id =
            HeapTupleHeaderGetOid((&mchange->data.tp.oldtuple->tuple)->t_data);
#endif
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_DATABASE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))    {
        /*
         * 删除数据库。
         *    drop database;
         */
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_DATABASE_DROP;
        ddlstate->m_inddl = true;
    }
#if 0
    else if (!strcmp(XK_PG_SYSDICT_PG_PROC_NAME, ddl_char_tolower(record_trans->m_base.m_tbname)))
    {
        /*
         * 删除函数。
         *    drop function func_name (type, ...);
         */
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                                record_trans->m_old_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.oldtuple->tuple)->t_data);
#endif
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_FUNCTION_DROP;

        ddlstate->m_inddl = true;
    }
#endif
//todo trigger
#if 0
    else if (!strcmp(XK_PG_SYSDICT_PG_TRIGGER_NAME, ddl_char_tolower(record_trans->m_base.m_tbname)))
    {
        /*
         * 删除触发器。
         *    drop trigger trigger_name on table_name;
         */
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                                record_trans->m_old_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.oldtuple->tuple)->t_data);
#endif
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TRIGGER_DROP;
        ddlstate->m_inddl = true;
    }
#endif
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_TYPE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 删除类型开始
         *    drop type trigger_name;
         */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop type begin \n");
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("oid",
                                                                record_trans->m_old_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.oldtuple->tuple)->t_data);
#endif
        ddlstate->m_nspname_oid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("typnamespace",
                                                                record_trans->m_old_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_nspname_oid_char);
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TYPE_DROP;
        ddlstate->m_type_item = record_trans;
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_REWRITE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /* 
         * drop view
         * 入口为删除pg_rewrite的一条数据
         * 出口为删除pg_class的记录
         */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop view begin \n");
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_VIEW_DROP;
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("ev_class",
                                                                record_trans->m_old_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_reloid_char);

        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_CONSTRAINT, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /* 
         * alter table drop constraint(foreign key 外键约束)
         */
        char* temp_reloid = NULL;
        char *temp_str = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table drop constraint begin \n");
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_DROP_CONSTRAINT;
        ddlstate->m_relname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("conname",
                                                                current_record->m_record->m_old_values,
                                                                current_record->m_record->m_valueCnt,
                                                                ddlstate->m_relname);
        temp_reloid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("conrelid",
                                                             current_record->m_record->m_old_values,
                                                             current_record->m_record->m_valueCnt,
                                                             temp_reloid);
        temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("conislocal",
                                                              current_record->m_record->m_old_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_str);
        ddlstate->m_cons_is_local = temp_str[0] == 't' ? true : false;
        ddlstate->m_reloid = strtoul(temp_reloid, NULL, 10);
        ddlstate->m_nspname_oid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("connamespace",
                                                                current_record->m_record->m_old_values,
                                                                current_record->m_record->m_valueCnt,
                                                                ddlstate->m_nspname_oid_char);

        ddlstate->m_inddl = true;
    }
}