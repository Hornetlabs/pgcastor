#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"

void xk_pg_parser_ddl_insertRecordTrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                               xk_pg_parser_translog_systb2dll_record *current_record,
                                               xk_pg_parser_ddlstate *ddlstate,
                                               int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_tbcol_values *record_trans = current_record->m_record;
    /* TODO 捕获所有的 DDL 语句信息。
     * 在现有模式下，如若遭遇 DDL 解析异常，那么一定是因为 current_record->m_tbtype 判断语句的
     * else if 分支中的调用造成的，
     * 主要的原因在于没能完全覆盖所有的 DDL 类型。
     */

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 每当我们在数据库中执行一条 CREATE 操作时，都会向 pg_class 表中插入一条数据，
         * 而这就是解析所有这些 DDL 语句的起点，
         * 其中主要的 RELKIND 类型如下（具体内容详见 src/backend/catalog/pg_class_d.h）：
         *        RELKIND_RELATION    'r'        ordinary table
         *        RELKIND_INDEX       'i'        secondary index
         *        RELKIND_SEQUENCE    'S'        sequence object
         *        RELKIND_VIEW        'v'        view
         *        RELKIND_MATVIEW     'm'        materialized view
         */
        char *relkind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("relkind",
                                                               record_trans->m_new_values,
                                                               record_trans->m_valueCnt,
                                                               relkind);
        
        if (XK_PG_SYSDICT_RELKIND_TOASTVALUE == relkind[0])
        {
            /*
             * 如果命令空间为 PG_TOAST_NAMESPACE，则表明接下来出现的 DDL 语句为：
             *    1. 创建名为 pg_toast.pg_toast_oid 的表；
             *    2. 创建名为 pg_toast.pg_toast_oid_index 的索引；
             * 这两者是不需要获取并同步的。
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture toast, ignore begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TOAST_ESCAPE;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        //todo rewrite(这里永远不会走到)
        else if (XK_PG_SYSDICT_RELKIND_RELATION == relkind[0])
        {
            /*
             * 若 DDL 语句为 CREATE TABLE，则对应的 pg_class 类型为 'r'
             */
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_CREATE;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        /* todo rewrite 在修改捕获create type后, 这里永远不会走到 */
        else if (XK_PG_SYSDICT_RELKIND_SEQUENCE == relkind[0])
        {
            /*
             * 若创建表时，指定的类型为 serial 时，那么 PG 在 WAL 日志中存储时会分成以下两步：
             * 1、首先创建一个 sequence，命名规则为 tableName_columnName_seq；
             * 2、创建表，并将相应的 columnName 的值设为 nextval('tableName_columnName_seq')；
             * 是故，在解析带有 serial 类型的 DDL 语句时，需要将这条语句拆分成两条。
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create sequence begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_SEQUENCE_CREATE;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        else if (XK_PG_SYSDICT_RELKIND_INDEX == relkind[0]
              || XK_PG_SYSDICT_RELKIND_PARTITIONED_INDEX == relkind[0])
        {
            /*
             * 若创建表时，指定某字段为主键，那么 PG 在 WAL 日志中存储时会分成以下两步：
             * 1、首先创建表；
             * 2、创建索引，命名规则为 tableName_pkey；
             * 是故，在解析带有 PRIMARY KEY (column_name) 的 DDL 语句时，需要拆分成两条。
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create index begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_INDEX_CREATE;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        else if (XK_PG_SYSDICT_RELKIND_VIEW == relkind[0])
        {
            /*
             * 若创建视图时，PG 在 WAL 日志中存储时会分成以下两步：
             * 1、往 pg_class 中插入视图信息；
             * 2、记录视图所引用的所有字段。
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create view begin \n");
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_VIEW_CREATE;
            if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, record_trans, true))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                return;
            }
            ddlstate->m_inddl = true;
        }
        else
        {
            /*
             * 没能捕获入口。
             */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: no ddl stmt capture \n");
            ddlstate->m_inddl = false;
        }
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 向表中添加字段, 一次只能添加一列, 因此该record的下一条语句一定是一个update class操作
         * text/varchar在WAL日志中会额外的插入两条记录，分别为：
         *     1、创建 pg_toast.pg_toast_OID_index 索引；
         *     2、创建 pg_toast.pg_toast_OID 表；
         * 这两条记录是不需要解析的, 跳过。
         */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table add column begin \n");
        if (!xk_pg_parser_ddl_get_attribute_info(ddlstate, record_trans, true))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
            return;
        }

        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_ADD_COLUMN;
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_ATTRDEF, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 在创建带有默认值的表时，PG 在 WAL 日志中的记录顺序如下：
         *     1、插入 pg_attrdef；
         *     2、更新 pg_attribute 中的对应字段；
         *     3、向 pg_depend 表中插入一条数据；
         *     (objid = pg_attrdef.oid, classid = AttrDefaultRelationId)
         */
        uint32_t m_reloid;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table add default begin \n");
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("adrelid",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_reloid_char);
        m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_COLUMN_DEFAULT;
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid = m_reloid;
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
        ddldata->m_att_def.att_id =
            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif
        ddlstate->m_att_def = record_trans;
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_NAMESPACE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 向 pg_namespace 表中插入一条数据，视为create schema (create namespace)。
         * WAL 日志中的记录顺序如下：
         *     1、插入 pg_namespace；
         *  2、提交 commit；（没有插入 pg_depend 的操作。）
         */
        char *temp_str = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create schema begin \n");
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_NAMESPACE_CREATE;
        ddlstate->m_nspname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("nspname",
                                                                   record_trans->m_new_values,
                                                                   record_trans->m_valueCnt,
                                                                   ddlstate->m_nspname);
        temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("nspowner",
                                                                   record_trans->m_new_values,
                                                                   record_trans->m_valueCnt,
                                                                   temp_str);
        ddlstate->m_owner = (uint32_t)strtoul(temp_str, NULL, 10);
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_CONSTRAINT, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 向 pg_constraint 表中插入一条数据，视为创建新的外键。
         */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture alter table add foreign key begin \n");
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("oid",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT_FOREIGN;
        ddlstate->m_constraint = record_trans;
        ddlstate->m_inddl = true;
    }
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_DATABASE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))    {
        /*
         * 向 pg_database 表中插入一条数据，视为创建新的数据库，无需解析，跳过
         */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create database begin \n");
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_DATABASE_CREATE;
        ddlstate->m_inddl = true;
    }
#if 0
    else if (!strcmp(XK_PG_SYSDICT_PG_PROC_NAME, ddl_char_tolower(record_trans->m_base.m_tbname)))
    {

        /*
         * 向 pg_proc 表中插入一条数据，视为创建一个函数或者存储过程。
         */
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                         record_trans->m_new_values,
                                                         record_trans->m_valueCnt,
                                                         ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_FUNCTION_CREATE;
        //todo create function
        ddlstate->m_inddl = true;
    }
#endif
//todo trigger
#if 0
    else if (!strcmp(XK_PG_SYSDICT_PG_TRIGGER_NAME, ddl_char_tolower(record_trans->m_base.m_tbname)))
    {
        /*
         * 向 pg_trigger 表中插入一条数据，视为创建一个触发器。
         */
#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                                record_trans->m_new_values,
                                                                record_trans->m_valueCnt,
                                                                ddlstate->m_reloid_char);
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TRIGGER_CREATE;

        ddlstate->m_inddl = true;
    }
#endif
    else if (xk_pg_parser_check_table_name(record_trans->m_base.m_tbname, SYS_TYPE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
    {
        /*
         * 向 pg_type 表中插入一条数据，视为创建一个类型 create type
         * 我们这里需要获取namespace名称和oid
         */
        char *temp_oid = NULL;
        char *temp_nspoid = NULL;
        temp_oid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("oid",
                                                          record_trans->m_new_values,
                                                          record_trans->m_valueCnt,
                                                          temp_oid);
        temp_nspoid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("typnamespace",
                                                                record_trans->m_new_values,
                                                                record_trans->m_valueCnt,
                                                                temp_nspoid);
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture create type begin \n");
        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TYPE_CREATE;

#if XK_PG_VERSION_NUM >= 120000
        ddlstate->m_reloid_char = temp_oid;
        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif

        ddlstate->m_nspname_oid_char = temp_nspoid;

        ddlstate->m_type_domain = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME_NO_RETURN("typbasetype",
                                                            record_trans->m_new_values,
                                                            record_trans->m_valueCnt,
                                                            ddlstate->m_type_domain);
        ddlstate->m_type_item = record_trans;
        ddlstate->m_inddl = true;
    }
}
