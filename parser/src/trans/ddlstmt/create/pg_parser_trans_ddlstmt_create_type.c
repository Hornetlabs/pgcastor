#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"

#define XK_DDL_CREATE_TYPE_MCXT NULL

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_create_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno,
                                                char typtype);

/*
 * 创建 type：
 * 1. 入口为向 pg_type 插入一个 tuple；
 * 2. 出口为向 pg_attribute 中插入 relnatts 个属性值。
 * 该过程中包含create table, create view的入口
 */
xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_translog_systb2dll_record *current_record,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    char * temp_str = NULL;
    char *typtype_temp = NULL;
    
    if (!ddlstate->m_type_item)
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_TYPE1;
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }
    typtype_temp = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typtype",
                                                          ddlstate->m_type_item->m_new_values,
                                                          ddlstate->m_type_item->m_valueCnt,
                                                          typtype_temp);
    switch (typtype_temp[0])
    {
        /* 遇到的第一条向pg_type插入的record的类型为c, 组合类型 */
        case XK_PG_SYSDICT_TYPTYPE_COMPOSITE:
        {
            /* 当前record为插入操作 */
            if (IS_INSERT(current_record->m_record))
            {
                /* 当前record为对pg_class进行操作 */
                if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, xk_pg_parser_ddl->m_dbtype , xk_pg_parser_ddl->m_dbversion))
                {
                    char *temp_relkind = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relkind",
                                                                current_record->m_record->m_new_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_relkind);
                    if (!temp_relkind)
                    {
                        //errcode
                        return false;
                    }
                    /* 当前record在pg_class中relkind为r或p, 代表这是一个表记录 */
                    if (XK_PG_SYSDICT_RELKIND_RELATION == temp_relkind[0])
                    {
                        /* 从创建type流程转向创建普通表流程 */
                        char *temp_ispartition = NULL;
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        
                        temp_ispartition = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relispartition",
                                                                current_record->m_record->m_new_values,
                                                                current_record->m_record->m_valueCnt,
                                                                temp_ispartition);
                        /* 这里实际不会有分区表, 判断是否为分区表子表的逻辑在create table的pg_depend中判断 */
                        if ('t' == temp_ispartition[0])
                        {
                            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, stmt type change to create table partition of \n");
                            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB;
                        }
                        else
                        {
                            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, stmt type change to create table \n");
                            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_CREATE;
                        }
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (XK_PG_SYSDICT_RELKIND_PARTITIONED_TABLE == temp_relkind[0])
                    {
                        /* 从创建type流程转向创建分区表流程 */
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, stmt type change to create table partition by \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_CREATE_PARTITION;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    /* 视图和物化视图 */
                    else if (XK_PG_SYSDICT_RELKIND_VIEW == temp_relkind[0] ||
                             XK_PG_SYSDICT_RELKIND_MATVIEW == temp_relkind[0])
                    {
                        /* 从创建type流程转向创建视图流程 */
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, stmt type change to create view \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_VIEW_CREATE;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (XK_PG_SYSDICT_RELKIND_TOASTVALUE == temp_relkind[0])
                    {
                        /* 行外存储处理, 用于过滤 */
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, stmt type change to skip toast \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TOAST_ESCAPE;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (XK_PG_SYSDICT_RELKIND_SEQUENCE == temp_relkind[0])
                    {
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, stmt type change to create sequence \n");
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_SEQUENCE_CREATE;
                        if (!xk_pg_parser_ddl_get_pg_class_info(ddlstate, current_record->m_record, true))
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_CLASSINFO;
                            return NULL;
                        }
                        ddlstate->m_inddl = true;
                    }
                    else if (XK_PG_SYSDICT_RELKIND_COMPOSITE_TYPE == temp_relkind[0])
                    {
                        /* 创建组合类型, 捕获组合类型的列值数量 */
                        temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relnatts",
                                                                    current_record->m_record->m_new_values,
                                                                    current_record->m_record->m_valueCnt,
                                                                    temp_str);
                        ddlstate->m_type_record_natts = strtoul(temp_str, NULL, 10);
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: create type, type is composite, get some info \n");
                    }
                    else
                    {
                        /* 
                         * 在现有的解析逻辑下, 程序不会运行到这里 
                         * 这是一个保险机制, 防止未知的DDL语句造成的解析错误 
                         * 因此在遇到这种情况时, 将现在的record作为第一条record重新处理DDL
                         */
                        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, "WARNING: xk_pg_parser_DDL_create_type JUMP UP DDL\n");
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        xk_pg_parser_ddl_firstTransDDL(xk_pg_parser_ddl, current_record, ddlstate, xk_pg_parser_errno);
                    }
                }
                /* 组合类型的列值处理 */
                else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                {
                    if (!xk_pg_parser_ddl_get_attribute_info(ddlstate, current_record->m_record, true))
                    {
                        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
                        return NULL;
                    }
                    ddlstate->m_type_current_natts++;
                    if (ddlstate->m_type_record_natts == ddlstate->m_type_current_natts)
                        result = xk_pg_parser_ddl_assemble_create_type(xk_pg_parser_ddl,
                                                                       ddlstate,
                                                                       xk_pg_parser_errno,
                                                                       XK_PG_SYSDICT_TYPTYPE_COMPOSITE);
                }
            }
            break;
        }
        /* 第一条record的typtype = e, 代表这是创建枚举类型*/
        case XK_PG_SYSDICT_TYPTYPE_ENUM:
        {
            if (IS_INSERT(current_record->m_record))
            {
                if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ENUM, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))                {
                    ddlstate->m_enumlist = xk_pg_parser_list_lappend(ddlstate->m_enumlist, current_record->m_record);
                }

                else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                {
                    char *temp_typarray = NULL;
                    char *temp_objid = NULL;
                    char *temp_classid = NULL;

                    temp_typarray = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typarray",
                                                          ddlstate->m_type_item->m_new_values,
                                                          ddlstate->m_type_item->m_valueCnt,
                                                          temp_typarray);
                    temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_objid);
                    temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_classid);

                    if (!strcmp(temp_typarray, temp_objid) && !strcmp(TypeRelationIdChar, temp_classid))
                        result = xk_pg_parser_ddl_assemble_create_type(xk_pg_parser_ddl,
                                                                       ddlstate,
                                                                       xk_pg_parser_errno,
                                                                       XK_PG_SYSDICT_TYPTYPE_ENUM);
                }
            }
            break;
        }
        /* range类型 */
        case XK_PG_SYSDICT_TYPTYPE_RANGE:
        {
            /*排除非INSERT语句之外的语句*/
            if (IS_INSERT(current_record->m_record))
            {
                /* 捕获向pg_range表写入的数据 */
                if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_RANGE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                    ddlstate->m_type_sub_item = current_record->m_record;
                /* 
                 * 创建pg_range的过程会自动创建两个函数
                 * 我们要捕获的pg_depend出口在第二个函数之后
                 * 因此在此设置标记
                 */
                else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_PROC, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                    ddlstate->m_type_current_natts++;
                /* 遇到pg_depend语句,判断是否到达了出口 */
                else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                {
                    /* 快速排除 */
                    if (2 == ddlstate->m_type_current_natts)
                    {
                        char *temp_refobjid = NULL;
                        char *temp_classid = NULL;
                        char *temp_refclassid = NULL;
                        temp_refobjid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refobjid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_refobjid);
                        temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_classid);
                        temp_refclassid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refclassid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_refclassid);
                        if (!temp_refobjid || !temp_classid || !temp_refclassid)
                        {
                            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_37;
                            return NULL;
                        }
                        if (!strcmp(ddlstate->m_reloid_char, temp_refobjid)
                            && !strcmp(ProcedureRelationIdChar, temp_classid)
                            && !strcmp(TypeRelationIdChar, temp_refclassid))
                            result = xk_pg_parser_ddl_assemble_create_type(xk_pg_parser_ddl,
                                                                        ddlstate,
                                                                        xk_pg_parser_errno,
                                                                        XK_PG_SYSDICT_TYPTYPE_RANGE);
                    }
                }
            }
            break;
        }
        case XK_PG_SYSDICT_TYPTYPE_PSEUDO:
        {
            /* range类型在最开始都是 p 伪类型，因此我们在这里等待更新操作 */
            if (IS_UPDATE(current_record->m_record))
            {
                if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_TYPE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                {
                    char *oid_old = NULL;
                    char *oid_new = NULL;
                    bool typischange = NULL;
                    oid_old = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                          ddlstate->m_type_item->m_new_values,
                                                          ddlstate->m_type_item->m_valueCnt,
                                                          oid_old);
                    oid_new = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          oid_new);
                    typischange = xk_pg_parser_ddl_checkChangeColumn("typtype",
                                                   current_record->m_record->m_new_values,
                                                   current_record->m_record->m_old_values,
                                                   current_record->m_record->m_valueCnt,
                                                   xk_pg_parser_errno);
                    if (!strcmp(oid_old, oid_new) && typischange)
                    {
                        char *temp_oid = NULL;
                        char *temp_nspoid = NULL;
                        temp_oid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                      current_record->m_record->m_new_values,
                                                      current_record->m_record->m_valueCnt,
                                                      temp_oid);
                        temp_nspoid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typnamespace",
                                                      current_record->m_record->m_new_values,
                                                      current_record->m_record->m_valueCnt,
                                                      temp_nspoid);
                        xk_pg_parser_ddl_init_ddlstate(ddlstate);
                        ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TYPE_CREATE;

#if XK_PG_VERSION_NUM >= 120000
                        ddlstate->m_reloid_char = temp_oid;
                        ddlstate->m_reloid = (uint32_t)strtoul(ddlstate->m_reloid_char, NULL, 10);
#else
                        ddldata->m_reloid =
                            HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif

                        ddlstate->m_nspname_oid_char = temp_nspoid;
                        ddlstate->m_type_domain = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typbasetype",
                                    current_record->m_record->m_new_values,
                                    current_record->m_record->m_valueCnt,
                                    ddlstate->m_type_domain);
                        ddlstate->m_type_item = current_record->m_record;
                        ddlstate->m_inddl = true;
                    }
                }
            }
            /* 如果创建了空类型, 那么我们需要捕获空类型的pg_depend的insert */
            if (IS_INSERT(current_record->m_record))
            {
                char *temp_typisdefined = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typisdefined",
                                                          ddlstate->m_type_item->m_new_values,
                                                          ddlstate->m_type_item->m_valueCnt,
                                                          temp_typisdefined);
                if ('f' == temp_typisdefined[0]
                 && xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                {
                    xk_pg_parser_translog_systb2dll_record *next = current_record->m_next;
                    /* 判断下条语句是否为update pg_type */
                    if (next && next->m_record->m_base.m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE
                     && xk_pg_parser_check_table_name(next->m_record->m_base.m_tbname, SYS_TYPE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                    {
                        char *oid_old = NULL;
                        char *oid_new = NULL;
                        bool typischange = NULL;
                        oid_old = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                            ddlstate->m_type_item->m_new_values,
                                                            ddlstate->m_type_item->m_valueCnt,
                                                            oid_old);
                        oid_new = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                            next->m_record->m_new_values,
                                                            next->m_record->m_valueCnt,
                                                            oid_new);
                        typischange = xk_pg_parser_ddl_checkChangeColumn("typtype",
                                                    next->m_record->m_new_values,
                                                    next->m_record->m_old_values,
                                                    next->m_record->m_valueCnt,
                                                    xk_pg_parser_errno);
                        if (!strcmp(oid_old, oid_new) && typischange)
                        {
                            /* 这是一条create range语句 */
                            break;
                        }
                    }
                    result = xk_pg_parser_ddl_assemble_create_type(xk_pg_parser_ddl,
                                                                   ddlstate,
                                                                   xk_pg_parser_errno,
                                                                   XK_PG_SYSDICT_TYPTYPE_NULL);
                }
            }
            break;
        }
#if 0
        case XK_PG_SYSDICT_TYPTYPE_PSEUDO:
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
#if XK_PG_VERSION_NUM >= 120000
                    ddldata->reloid = fpt->oid;
#else
                    ddldata->reloid =
                        HeapTupleHeaderGetOid((&mchange->data.tp.newtuple->tuple)->t_data);
#endif
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
        /* 创建域类型 */
        case XK_PG_SYSDICT_TYPTYPE_DOMAIN:
        {
            /*排除非INSERT语句之外的语句*/
            if (IS_INSERT(current_record->m_record))
            {
                /* 捕获出口pg_depend */
                if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
                {
                    char *temp_refobjid = NULL;
                    char *temp_classid = NULL;
                    temp_refobjid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refobjid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_refobjid);
                    temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_classid);

                    if (!strcmp(ddlstate->m_reloid_char, temp_refobjid)
                        && !strcmp(TypeRelationIdChar, temp_classid))
                        result = xk_pg_parser_ddl_assemble_create_type(xk_pg_parser_ddl,
                                                                       ddlstate,
                                                                       xk_pg_parser_errno,
                                                                       XK_PG_SYSDICT_TYPTYPE_DOMAIN);
                }
            }
            break;
        }
        default:
        {
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, "WARNING: unsupport CREATE TYPE type\n");
            xk_pg_parser_ddl_init_ddlstate(ddlstate);
            break;
        }
    }
    return result;
}

/* 处理create type的返回值 */
static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_create_type(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno,
                                                char typtype)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;
    xk_pg_parser_translog_ddlstmt_type *type_return = NULL;
    char *temp_str = NULL;

    XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
    XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

    //todo free
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_TYPE_MCXT,
                                 (void **)&result,
                                  sizeof(xk_pg_parser_translog_ddlstmt)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_38;
        return NULL;
    }
    //todo free
    if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_TYPE_MCXT,
                                 (void **)&type_return,
                                  sizeof(xk_pg_parser_translog_ddlstmt_type)))
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_39;
        return NULL;
    }

    temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typowner",
                                            ddlstate->m_type_item->m_new_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            temp_str);
    type_return->m_owner = (uint32_t) strtoul(temp_str, NULL, 10);

    if (XK_PG_SYSDICT_TYPTYPE_COMPOSITE == typtype)
    {
        xk_pg_parser_translog_ddlstmt_typcol *typcol = NULL;
        int32_t i = 0;
        xk_pg_parser_ListCell *cell = NULL;
        xk_pg_parser_translog_tbcol_values *typatt = NULL;

        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, type is composite type \n");
        type_return->m_typtype = XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_COMPOSITE;
        type_return->m_type_name = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_new_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_type_name);

        /* 将存放于dlstate->m_ddlList中的子类型信息附加到返回值中 */

        type_return->m_typvalcnt = ddlstate->m_type_record_natts;
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        //todo free
        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_TYPE_MCXT,
                                     (void **) &typcol,
                                      sizeof(xk_pg_parser_translog_ddlstmt_typcol)
                                             * type_return->m_typvalcnt))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3A;
            return NULL;
        }
        i = 0;
        xk_pg_parser_foreach(cell, ddlstate->m_attList)
        {
            typatt = (xk_pg_parser_translog_tbcol_values *)xk_pg_parser_lfirst(cell);
            typcol[i].m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                typatt->m_new_values,
                                                typatt->m_valueCnt,
                                                typcol[i].m_colname);
            temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                typatt->m_new_values,
                                                typatt->m_valueCnt,
                                                temp_str);
            typcol[i].m_coltypid = strtoul(temp_str, NULL, 10);
            i++;
        }
        type_return->m_typptr = (void *)typcol;
    }
    else if (XK_PG_SYSDICT_TYPTYPE_ENUM == typtype)
    {
        xk_pg_parser_translog_ddlstmt_valuebase *enumvalue = NULL;
        int32_t i = 0;
        xk_pg_parser_ListCell *cell = NULL;
        xk_pg_parser_translog_tbcol_values *temp_value = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, type is enum type \n");
        type_return->m_typtype = XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_ENUM;
        type_return->m_type_name = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_new_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_type_name);

        /* 将存放于dlstate->m_enumlist中的枚举值名信息附加到返回值中 */

        type_return->m_typvalcnt = ddlstate->m_enumlist->length;
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

        //todo free
        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_TYPE_MCXT,
                                     (void **) &enumvalue,
                                      sizeof(xk_pg_parser_translog_ddlstmt_valuebase)
                                             * type_return->m_typvalcnt))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3B;
            return NULL;
        }
        i = 0;
        xk_pg_parser_foreach(cell, ddlstate->m_enumlist)
        {
            temp_value = (xk_pg_parser_translog_tbcol_values *)xk_pg_parser_lfirst(cell);
            enumvalue[i].m_value = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("enumlabel",
                                                                temp_value->m_new_values,
                                                                temp_value->m_valueCnt,
                                                                enumvalue[i].m_value);
            enumvalue[i].m_valuelen = strlen(enumvalue[i].m_value);
            i++;
        }
        type_return->m_typptr = (void *)enumvalue;
    }
    else if (XK_PG_SYSDICT_TYPTYPE_RANGE == typtype)
    {
        xk_pg_parser_translog_ddlstmt_typrange *rangedef = NULL;
        char *temp_collation = NULL;
        char *temp_subtype = NULL;
        char *temp_subtype_opclass = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, type is range type \n");
        type_return->m_typtype = XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_RANGE;
        type_return->m_type_name = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_new_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_type_name);

        /* 构建返回值 */
        type_return->m_typvalcnt = 1;
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

        //todo free
        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_TYPE_MCXT,
                                     (void **) &rangedef,
                                      sizeof(xk_pg_parser_translog_ddlstmt_typrange)))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3C;
            return NULL;
        }
        if (!ddlstate->m_type_sub_item)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_TYPE2;
            xk_pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        temp_subtype = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("rngsubtype",
                                            ddlstate->m_type_sub_item->m_new_values,
                                            ddlstate->m_type_sub_item->m_valueCnt,
                                            temp_subtype);
        temp_subtype_opclass = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("rngsubopc",
                                            ddlstate->m_type_sub_item->m_new_values,
                                            ddlstate->m_type_sub_item->m_valueCnt,
                                            temp_subtype_opclass);
        temp_collation = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("rngcollation",
                                            ddlstate->m_type_sub_item->m_new_values,
                                            ddlstate->m_type_sub_item->m_valueCnt,
                                            temp_collation);
        if (!temp_subtype || !temp_subtype_opclass || !temp_collation)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3D;
            return NULL;
        }
        rangedef->m_collation = strtoul(temp_collation, NULL, 10);
        rangedef->m_subtype = strtoul(temp_subtype, NULL, 10);
        rangedef->m_subtype_opclass = strtoul(temp_subtype_opclass, NULL, 10);

        type_return->m_typptr = (void *)rangedef;
    }
    else if (XK_PG_SYSDICT_TYPTYPE_DOMAIN == typtype)
    {
        uint32_t *domaintyp = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, type is domain type \n");
        type_return->m_typtype = XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_DOMAIN;
        type_return->m_type_name = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_new_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_type_name);
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        type_return->m_typvalcnt = 1;

        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_TYPE_MCXT,
                                     (void **) &domaintyp,
                                      sizeof(uint32_t)))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_3E;
            return NULL;
        }
        *domaintyp = strtoul(ddlstate->m_type_domain, NULL, 10);
        type_return->m_typptr = (void*)domaintyp;
    }
    else if (XK_PG_SYSDICT_TYPTYPE_NULL == typtype)
    {
        /* 空类型处理 */
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: in create type, type is NULL type \n");
        type_return->m_typtype = XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_NULL;
        type_return->m_type_name = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("typname",
                                            ddlstate->m_type_item->m_new_values,
                                            ddlstate->m_type_item->m_valueCnt,
                                            type_return->m_type_name);
        type_return->m_typnspid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        type_return->m_typvalcnt = 1;
    }
    else
    {
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return NULL;
    }

    result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_CREATE;
    result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_CREATE_TYPE;
    result->m_ddlstmt = (void*) type_return;
    result->m_next = NULL;

    xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                            "DEBUG, DDL PARSER: create type end \n");
    xk_pg_parser_ddl_init_ddlstate(ddlstate);

    return result;
}

