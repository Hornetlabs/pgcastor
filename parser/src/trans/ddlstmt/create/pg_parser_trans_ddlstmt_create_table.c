#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode.h"

#define DDL_CREATE_TABLE_MCXT NULL

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_create_table(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_create_table_partition(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_create_table_partition_sub(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno);

pg_parser_translog_ddlstmt* pg_parser_DDL_create_table(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                            pg_parser_translog_systb2dll_record *current_record,
                                                            pg_parser_ddlstate *ddlstate,
                                                            int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    PG_PARSER_UNUSED(pg_parser_ddl);
    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: create table, get attribute info \n");
            if (!pg_parser_ddl_get_attribute_info(ddlstate, current_record->m_record, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
                return NULL;
            }
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            char *temp_refclassid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_classid);
            temp_refclassid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refclassid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_refclassid);
            if (!strcmp(temp_objid, ddlstate->m_reloid_char)
                 && !strcmp(RelationRelationIdChar, temp_classid)
                 && !strcmp(NamespaceRelationIdChar, temp_refclassid))
            {
                pg_parser_translog_systb2dll_record *next_record = current_record->m_next;
                int i = 0;
                int deep = 0;
                /* 
                 * 首先检查这是否是一个alter table set logged/unlogged语句 这里需要遍历, 考虑到可能会有行外存储表的建立
                 * 因此允许跨越一个建表DDL, 及允许一个向pg_type表插入数据的语句
                 */
                while (next_record)
                {
                    if (IS_UPDATE(next_record->m_record)
                    && (pg_parser_check_table_name(next_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion)))
                    {
                        bool temp_check_relpersistence_change = false;
                        bool temp_check_relfilenode_change = false;

                        temp_check_relpersistence_change = pg_parser_ddl_checkChangeColumn("relpersistence",
                                                        next_record->m_record->m_new_values,
                                                        next_record->m_record->m_old_values,
                                                        next_record->m_record->m_valueCnt,
                                                        pg_parser_errno);
                        temp_check_relfilenode_change = pg_parser_ddl_checkChangeColumn("relfilenode",
                                                        next_record->m_record->m_new_values,
                                                        next_record->m_record->m_old_values,
                                                        next_record->m_record->m_valueCnt,
                                                        pg_parser_errno);

                        if ((temp_check_relpersistence_change)
                         && (ddlstate->m_log_step == PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_BEGIN)
                         && (pg_parser_check_table_name(ddlstate->m_relname, TEMPTABLE_PREFIX, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion)))
                        {
                            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                                    "DEBUG, DDL PARSER: in create table catch change relpersistence \n");
                            ddlstate->m_log_step = PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_RELPERSISTENCE_STEP;
                            ddlstate->m_reloid_temp = ddlstate->m_reloid;
                        }
                        else if (temp_check_relfilenode_change)
                        {
                            char *temp_str = NULL;
                            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                                        "DEBUG, DDL PARSER: in create table catch change relfilenode \n");
                            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                                    next_record->m_record->m_new_values,
                                                                    next_record->m_record->m_valueCnt,
                                                                    ddlstate->m_reloid);
                            if (PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_RELPERSISTENCE_STEP == ddlstate->m_log_step )
                            {
                                if (ddlstate->m_reloid_temp == strtoul(temp_str, NULL, 10))
                                {
                                    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                                        "DEBUG, DDL PARSER: in create table, stmt change to alter table logged/unlogged \n");
                                    pg_parser_ddl_init_ddlstate(ddlstate);
                                    ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_ALTER_TABLE_SET_LOG;
                                    ddlstate->m_log_step = PG_PARSER_DDL_ALTER_LOG_GET_CLASS_UPDATE_BEGIN;
                                    ddlstate->m_inddl = true;
                                    return result;
                                }
                                else
                                    break;
                            }
                        }
                    }
                    else if (IS_INSERT(next_record->m_record))
                    {
                        if  (pg_parser_check_table_name(next_record->m_record->m_base.m_tbname, SYS_TYPE, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                        {
                            if (1 >= deep)
                                deep += 1;
                            else
                                break;
                        }
                    }
                    next_record = next_record->m_next;
                }
                next_record = current_record->m_next;
                /* 我们捕获到了一个建表语句, 但这有可能是一个分区表子表,也有可能是继承表 进行判断 */
                while (next_record)
                {
                    /* 遇到了更新pg_class表的语句, 可能为partition of或者alter table logged/unlogged */
                    if (IS_UPDATE(next_record->m_record))
                    {
                        if (pg_parser_check_table_name(next_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                        {
                            bool temp_check_partition_change = false;
                            temp_check_partition_change = pg_parser_ddl_checkChangeColumn("relispartition",
                                                            next_record->m_record->m_new_values,
                                                            next_record->m_record->m_old_values,
                                                            next_record->m_record->m_valueCnt,
                                                            pg_parser_errno);
                            if (temp_check_partition_change)
                            {
                                char *Node = NULL;
                                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                        "DEBUG, DDL PARSER: in create table change ddl type to create partition of \n");
                                Node = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relpartbound",
                                                                              next_record->m_record->m_new_values,
                                                                              next_record->m_record->m_valueCnt,
                                                                              Node);
                                ddlstate->m_partitionsub_node = pg_parser_get_expr(Node, ddlstate->m_zicinfo);
                                ddlstate->m_ddlKind = PG_PARSER_DDL_TABLE_CREATE_PARTITION_SUB;
                                break;
                            }
                            /* 非以上情况, 跳出 */
                            else
                                break;
                        }
                    }
                    /* 有以下几种情况, 继承表, 默认值, 约束 */
                    else if (IS_INSERT(next_record->m_record))
                    {
                        /* 继承表 */
                        if (pg_parser_check_table_name(next_record->m_record->m_base.m_tbname, SYS_INHERITS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                        {
                            char *temp_str = NULL;
                            uint32_t temp_uint32 = 0;
                            ddlstate->m_inherits_cnt += 1;
                            if (!pg_parser_mcxt_realloc(DDL_CREATE_TABLE_MCXT,
                                                        (void **)&(ddlstate->m_inherits_oid),
                                                        ddlstate->m_inherits_cnt * sizeof(uint32_t)))
                            {
                                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_52;
                                return NULL;
                            }
                            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("inhrelid",
                                                            next_record->m_record->m_new_values,
                                                            next_record->m_record->m_valueCnt,
                                                            temp_str);
                            temp_uint32 = strtoul(temp_str, NULL, 10);
                            if (ddlstate->m_reloid == temp_uint32)
                            {
                                temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("inhparent",
                                                            next_record->m_record->m_new_values,
                                                            next_record->m_record->m_valueCnt,
                                                            temp_str);
                                temp_uint32 = strtoul(temp_str, NULL, 10);
                                ddlstate->m_inherits_oid[ddlstate->m_inherits_cnt - 1] = temp_uint32;
                            }
                            break;
                        }
                        /* 处理默认值的跳过处理 */
                        else if (pg_parser_check_table_name(next_record->m_record->m_base.m_tbname, SYS_ATTRDEF, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                        {
                            /* 跳过3条record */
                            for (i = 0; i < 3; i++)
                            {
                                next_record = next_record->m_next;
                                if (!next_record)
                                    break;
                            }
                        }
                        /* 处理约束的跳过处理 */
                        else if (pg_parser_check_table_name(next_record->m_record->m_base.m_tbname, SYS_CONSTRAINT, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
                        {
                            /* 跳过3条record */
                            for (i = 0; i < 4; i++)
                            {
                                next_record = next_record->m_next;
                                if (!next_record)
                                    break;
                            }
                        }
                        else
                            break;
                    }
                    else
                        break;
                }
                if (PG_PARSER_DDL_TABLE_CREATE == ddlstate->m_ddlKind)
                    result = pg_parser_ddl_assemble_create_table(pg_parser_ddl, ddlstate, pg_parser_errno);
            }
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_create_table_partition(
                                                        pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                        pg_parser_translog_systb2dll_record *current_record,
                                                        pg_parser_ddlstate *ddlstate,
                                                        int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    PG_PARSER_UNUSED(pg_parser_ddl);
    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            if (!pg_parser_ddl_get_attribute_info(ddlstate, current_record->m_record, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
                return NULL;
            }
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table partition by, get attributes \n");
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_PARTITIONED, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            ddlstate->m_get_partition = true;
            ddlstate->m_partition = current_record->m_record;
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table partition by, get partition define \n");
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_INHERITS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            ddlstate->m_inherits = current_record->m_record;
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table partition by, get inherits means stmt have partition of \n");
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRDEF, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
            ddlstate->m_attrdef_list = pg_parser_list_lappend(ddlstate->m_attrdef_list, current_record->m_record);

        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            char *temp_refclassid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_classid);
            temp_refclassid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refclassid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_refclassid);
            if (ddlstate->m_get_partition
                 && !strcmp(temp_objid, ddlstate->m_reloid_char)
                 && !strcmp(RelationRelationIdChar, temp_classid)
                 && !strcmp(RelationRelationIdChar, temp_refclassid))
            {
                result = pg_parser_ddl_assemble_create_table_partition(pg_parser_ddl, ddlstate, pg_parser_errno);
            }
        }
    }
    else if(IS_UPDATE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *Node = NULL;
            bool temp_check_partition_change = pg_parser_ddl_checkChangeColumn("relispartition",
                                            current_record->m_record->m_new_values,
                                            current_record->m_record->m_old_values,
                                            current_record->m_record->m_valueCnt,
                                            pg_parser_errno);
            if (temp_check_partition_change)
            {
                Node = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("relpartbound",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              Node);
                ddlstate->m_partitionsub_node = pg_parser_get_expr(Node, ddlstate->m_zicinfo);
            }
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_DDL_create_table_partition_sub(
                                                        pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                        pg_parser_translog_systb2dll_record *current_record,
                                                        pg_parser_ddlstate *ddlstate,
                                                        int32_t *pg_parser_errno)
{
    pg_parser_translog_ddlstmt *result = NULL;
    PG_PARSER_UNUSED(pg_parser_ddl);
    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            if (!pg_parser_ddl_get_attribute_info(ddlstate, current_record->m_record, true))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
                return NULL;
            }
            pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table partition of, get attributes \n");
        }

        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_INHERITS, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
            ddlstate->m_inherits = current_record->m_record;
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            char *temp_refclassid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_classid);
            temp_refclassid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("refclassid",
                                                          current_record->m_record->m_new_values,
                                                          current_record->m_record->m_valueCnt,
                                                          temp_refclassid);
            if (!strcmp(temp_objid, ddlstate->m_reloid_char)
                 && !strcmp(RelationRelationIdChar, temp_classid)
                 && !strcmp(RelationRelationIdChar, temp_refclassid))
            {
                result = pg_parser_ddl_assemble_create_table_partition_sub(pg_parser_ddl, ddlstate, pg_parser_errno);
            }
        }
    }
    return result;
}

static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_create_table(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
        int32_t i = 0;
        pg_parser_translog_ddlstmt *result = NULL;
        pg_parser_translog_ddlstmt_createtable *table_return = NULL;
        pg_parser_translog_ddlstmt_col *column = NULL;
        pg_parser_ListCell *cell = NULL;
        pg_parser_translog_tbcol_values *tbatt = NULL;
        int32_t typmod = -1;
        uint32_t typid = 0;
        char *tempstr = NULL;

        PG_PARSER_UNUSED(pg_parser_ddl);

        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&result,
                                      sizeof(pg_parser_translog_ddlstmt)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_2A;
            *pg_parser_errno = 1;
            return NULL;
        }
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&table_return,
                                      sizeof(pg_parser_translog_ddlstmt_createtable)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_2B;
            return NULL;
        }
        /* 判断表是否为临时表或不记录日志 */
        if (ddlstate->m_unlogged)
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_UNLOGGED;
        else if (ddlstate->m_temp)
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_TEMP;
        else
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_LOGGED;
        if (!(ddlstate->m_attList))
        {
            table_return->m_tableflag = PG_PARSER_DDL_TABLE_FLAG_EMPTY;
            table_return->m_colcnt = 0;
        }
        else
        {
            table_return->m_tableflag = PG_PARSER_DDL_TABLE_FLAG_NORMAL;
            table_return->m_colcnt = ddlstate->m_attList->length;
        }

        table_return->m_relid = ddlstate->m_reloid;
        table_return->m_tabletype = PG_PARSER_DDL_TABLE_TYPE_NORMAL;
        table_return->m_tabname = ddlstate->m_relname;
        table_return->m_nspoid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        table_return->m_owner = ddlstate->m_owner;
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **) &column,
                                      sizeof(pg_parser_translog_ddlstmt_col)
                                             * table_return->m_colcnt))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_2C;
            return NULL;
        }
        i = 0;
        if (ddlstate->m_attList)
        {
            /* 将存放于ddlstate->m_ddlList中的列类型信息附加到返回值中 */
            pg_parser_foreach(cell, ddlstate->m_attList)
            {
                char *temp_notnull = NULL;
                tbatt = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
                temp_notnull = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attnotnull",
                                                    tbatt->m_new_values,
                                                    tbatt->m_valueCnt,
                                                    temp_notnull);
                column[i].m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                    tbatt->m_new_values,
                                                    tbatt->m_valueCnt,
                                                    column[i].m_colname);
                tempstr = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                    tbatt->m_new_values,
                                                    tbatt->m_valueCnt,
                                                    tempstr);
                column[i].m_coltypid = strtoul(tempstr, NULL, 10);
                typid = column[i].m_coltypid;
                tempstr = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypmod",
                                                                    tbatt->m_new_values,
                                                                    tbatt->m_valueCnt,
                                                                    tempstr);
                typmod = atoi(tempstr);
                if (0 <= typmod)
                {
                    column[i].m_typemod = typmod;
                    if (PG_SYSDICT_BPCHAROID == typid || PG_SYSDICT_VARCHAROID == typid)
                    {
                        typmod -= (int32_t) sizeof(int32_t);
                        column[i].m_length = typmod;
                        column[i].m_precision = -1;
                        column[i].m_scale = -1;
                    }
                    else if (PG_SYSDICT_TIMEOID == typid || PG_SYSDICT_TIMETZOID == typid
                        || PG_SYSDICT_TIMESTAMPOID == typid || PG_SYSDICT_TIMESTAMPTZOID == typid)
                    {
                        column[i].m_length = -1;
                        column[i].m_precision = typmod;
                        column[i].m_scale = -1;
                    }
                    else if (PG_SYSDICT_NUMERICOID == typid)
                    {
                        typmod -= (int32_t) sizeof(int32_t);
                        column[i].m_length = -1;
                        column[i].m_precision = (typmod >> 16) & 0xffff;
                        column[i].m_scale = typmod & 0xffff;
                    }
                    else if (PG_SYSDICT_BITOID == typid|| PG_SYSDICT_VARBITOID == typid)
                    {
                        column[i].m_length = typmod;
                        column[i].m_precision = -1;
                        column[i].m_scale = -1;
                    }
                    else
                    {
                        /* 其他情况下, 确保3个值为-1 */
                        column[i].m_length = -1;
                        column[i].m_precision = -1;
                        column[i].m_scale = -1;
                    }
                }
                else
                {
                    column[i].m_length = -1;
                    column[i].m_precision = -1;
                    column[i].m_scale = -1;
                }
                if ('t' == temp_notnull[0])
                    column[i].m_flag = PG_PARSER_DDL_COLUMN_NOTNULL;
                i++;
            }
        }

        table_return->m_cols = column;
        table_return->m_inherits_cnt = ddlstate->m_inherits_cnt;

        if (table_return->m_inherits_cnt > 0)
        {
            pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                    (void **) &table_return->m_inherits,
                                    sizeof(uint32_t) * table_return->m_inherits_cnt);

            rmemcpy0(table_return->m_inherits, 0, ddlstate->m_inherits_oid, sizeof(uint32_t) * table_return->m_inherits_cnt);
        }
        result->m_base.m_ddlinfo = PG_PARSER_DDLTYPE_CREATE;
        result->m_base.m_ddltype = PG_PARSER_DDLINFO_CREATE_TABLE;
        result->m_ddlstmt = (void*) table_return;
        result->m_next = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table end \n");
        pg_parser_ddl_init_ddlstate(ddlstate);
        return result;
}

//todo inherits处理
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_create_table_partition(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
        int32_t i = 0;
        int32_t num = 0;
        char  temp_str2num[7] = {'\0'};
        char *temp_part = NULL;
        char *partattrs_cursor = NULL;
        char *temp_str = NULL;
        bool  have_expr = false;
        pg_parser_translog_ddlstmt *result = NULL;
        pg_parser_translog_ddlstmt_createtable *table_return = NULL;
        pg_parser_translog_ddlstmt_createtable_partitionby *partition = NULL;
        pg_parser_translog_ddlstmt_col *column = NULL;
        pg_parser_ListCell *cell = NULL;
        pg_parser_translog_tbcol_values *tbatt = NULL;
        pg_parser_translog_tbcol_values *tbattdef = NULL;

        PG_PARSER_UNUSED(pg_parser_ddl);

        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&result,
                                      sizeof(pg_parser_translog_ddlstmt)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_2D;
            *pg_parser_errno = 1;
            return NULL;
        }
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&table_return,
                                      sizeof(pg_parser_translog_ddlstmt_createtable)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_2E;
            return NULL;
        }
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&partition,
                                      sizeof(pg_parser_translog_ddlstmt_createtable_partitionby)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_2F;
            return NULL;
        }
        /* 判断表是否为临时表或不记录日志 */
        if (ddlstate->m_unlogged)
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_UNLOGGED;
        else if (ddlstate->m_temp)
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_TEMP;
        else
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_LOGGED;
        if (!ddlstate->m_attList)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_PARTITION1;
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        table_return->m_colcnt = ddlstate->m_attList->length;
        table_return->m_tabletype = PG_PARSER_DDL_TABLE_TYPE_PARTITION;
        table_return->m_tabname = ddlstate->m_relname;
        table_return->m_relid = ddlstate->m_reloid;
        table_return->m_nspoid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        table_return->m_owner = ddlstate->m_owner;
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **) &column,
                                      sizeof(pg_parser_translog_ddlstmt_col)
                                             * table_return->m_colcnt))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_30;
            return NULL;
        }
        i = 0;
        /* 将存放于dlstate->m_ddlList中的列类型信息附加到返回值中 */
        pg_parser_foreach(cell, ddlstate->m_attList)
        {
            char *temp_notnull = NULL;
            tbatt = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
            temp_notnull = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attnotnull",
                                                tbatt->m_new_values,
                                                tbatt->m_valueCnt,
                                                temp_notnull);
            column[i].m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                tbatt->m_new_values,
                                                tbatt->m_valueCnt,
                                                column[i].m_colname);
            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                tbatt->m_new_values,
                                                tbatt->m_valueCnt,
                                                temp_str);
            column[i].m_coltypid = strtoul(temp_str, NULL, 10);
            if ('t' == temp_notnull[0])
                column[i].m_flag = PG_PARSER_DDL_COLUMN_NOTNULL;
            i++;
        }
        table_return->m_cols = column;
        pg_parser_foreach(cell, ddlstate->m_attrdef_list)
        {
            char *temp_num = NULL;
            char *Node = NULL;
            tbattdef = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
            temp_num = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("adnum",
                                                tbattdef->m_new_values,
                                                tbattdef->m_valueCnt,
                                                temp_num);
            Node = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("adbin",
                                                          tbattdef->m_new_values,
                                                          tbattdef->m_valueCnt,
                                                          Node);
        
            table_return->m_cols[atoi(temp_num) - 1].m_default = pg_parser_get_expr(Node, ddlstate->m_zicinfo);
        }

        if (!ddlstate->m_partition)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_PARTITION2;
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        /* 处理分区表 */
        temp_part = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("partstrat",
                                        ddlstate->m_partition->m_new_values,
                                        ddlstate->m_partition->m_valueCnt,
                                        temp_part);
        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("partnatts",
                                                ddlstate->m_partition->m_new_values,
                                                ddlstate->m_partition->m_valueCnt,
                                                temp_str);
        partition->m_column_num = (uint16_t) strtoul(temp_str, NULL, 10);
        if ('h' == temp_part[0])
            partition->m_partition_type = PG_PARSER_DDL_PARTITION_TABLE_HASH;
        else if ('l' == temp_part[0])
            partition->m_partition_type = PG_PARSER_DDL_PARTITION_TABLE_LIST;
        else if ('r' == temp_part[0])
            partition->m_partition_type = PG_PARSER_DDL_PARTITION_TABLE_RANGE;

        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **) &partition->m_column,
                                      sizeof(uint16_t) * partition->m_column_num))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_31;
            return NULL;
        }
        temp_part = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("partattrs",
                                        ddlstate->m_partition->m_new_values,
                                        ddlstate->m_partition->m_valueCnt,
                                        temp_part);
        partattrs_cursor = temp_part;
        i = 0;
        while (*temp_part)
        {
            if (' ' == *temp_part)
            {
                num = temp_part - partattrs_cursor;
                strncpy(temp_str2num, partattrs_cursor, num);
                temp_str2num[num] = '\0';
                partition->m_column[i] = (int16_t) atoi(temp_str2num);
                rmemset1(temp_str2num, 0, 0, 7);
                partattrs_cursor = temp_part + 1;
                i++;
            }

            temp_part++;

            if ('\0' == *temp_part)
            {
                num = temp_part - partattrs_cursor;
                strncpy(temp_str2num, partattrs_cursor, num);
                temp_str2num[num] = '\0';
                partition->m_column[i] = (int16_t) atoi(temp_str2num);
                rmemset1(temp_str2num, 0, 0, 7);
                i++;
            }
        }
        for (i = 0; i < partition->m_column_num; i++)
        {
            if (0 == partition->m_column[i])
                have_expr = true;
        }
        if (have_expr)
        {
            char *Node = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("partexprs",
                                                                ddlstate->m_partition->m_new_values,
                                                                ddlstate->m_partition->m_valueCnt,
                                                                Node);
            partition->m_colnode = pg_parser_get_expr(Node, ddlstate->m_zicinfo);
        }

        table_return->m_partitionby = partition;

        /* 如果有m_inherits, 那么这是一个二级分区, 获取二级分区信息 */
        if (ddlstate->m_inherits)
        {
            pg_parser_translog_ddlstmt_createtable_partitionsub *partitionsub = NULL;
            if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&partitionsub,
                                      sizeof(pg_parser_translog_ddlstmt_createtable_partitionsub)))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_32;
                *pg_parser_errno = 1;
                return NULL;
            }

            partitionsub->m_partitionof_node = ddlstate->m_partitionsub_node;
            if (!ddlstate->m_inherits)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_PARTITION3;
                pg_parser_ddl_init_ddlstate(ddlstate);
                return NULL;
            }
            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("inhparent",
                                                            ddlstate->m_inherits->m_new_values,
                                                            ddlstate->m_inherits->m_valueCnt,
                                                            temp_str);
            partitionsub->m_partitionof_table_oid = strtoul(temp_str, NULL, 10);
            table_return->m_tabletype = PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH;
            table_return->m_partitionof = partitionsub;
        }

        result->m_base.m_ddlinfo = PG_PARSER_DDLTYPE_CREATE;
        result->m_base.m_ddltype = PG_PARSER_DDLINFO_CREATE_TABLE;
        result->m_ddlstmt = (void*) table_return;
        result->m_next = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table partition by end \n");
        pg_parser_ddl_init_ddlstate(ddlstate);
        return result;
}

//todo pg_nodetree处理
static pg_parser_translog_ddlstmt *pg_parser_ddl_assemble_create_table_partition_sub(
                                                pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                pg_parser_ddlstate *ddlstate,
                                                int32_t *pg_parser_errno)
{
        int32_t i = 0;
        pg_parser_translog_ddlstmt *result = NULL;
        pg_parser_translog_ddlstmt_createtable *table_return = NULL;
        pg_parser_translog_ddlstmt_createtable_partitionsub *partitionsub = NULL;
        pg_parser_translog_ddlstmt_col *column = NULL;
        pg_parser_ListCell *cell = NULL;
        pg_parser_translog_tbcol_values *tbatt = NULL;
        char * temp_str = NULL;

        PG_PARSER_UNUSED(pg_parser_ddl);

        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&result,
                                      sizeof(pg_parser_translog_ddlstmt)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_33;
            *pg_parser_errno = 1;
            return NULL;
        }
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&table_return,
                                      sizeof(pg_parser_translog_ddlstmt_createtable)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_34;
            return NULL;
        }
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **)&partitionsub,
                                      sizeof(pg_parser_translog_ddlstmt_createtable_partitionsub)))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_35;
            return NULL;
        }
        /* 判断表是否为临时表或不记录日志 */
        if (ddlstate->m_unlogged)
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_UNLOGGED;
        else if (ddlstate->m_temp)
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_TEMP;
        else
            table_return->m_logtype = PG_PARSER_DDL_TABLE_LOG_LOGGED;
        if (!ddlstate->m_attList)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_PARTITION_SUB1;
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        table_return->m_colcnt = ddlstate->m_attList->length;
        table_return->m_tabletype = PG_PARSER_DDL_TABLE_TYPE_PARTITION_SUB;
        table_return->m_tabname = ddlstate->m_relname;
        table_return->m_relid = ddlstate->m_reloid;
        table_return->m_nspoid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
        table_return->m_owner = ddlstate->m_owner;
        //todo free
        if (!pg_parser_mcxt_malloc(DDL_CREATE_TABLE_MCXT,
                                     (void **) &column,
                                      sizeof(pg_parser_translog_ddlstmt_col)
                                             * table_return->m_colcnt))
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_36;
            return NULL;
        }
        i = 0;
        /* 将存放于dlstate->m_ddlList中的列类型信息附加到返回值中 */
        pg_parser_foreach(cell, ddlstate->m_attList)
        {
            char *temp_notnull = NULL;
            tbatt = (pg_parser_translog_tbcol_values *)pg_parser_lfirst(cell);
            temp_notnull = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attnotnull",
                                                tbatt->m_new_values,
                                                tbatt->m_valueCnt,
                                                temp_notnull);
            column[i].m_colname = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                tbatt->m_new_values,
                                                tbatt->m_valueCnt,
                                                column[i].m_colname);
            temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                tbatt->m_new_values,
                                                tbatt->m_valueCnt,
                                                temp_str);
            column[i].m_coltypid = strtoul(temp_str, NULL, 10);
            if ('t' == temp_notnull[0])
                column[i].m_flag = PG_PARSER_DDL_COLUMN_NOTNULL;
            i++;
        }
        table_return->m_cols = column;

        table_return->m_partitionby = NULL;

        partitionsub->m_partitionof_node = ddlstate->m_partitionsub_node;
        if (!ddlstate->m_inherits)
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_PARTITION_SUB2;
            pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        temp_str = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("inhparent",
                                                        ddlstate->m_inherits->m_new_values,
                                                        ddlstate->m_inherits->m_valueCnt,
                                                        temp_str);
        partitionsub->m_partitionof_table_oid = strtoul(temp_str, NULL, 10);
        table_return->m_partitionof = partitionsub;
        result->m_base.m_ddlinfo = PG_PARSER_DDLTYPE_CREATE;
        result->m_base.m_ddltype = PG_PARSER_DDLINFO_CREATE_TABLE;
        result->m_ddlstmt = (void*) table_return;
        result->m_next = NULL;
        pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create table partition of end \n");
        pg_parser_ddl_init_ddlstate(ddlstate);
        return result;
}
