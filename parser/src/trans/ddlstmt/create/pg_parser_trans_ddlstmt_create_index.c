#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_transfunc.h"
#include "thirdparty/parsernode/xk_pg_parser_thirdparty_parsernode.h"

#define XK_DDL_CREATE_INDEX_MCXT NULL

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_create_index(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno);

xk_pg_parser_translog_ddlstmt* xk_pg_parser_DDL_create_index(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                            xk_pg_parser_translog_systb2dll_record *current_record,
                                                            xk_pg_parser_ddlstate *ddlstate,
                                                            int32_t *xk_pg_parser_errno)
{
    xk_pg_parser_translog_ddlstmt *result = NULL;


    if (IS_INSERT(current_record->m_record))
    {
        if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            /* 处理pg_attribute记录。 */
            if (!xk_pg_parser_ddl_get_attribute_info(ddlstate, current_record->m_record, true))
            {
                *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_ATTRIBUTE_LIST;
                return NULL;
            }
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create index, get attribute info \n");
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_INDEX, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            /* 存储pg_index的记录 */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create index, get index info \n");
            ddlstate->m_index = current_record->m_record;
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CONSTRAINT, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            /* 存储pg_constraint的记录 add primary key */
            xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: in create index, stmt type chane to alter table add constraint \n");
            ddlstate->m_constraint = current_record->m_record;
            ddlstate->m_ddlKind = XK_PG_PARSER_DDL_TABLE_ALTER_ADD_CONSTRAINT;
        }
        else if (xk_pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND, xk_pg_parser_ddl->m_dbtype, xk_pg_parser_ddl->m_dbversion))
        {
            char *temp_objid = NULL;
            char *temp_classid = NULL;
            temp_objid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("objid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_objid);
            temp_classid = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("classid",
                                                              current_record->m_record->m_new_values,
                                                              current_record->m_record->m_valueCnt,
                                                              temp_classid);
            if (!strcmp(ddlstate->m_reloid_char, temp_objid)
                && !strcmp(RelationRelationIdChar, temp_classid))
                result = xk_pg_parser_ddl_assemble_create_index(xk_pg_parser_ddl, ddlstate, xk_pg_parser_errno);
        }
    }
    return result;
}

static xk_pg_parser_translog_ddlstmt *xk_pg_parser_ddl_assemble_create_index(
                                                xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                                xk_pg_parser_ddlstate *ddlstate,
                                                int32_t *xk_pg_parser_errno)
{
        int32_t i = 0;
        char *temp_isunique = NULL;
        xk_pg_parser_translog_ddlstmt *result = NULL;
        xk_pg_parser_translog_ddlstmt_index *index_return = NULL;
        xk_pg_parser_translog_ddlstmt_col *column = NULL;
        xk_pg_parser_ListCell *cell = NULL;
        xk_pg_parser_translog_tbcol_values *indatt = NULL;
        char * temp_str = NULL;
        char *indkey_cursor = NULL;
        char *temp_part = NULL;
        char  temp_str2num[7] = {'\0'};
        char *Node = NULL;
        int32_t num = 0;

        XK_PG_PARSER_UNUSED(xk_pg_parser_ddl);
        XK_PG_PARSER_UNUSED(xk_pg_parser_errno);

        //todo free
        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_INDEX_MCXT,
                                     (void **)&result,
                                      sizeof(xk_pg_parser_translog_ddlstmt)))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_23;
            return NULL;
        }
        //todo free
        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_INDEX_MCXT,
                                     (void **)&index_return,
                                      sizeof(xk_pg_parser_translog_ddlstmt_index)))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_24;
            return NULL;
        }
        index_return->m_indtype = strtoul(ddlstate->m_amoid_char, NULL, 10);
        if (!ddlstate->m_attList)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_INDEX1;
            xk_pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }
        index_return->m_colcnt = ddlstate->m_attList->length;

        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_INDEX_MCXT,
                                     (void **) &index_return->m_column,
                                      sizeof(uint16_t) * index_return->m_colcnt))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_INDEX2;
            xk_pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }

        index_return->m_indname = ddlstate->m_relname;
        index_return->m_indnspoid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);

        if (!ddlstate->m_index)
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_CHECKPARAM_CREATE_INDEX2;
            xk_pg_parser_ddl_init_ddlstate(ddlstate);
            return NULL;
        }

        temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("indrelid",
                                                ddlstate->m_index->m_new_values,
                                                ddlstate->m_index->m_valueCnt,
                                                temp_str);
        index_return->m_relid = strtoul(temp_str, NULL, 10);
        temp_isunique = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("indisunique",
                                                ddlstate->m_index->m_new_values,
                                                ddlstate->m_index->m_valueCnt,
                                                temp_isunique);
        if ('t' == temp_isunique[0])
            index_return->m_option |= XK_PG_PARSER_DDL_INDEX_UNIQUE;
        //todo free
        if (!xk_pg_parser_mcxt_malloc(XK_DDL_CREATE_INDEX_MCXT,
                                     (void **) &column,
                                      sizeof(xk_pg_parser_translog_ddlstmt_col)
                                             * index_return->m_colcnt))
        {
            *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_25;
            return NULL;
        }
        i = 0;
        /* 将存放于dlstate->m_ddlList中的列类型信息附加到返回值中 */
        xk_pg_parser_foreach(cell, ddlstate->m_attList)
        {
            indatt = (xk_pg_parser_translog_tbcol_values *)xk_pg_parser_lfirst(cell);
            column[i].m_colname = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("attname",
                                                indatt->m_new_values,
                                                indatt->m_valueCnt,
                                                column[i].m_colname);

            temp_str = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("atttypid",
                                                indatt->m_new_values,
                                                indatt->m_valueCnt,
                                                temp_str);
            column[i].m_coltypid = strtoul(temp_str, NULL, 10);
            i++;
        }
        index_return->m_includecols = column;

        /* indexprs可能是空的 */
        Node = xk_pg_parser_ddl_getColumnValueByName("indexprs",
                                        ddlstate->m_index->m_new_values,
                                        ddlstate->m_index->m_valueCnt);
        if (Node)
            index_return->m_colnode = xk_pg_parser_get_expr(Node, ddlstate->m_zicinfo);

        temp_part = XK_PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("indkey",
                                        ddlstate->m_index->m_new_values,
                                        ddlstate->m_index->m_valueCnt,
                                        temp_part);
        indkey_cursor = temp_part;
        i = 0;
        while (*temp_part)
        {
            if (' ' == *temp_part)
            {
                num = temp_part - indkey_cursor;
                strncpy(temp_str2num, indkey_cursor, num);
                temp_str2num[num] = '\0';
                index_return->m_column[i] = (int16_t) atoi(temp_str2num);
                rmemset1(temp_str2num, 0, 0, 7);
                indkey_cursor = temp_part + 1;
                i++;
            }

            temp_part++;

            if ('\0' == *temp_part)
            {
                num = temp_part - indkey_cursor;
                strncpy(temp_str2num, indkey_cursor, num);
                temp_str2num[num] = '\0';
                index_return->m_column[i] = (int16_t) atoi(temp_str2num);
                rmemset1(temp_str2num, 0, 0, 7);
                i++;
            }
        }

        result->m_base.m_ddltype = XK_PG_PARSER_DDLTYPE_CREATE;
        result->m_base.m_ddlinfo = XK_PG_PARSER_DDLINFO_CREATE_INDEX;

        result->m_ddlstmt = (void*) index_return;
        result->m_next = NULL;
        xk_pg_parser_log_errlog(xk_pg_parser_ddl->m_debugLevel, 
                           "DEBUG, DDL PARSER: create index end \n");
        xk_pg_parser_ddl_init_ddlstate(ddlstate);
        return result;
}
