#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_CREATE_VIEW_MCXT NULL

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_create_view(
    pg_parser_translog_systb2ddl* pg_parser_ddl, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

pg_parser_translog_ddlstmt* pg_parser_DDL_create_view(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;
    if (IS_INSERT(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_ATTRIBUTE,
                                       pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            /* nothing need to do */
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_REWRITE,
                                            pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            /* nothing need to do */
            // ddlstate->m_view_def = current_record->m_record;
        }
        else if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND,
                                            pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
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
                !strcmp(RewriteRelationIdChar, temp_classid))
            {
                result =
                    pg_parser_ddl_assemble_create_view(pg_parser_ddl, ddlstate, pg_parser_errno);
            }
        }
    }
    return result;
}

static pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_create_view(
    pg_parser_translog_systb2ddl* pg_parser_ddl, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno)
{
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);
    // todo COMPLETE
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel,
                         "DEBUG, DDL PARSER: create view(ignore) end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return NULL;
}
