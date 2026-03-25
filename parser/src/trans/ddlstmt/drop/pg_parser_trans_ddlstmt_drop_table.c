#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_DROP_TABLE_MCXT NULL

pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_drop_table(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno);

pg_parser_translog_ddlstmt* pg_parser_DDL_drop_table(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno)
{
    pg_parser_translog_ddlstmt* result = NULL;
    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_DEPEND,
                                       pg_parser_ddl->m_dbtype, pg_parser_ddl->m_dbversion))
        {
            char* temp_objid = NULL;
            char* temp_classid = NULL;
            temp_objid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                "objid", current_record->m_record->m_old_values,
                current_record->m_record->m_valueCnt, temp_objid);
            temp_classid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME(
                "classid", current_record->m_record->m_old_values,
                current_record->m_record->m_valueCnt, temp_classid);
            if (!(strcmp(ddlstate->m_reloid_char, temp_objid)) &&
                !(strcmp(RelationRelationIdChar, temp_classid)))
            {
                /* Filter external tables */
                if (ddlstate->m_relkind == 'f')
                {
                    pg_parser_ddl_init_ddlstate(ddlstate);
                    result = NULL;
                }
                else
                {
                    result = pg_parser_ddl_assemble_drop_table(pg_parser_ddl, current_record,
                                                               ddlstate, pg_parser_errno);
                }
            }
        }
    }
    return result;
}

pg_parser_translog_ddlstmt* pg_parser_ddl_assemble_drop_table(
    pg_parser_translog_systb2ddl*        pg_parser_ddl,
    pg_parser_translog_systb2dll_record* current_record, pg_parser_ddlstate* ddlstate,
    int32_t* pg_parser_errno)
{
    pg_parser_translog_ddlstmt*           result = NULL;
    pg_parser_translog_ddlstmt_drop_base* drop;

    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(current_record);
    PG_PARSER_UNUSED(pg_parser_errno);

    // todo free
    if (!pg_parser_mcxt_malloc(DDL_DROP_TABLE_MCXT, (void**)&result,
                               sizeof(pg_parser_translog_ddlstmt)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_45;
        return NULL;
    }
    if (!pg_parser_mcxt_malloc(DDL_DROP_TABLE_MCXT, (void**)&drop,
                               sizeof(pg_parser_translog_ddlstmt_drop_base)))
    {
        *pg_parser_errno = ERRNO_PG_PARSER_DDL_MEMERR_ALLOC_46;
        return NULL;
    }
    drop->m_namespace_oid = strtoul(ddlstate->m_nspname_oid_char, NULL, 10);
    drop->m_name = ddlstate->m_relname;
    drop->m_relid = ddlstate->m_reloid;
    result->m_base.m_ddltype = PG_PARSER_DDLTYPE_DROP;
    result->m_base.m_ddlinfo = PG_PARSER_DDLINFO_DROP_TABLE;
    result->m_ddlstmt = (void*)drop;
    result->m_next = NULL;
    pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, "DEBUG, DDL PARSER: drop table end \n");
    pg_parser_ddl_init_ddlstate(ddlstate);
    return result;
}
