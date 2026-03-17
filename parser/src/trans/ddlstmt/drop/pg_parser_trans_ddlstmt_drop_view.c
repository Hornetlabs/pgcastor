#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "trans/ddlstmt/pg_parser_trans_ddlstmt_transfunc.h"

#define DDL_DROP_VIEW_MCXT NULL

/* 
 * 该版本中只是对drop view做一个过滤操作, 不返回实际解析内容
 */
pg_parser_translog_ddlstmt* pg_parser_DDL_drop_view(pg_parser_translog_systb2ddl *pg_parser_ddl,
                                                          pg_parser_translog_systb2dll_record *current_record,
                                                          pg_parser_ddlstate *ddlstate,
                                                          int32_t *pg_parser_errno)
{
    PG_PARSER_UNUSED(pg_parser_ddl);
    PG_PARSER_UNUSED(pg_parser_errno);
    if (IS_DELETE(current_record->m_record))
    {
        if (pg_parser_check_table_name(current_record->m_record->m_base.m_tbname, SYS_CLASS, pg_parser_ddl->m_dbtype , pg_parser_ddl->m_dbversion))
        {
            char *temprelid = PG_PARSER_DDL_GETCOLUMNVALUEBYNAME("oid",
                                                        current_record->m_record->m_old_values,
                                                        current_record->m_record->m_valueCnt,
                                                        temprelid);
            if (!strcmp(temprelid, ddlstate->m_reloid_char))
            {
                pg_parser_log_errlog(pg_parser_ddl->m_debugLevel, 
                                   "DEBUG, DDL PARSER: capture drop view(ignore) end \n");
                pg_parser_ddl_init_ddlstate(ddlstate);
            }
        }
    }
    return NULL;
}