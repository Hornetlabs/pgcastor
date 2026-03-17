#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt.h"
#include "trans/ddlstmt/xk_pg_parser_trans_ddlstmt_getcolumninfo.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"

char *xk_pg_parser_ddl_getColumnValueByName(char *column_name,
                                           xk_pg_parser_translog_tbcol_value *record_value,
                                           uint16_t cnt)
{
    int32_t i = 0;
    for (i = 0; i < cnt; i++)
    {
        if (!record_value)
            return NULL;

        if (record_value[i].m_colName && !strcmp(ddl_char_tolower((char *)record_value[i].m_colName),
                                                 column_name))
        {
            if (INFO_COL_IS_NULL == record_value[i].m_info
             || INFO_COL_MAY_NULL== record_value[i].m_info)
                return NULL;
            else
                return (char *)record_value[i].m_value;
        }
    }
    return NULL;
}

bool xk_pg_parser_ddl_get_pg_class_info(xk_pg_parser_ddlstate *ddlstate,
                                        xk_pg_parser_translog_tbcol_values *record,
                                        bool isnew)
{
    char *temp_str = NULL;
    xk_pg_parser_translog_tbcol_value *values = NULL;
    if (isnew)
        values = record->m_new_values;
    else
        values = record->m_old_values;
    temp_str = xk_pg_parser_ddl_getColumnValueByName("relpersistence", values, record->m_valueCnt);
    if (!temp_str)
        return false;
    ddlstate->m_relpersistence = temp_str[0];
    temp_str = xk_pg_parser_ddl_getColumnValueByName("relpersistence", values, record->m_valueCnt);
    if (!temp_str)
        return false;
    ddlstate->m_unlogged = ('u' == temp_str[0] ? true: false);
    ddlstate->m_temp = ('t' == temp_str[0] ? true: false);
#if XK_PG_VERSION_NUM >= 120000
    temp_str = xk_pg_parser_ddl_getColumnValueByName("oid", values, record->m_valueCnt);
    if (!temp_str)
        return false;
    ddlstate->m_reloid_char = temp_str;
    ddlstate->m_reloid = (uint32_t)strtoul(temp_str, NULL, 10);
#else
    if (!new)
        ddldata->m_reloid =
            HeapTupleHeaderGetOid(((HeapTuple)&mchange->data.tp.oldtuple->tuple)->t_data);
    else
        ddldata->m_reloid =
            HeapTupleHeaderGetOid(((HeapTuple)&mchange->data.tp.newtuple->tuple)->t_data);
#endif
    ddlstate->m_amoid_char = xk_pg_parser_ddl_getColumnValueByName("relam", values, record->m_valueCnt);
    if (!ddlstate->m_amoid_char)
        return false;
    ddlstate->m_relname = xk_pg_parser_ddl_getColumnValueByName("relname", values, record->m_valueCnt);
    if (!ddlstate->m_relname)
        return false;
    ddlstate->m_nspname_oid_char = xk_pg_parser_ddl_getColumnValueByName("relnamespace", values, record->m_valueCnt);
    if (!ddlstate->m_nspname_oid_char)
        return false;
    temp_str = NULL;
    temp_str = xk_pg_parser_ddl_getColumnValueByName("relowner", values, record->m_valueCnt);
    if (!temp_str)
        return false;
    ddlstate->m_owner = (uint32_t)strtoul(temp_str, NULL, 10);
    return true;
}

bool xk_pg_parser_ddl_get_attribute_info(xk_pg_parser_ddlstate *ddlstate,
                                         xk_pg_parser_translog_tbcol_values *record,
                                         bool isnew)
{
    char *temp_str = NULL;
    int16_t attnum = 0;
    xk_pg_parser_translog_tbcol_value *values = NULL;
    bool attisdropped = false;

    if (isnew)
        values = record->m_new_values;
    else
        values = record->m_old_values;
    temp_str = xk_pg_parser_ddl_getColumnValueByName("attnum",
                                                        values,
                                                        record->m_valueCnt);
    if (!temp_str)
        return false;
    attnum = (int16_t) atoi(temp_str);
    temp_str = NULL;
    temp_str = xk_pg_parser_ddl_getColumnValueByName("attisdropped",
                                                     values,
                                                     record->m_valueCnt);
    if (!temp_str)
        return false;
    if ('t' == temp_str[0])
        attisdropped = true;

    if (0 < attnum && false == attisdropped)
        ddlstate->m_attList = xk_pg_parser_list_lappend(ddlstate->m_attList, record);
    return true;
}

bool xk_pg_parser_ddl_checkChangeColumn(char *column_name,
                                      xk_pg_parser_translog_tbcol_value *record_new,
                                      xk_pg_parser_translog_tbcol_value *record_old,
                                      uint16_t cnt,
                                      int32_t *xk_pg_parser_errno)
{
    char *new = NULL,
         *old = NULL;

    new = xk_pg_parser_ddl_getColumnValueByName(column_name, record_new, cnt);
    old = xk_pg_parser_ddl_getColumnValueByName(column_name, record_old, cnt);

    if (!new && !old) 
    {
        *xk_pg_parser_errno = XK_ERRNO_PG_PARSER_DDLSTMT_GET_COLUMN_BY_COLNAME;
        return NULL;
    }
    else if ((new && !old) || (!new && old))
        return true;
    if (strcmp(new, old))
        return true;
    else
        return false;
}
#if 0
bool xk_pg_parser_ddl_getTypnameByOid(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                       uint32_t typid,
                                       char **typname)
{
    return xk_pg_parser_sysdict_getTypeNameByOid(typid,
                                                 &xk_pg_parser_ddl->m_systbs->m_pg_type,
                                                 typname);
}

bool xk_pg_parser_ddl_getRelname(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                 char *reloid_char,
                                 char **relname)
{
    return xk_pg_parser_sysdict_getRelNameByOid((uint32_t)strtoul(reloid_char, NULL, 10),
                                              &xk_pg_parser_ddl->m_systbs->m_pg_class,
                                              relname);
}

bool xk_pg_parser_ddl_getNameSpace(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                   char *nspoid_char,
                                   char **nspname)
{
    return xk_pg_parser_sysdict_getNspNameByOid((uint32_t)strtoul(nspoid_char, NULL, 10),
                                              &xk_pg_parser_ddl->m_systbs->m_pg_namespace,
                                              nspname);
}

bool xk_pg_parser_ddl_getRelNameSpace(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                      char *reloid_char,
                                      char **nspname)
{
    uint32_t nspid = xk_pg_parser_InvalidOid;

    if (!xk_pg_parser_sysdict_getNspidByRelid((uint32_t)strtoul(reloid_char, NULL, 10),
                                              &xk_pg_parser_ddl->m_systbs->m_pg_class,
                                              &nspid))
        return false;

    return xk_pg_parser_sysdict_getNspNameByOid(nspid, &xk_pg_parser_ddl->m_systbs->m_pg_namespace, nspname);
}
#endif