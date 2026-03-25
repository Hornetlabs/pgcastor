#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"

#define FMGR_MCXT NULL

const pg_parser_FmgrBuiltinNormal pg_parser_fmgr_builtins_normal[] = {
    {"charout", charout},
    {"nameout", nameout},
    {"int2out", int2out},
    {"int4out", int4out},
    {"tidout", tidout},
    {"xidout", xidout},
    {"cidout", cidout},
    {"oidvectorout", oidvectorout},
    {"point_out", point_out},
    {"lseg_out", lseg_out},
    {"box_out", box_out},
    {"float4out", float4out},
    {"float8out", float8out},
    {"macaddr_out", macaddr_out},
    {"int8out", int8out},
    {"inet_out", inet_out},
    {"date_out", date_out},
    {"time_out", time_out},
    {"interval_out", interval_out},
    {"yminterval_out", interval_out},
    {"dsinterval_out", dsinterval_out},
    {"boolout", boolout},
    {"timetz_out", timetz_out},
    {"cidr_out", cidr_out},
    {"circle_out", circle_out},
    {"line_out", line_out},
    {"oidout", oidout},
    {"cstring_out", cstring_out},
    {"void_out", void_out},
    {"shell_out", shell_out},
    {"uuid_out", uuid_out},
    {"tsqueryout", tsqueryout},
    {"macaddr8_out", macaddr8_out},
    {"pg_lsn_out", pg_lsn_out},
    {"regprocout", oidout},
    {"regclassout", regclassout}};

const int32_t pg_parser_fmgr_nbuiltins_normal =
    (sizeof(pg_parser_fmgr_builtins_normal) / sizeof(pg_parser_FmgrBuiltinNormal));

const pg_parser_FmgrBuiltinSpecial pg_parser_fmgr_builtins_special[] = {
    {"byteaout", byteaout},
    {"record_out", record_out},
    {"pg_node_tree_out", pg_node_tree_out},
    {"textout", textout},
    {"int2vectorout", int2vectorout},
    {"json_out", json_out},
    {"cash_out", cash_out},
    {"bpcharout", bpcharout},
    {"varcharout", varcharout},
    {"timestamptz_out", timestamptz_out},
    {"timestamp_out", timestamp_out},
    {"numeric_out", numeric_out},
    {"xml_out", xml_out},
    {"enum_out", enum_out},
    {"anyenum_out", anyenum_out},
    {"tsvectorout", tsvectorout},
    {"bit_out", bit_out},
    {"varbit_out", varbit_out},
    {"path_out", path_out},
    {"poly_out", poly_out},
    {"array_out", array_out},
    {"range_out", range_out},
    {"jsonb_out", jsonb_out},
    {"spheroid_out", spheroid_out},  /* postgis support */
    {"box3d_out", box3d_out},        /* postgis support */
    {"box2d_out", box2d_out},        /* postgis support */
    {"box2df_out", box2df_out},      /* postgis support */
    {"gidx_out", gidx_out},          /* postgis support */
    {"raster_out", raster_out},      /* postgis support */
    {"geometry_out", geometry_out},  /* postgis support */
    {"geography_out", geography_out} /* postgis support */
};
const int32_t pg_parser_fmgr_nbuiltins_special =
    (sizeof(pg_parser_fmgr_builtins_special) / sizeof(pg_parser_FmgrBuiltinSpecial));

static const pg_parser_FmgrBuiltinNormal*  pg_parser_getNormalOutputFuncByName(char* typoutput);
static const pg_parser_FmgrBuiltinSpecial* pg_parser_getSpecialOutputFuncByName(char* typoutput);

static char* typoutput_tolower(char* output)
{
    int i = 0;

    for (i = 0; i < (int32_t)strlen(output); i++)
    {
        if (output[i] >= 'A' && output[i] <= 'Z')
        {
            output[i] += 'a' - 'A';
        }
    }
    return output;
}

bool pg_parser_convert_attr_to_str_value(pg_parser_Datum attr, pg_parser_sysdicts* sysdicts,
                                         pg_parser_translog_tbcol_value*          colvalue,
                                         pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    char*                               typoutput = NULL;
    bool                                is_special = false;
    uint32_t                            typid = colvalue->m_coltype;
    pg_parser_sysdict_TypeInfo          typinfo;
    const pg_parser_FmgrBuiltinNormal*  fmgr_normal = NULL;
    const pg_parser_FmgrBuiltinSpecial* fmgr_special = NULL;

    if (!pg_parser_sysdict_getTypeInfo(typid, sysdicts, &typinfo))
    {
        return false;
    }

    typoutput = typinfo.typoutput_proname;
    typoutput = typoutput_tolower(typoutput);
    pg_parser_log_errlog(zicinfo->debuglevel, "DEBUG: ready use %s convert attr to str\n",
                         typoutput);
    /* Currently does not support anyarray_out */
    if (!strcmp("anyarray_out", typoutput))
    {
        colvalue->m_info = INFO_COL_IS_NULL;
        return true;
    }
    fmgr_normal = pg_parser_getNormalOutputFuncByName(typoutput);
    if (!fmgr_normal)
    {
        is_special = true;
        fmgr_special = pg_parser_getSpecialOutputFuncByName(typoutput);
        if (!fmgr_special)
        {
            colvalue->m_value = (void*)pg_parser_mcxt_strdup(">UNSUPPORT TYPE<");
            colvalue->m_valueLen = strlen(colvalue->m_value);
            return true;
        }
    }

    if (!is_special)
    {
        /* Normal types do not need extra processing, directly Assignment after conversion */
        colvalue->m_value = (void*)fmgr_normal->func(attr);
        if (!colvalue->m_value)
        {
            return false;
        }

        colvalue->m_valueLen = strlen(colvalue->m_value);
    }
    else
    {
        /* Special functions need extra parameters, prepare parameters first */
        pg_parser_extraTypoutInfo info = {'\0'};
        info.typrelid = typinfo.typrelid;
        info.sysdicts = sysdicts;
        info.valueinfo = colvalue->m_info;
        info.zicinfo = zicinfo;
        colvalue->m_value = (void*)fmgr_special->func(attr, &info);
        if (!colvalue->m_value)
        {
            /* If there is a problem with pg_nodetree, filter pg_rewrite */
            if (!strcmp(typoutput, "pg_node_tree_out"))
            {
                colvalue->m_info = INFO_NOTHING;
                colvalue->m_value = (void*)pg_parser_mcxt_strdup(">UNSUPPORT<");
                colvalue->m_valueLen = strlen(colvalue->m_value);
                return true;
            }
            else
            {
                return false;
            }
        }

        /* Set length and flags */
        colvalue->m_valueLen = info.valuelen;
        colvalue->m_info = info.valueinfo;
    }
    if (colvalue->m_info == INFO_NOTHING &&
        strcmp(zicinfo->convertinfo->m_tartgetcharset, zicinfo->convertinfo->m_dbcharset))
    {
        char* temp_ptr = (char*)colvalue->m_value;
        bool  needfree = false;
        colvalue->m_value = pg_parser_encoding_convert((char*)colvalue->m_value, &needfree,
                                                       zicinfo->convertinfo->m_tartgetcharset,
                                                       zicinfo->convertinfo->m_dbcharset);
        if (needfree)
        {
            pg_parser_mcxt_free(FMGR_MCXT, temp_ptr);
        }
        if (!colvalue->m_value)
        {
            return false;
        }
    }
    if ((colvalue->m_info == INFO_NOTHING))
    {
        pg_parser_log_errlog(zicinfo->debuglevel, "DEBUG: success get values[%s]\n",
                             (char*)(colvalue->m_value));
    }

    return true;
}

char* pg_parser_convert_attr_to_str_char(pg_parser_Datum attr, pg_parser_sysdicts* sysdicts,
                                         uint32_t typid, bool* istoast,
                                         pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    char*                               typoutput = NULL;
    char*                               result = NULL;
    bool                                is_special = false;
    uint8_t                             value_info = 0;
    pg_parser_sysdict_TypeInfo          typinfo;
    const pg_parser_FmgrBuiltinNormal*  fmgr_normal = NULL;
    const pg_parser_FmgrBuiltinSpecial* fmgr_special = NULL;

    if (!pg_parser_sysdict_getTypeInfo(typid, sysdicts, &typinfo))
    {
        return NULL;
    }

    typoutput = typinfo.typoutput_proname;
    typoutput = typoutput_tolower(typoutput);
    pg_parser_log_errlog(zicinfo->debuglevel, "DEBUG: ready use %s convert attr to str\n",
                         typoutput);
    /* Currently does not support anyarray_out */
    fmgr_normal = pg_parser_getNormalOutputFuncByName(typoutput);
    if (!strcmp("anyarray_out", typoutput))
    {
        return pg_parser_mcxt_strdup(">UNSUPPORT<");
    }

    if (!fmgr_normal)
    {
        is_special = true;
        fmgr_special = pg_parser_getSpecialOutputFuncByName(typoutput);
        if (!fmgr_special)
        {
            return pg_parser_mcxt_strdup(">UNSUPPORT TYPE<");
        }
    }

    if (!is_special)
    {
        /* Normal types do not need extra processing, directly Assignment after conversion */
        result = (char*)fmgr_normal->func(attr);
        if (!result)
        {
            return NULL;
        }
    }
    else
    {
        /* Special functions need extra parameters, prepare parameters first */
        pg_parser_extraTypoutInfo info = {'\0'};
        info.typrelid = typinfo.typrelid;
        info.sysdicts = sysdicts;
        info.valueinfo = 0;
        info.zicinfo = zicinfo;
        result = (char*)fmgr_special->func(attr, &info);
        if (!result)
        {
            return NULL;
        }
        if (info.valueinfo == INFO_COL_IS_TOAST)
        {
            *istoast = true;
        }
        value_info = info.valueinfo;
    }

    if (value_info == INFO_NOTHING &&
        strcmp(zicinfo->convertinfo->m_tartgetcharset, zicinfo->convertinfo->m_dbcharset))
    {
        char* temp_ptr = result;
        bool  needfree = false;
        result =
            pg_parser_encoding_convert(result, &needfree, zicinfo->convertinfo->m_tartgetcharset,
                                       zicinfo->convertinfo->m_dbcharset);
        if (needfree)
        {
            pg_parser_mcxt_free(FMGR_MCXT, temp_ptr);
        }
        if (!result)
        {
            return NULL;
        }
    }
    return result;
}

bool pg_parser_convert_attr_to_str_external_value(pg_parser_Datum attr, char* typoutput,
                                                  pg_parser_translog_tbcol_value*          colvalue,
                                                  pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    bool                                is_special = false;
    const pg_parser_FmgrBuiltinNormal*  fmgr_normal = NULL;
    const pg_parser_FmgrBuiltinSpecial* fmgr_special = NULL;

    typoutput = typoutput_tolower(typoutput);
    pg_parser_log_errlog(zicinfo->debuglevel, "DEBUG: ready use %s convert attr to str\n",
                         typoutput);
    /* Currently does not support anyarray_out */
    if (!strcmp("anyarray_out", typoutput))
    {
        colvalue->m_info = INFO_COL_IS_NULL;
        return true;
    }
    fmgr_normal = pg_parser_getNormalOutputFuncByName(typoutput);
    if (!fmgr_normal)
    {
        is_special = true;
        fmgr_special = pg_parser_getSpecialOutputFuncByName(typoutput);
        if (!fmgr_special)
        {
            colvalue->m_value = (void*)pg_parser_mcxt_strdup(">UNSUPPORT TYPE<");
            colvalue->m_valueLen = strlen(colvalue->m_value);
            return true;
        }
    }

    if (!is_special)
    {
        /* Normal types do not need extra processing, directly Assignment after conversion */
        colvalue->m_value = (void*)fmgr_normal->func(attr);
        if (!colvalue->m_value)
        {
            return false;
        }
        colvalue->m_valueLen = strlen(colvalue->m_value);
    }
    else
    {
        /* Special functions need extra parameters, prepare parameters first */
        pg_parser_extraTypoutInfo info = {'\0'};
        info.sysdicts = NULL;
        info.valueinfo = colvalue->m_info;
        info.zicinfo = zicinfo;
        colvalue->m_value = (void*)fmgr_special->func(attr, &info);
        if (!colvalue->m_value)
        {
            /* If there is a problem with pg_nodetree, filter pg_rewrite */
            if (!strcmp(typoutput, "pg_node_tree_out"))
            {
                colvalue->m_info = INFO_NOTHING;
                colvalue->m_value = (void*)pg_parser_mcxt_strdup(">UNSUPPORT<");
                colvalue->m_valueLen = strlen(colvalue->m_value);
                return true;
            }
            else
            {
                return false;
            }
        }

        /* Set length and flags */
        colvalue->m_valueLen = info.valuelen;
        colvalue->m_info = info.valueinfo;
    }
    if (colvalue->m_info == INFO_NOTHING &&
        strcmp(zicinfo->convertinfo->m_tartgetcharset, zicinfo->convertinfo->m_dbcharset))
    {
        char* temp_ptr = (char*)colvalue->m_value;
        bool  needfree = false;
        colvalue->m_value = pg_parser_encoding_convert((char*)colvalue->m_value, &needfree,
                                                       zicinfo->convertinfo->m_tartgetcharset,
                                                       zicinfo->convertinfo->m_dbcharset);
        if (needfree)
        {
            pg_parser_mcxt_free(FMGR_MCXT, temp_ptr);
        }
        if (!colvalue->m_value)
        {
            return false;
        }
    }
    return true;
}

static const pg_parser_FmgrBuiltinNormal* pg_parser_getNormalOutputFuncByName(char* typoutput)
{
    int32_t i = 0;
    for (i = 0; i < pg_parser_fmgr_nbuiltins_normal; i++)
    {
        if (!strcmp(typoutput, pg_parser_fmgr_builtins_normal[i].funcName))
        {
            return &pg_parser_fmgr_builtins_normal[i];
        }
    }
    return NULL;
}

static const pg_parser_FmgrBuiltinSpecial* pg_parser_getSpecialOutputFuncByName(char* typoutput)
{
    int32_t i = 0;
    for (i = 0; i < pg_parser_fmgr_nbuiltins_special; i++)
    {
        if (!strcmp(typoutput, pg_parser_fmgr_builtins_special[i].funcName))
        {
            return &pg_parser_fmgr_builtins_special[i];
        }
    }
    return NULL;
}

char* pg_parser_convert_attr_to_str_by_typid_typoptput(
    pg_parser_Datum attr, uint32_t typid, char* typoutput,
    pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    char*                               result = NULL;
    bool                                is_special = false;
    uint8_t                             value_info = 0;
    const pg_parser_FmgrBuiltinNormal*  fmgr_normal = NULL;
    const pg_parser_FmgrBuiltinSpecial* fmgr_special = NULL;

    typoutput = typoutput_tolower(typoutput);
    pg_parser_log_errlog(zicinfo->debuglevel, "DEBUG: ready use %s convert attr to str\n",
                         typoutput);
    /* Currently does not support anyarray_out */
    if (!strcmp("anyarray_out", typoutput))
    {
        pg_parser_mcxt_strdup(">UNSUPPORT<");
    }

    if (!typoutput)
    {
        return NULL;
    }

    fmgr_normal = pg_parser_getNormalOutputFuncByName(typoutput);
    if (!fmgr_normal)
    {
        is_special = true;
        fmgr_special = pg_parser_getSpecialOutputFuncByName(typoutput);
        if (!fmgr_special)
        {
            return pg_parser_mcxt_strdup(">UNSUPPORT TYPE<");
        }
    }

    if (!is_special)
    {
        /* Normal types do not need extra processing, directly Assignment after conversion */
        result = (char*)fmgr_normal->func(attr);
        if (!result)
        {
            return NULL;
        }
    }
    else
    {
        /* Special functions need extra parameters, prepare parameters first */
        pg_parser_extraTypoutInfo info = {'\0'};
        info.typrelid = typid;
        info.sysdicts = NULL;
        info.valueinfo = 0;
        info.zicinfo = zicinfo;
        result = (char*)fmgr_special->func(attr, &info);
        if (!result)
        {
            return NULL;
        }
        if (info.valueinfo == INFO_COL_IS_TOAST)
        {
            return NULL;
        }
        value_info = info.valueinfo;
    }

    if (value_info == INFO_NOTHING &&
        strcmp(zicinfo->convertinfo->m_tartgetcharset, zicinfo->convertinfo->m_dbcharset))
    {
        char* temp_ptr = result;
        bool  needfree = false;
        result =
            pg_parser_encoding_convert(result, &needfree, zicinfo->convertinfo->m_tartgetcharset,
                                       zicinfo->convertinfo->m_dbcharset);
        if (needfree)
        {
            pg_parser_mcxt_free(FMGR_MCXT, temp_ptr);
        }
        if (!result)
        {
            return NULL;
        }
    }
    return result;
}
