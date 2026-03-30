#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "sysdict/pg_parser_sysdict_pg_type.h"

#define SYSDICT_INVALID_COUNT -1
#define SYSDICT_INVALID_OID   -1
#define SYSDICT_MCXT          NULL

static int32_t getcount_from_pgclass_by_relfilenode(pg_parser_sysdicts* sysdict, uint32_t relfilenode);
static char* getname_from_sysdict_by_oid(pg_parser_sysdicts*    sysdict,
                                         uint32_t               oid,
                                         pg_parser_sysdict_type sysdict_type);

static pg_parser_sysdict_pgattributes* get_attr_by_oid(pg_parser_sysdicts* sysdict, uint32_t oid, int16_t seqnum);

static int32_t getcount_from_sysdict_by_oid(pg_parser_sysdicts*    sysdict,
                                            uint32_t               oid,
                                            pg_parser_sysdict_type sysdict_type);

bool pg_parser_sysdict_getTableInfo_byoid(uint32_t                     oid,
                                          pg_parser_sysdicts*          sysdict,
                                          pg_parser_sysdict_tableInfo* tbinfo)
{
    int32_t  count = SYSDICT_INVALID_COUNT;
    int32_t  i = 0;
    uint32_t temp_nspoid = 0;
    if (NULL == sysdict)
    {
        return false;
    }
    if (!oid)
    {
        return false;
    }
    count = getcount_from_sysdict_by_oid(sysdict, oid, PG_PARSER_SYSDICT_PG_CLASS_TYPE);
    if (count < 0)
    {
        return false;
    }
    /* Assignment */
    tbinfo->natts = sysdict->m_pg_class.m_pg_class[count].relnatts;
    if (tbinfo->natts < 0)
    {
        return false;
    }
    tbinfo->oid = oid;
    tbinfo->relfilenode = sysdict->m_pg_class.m_pg_class[count].relfilenode;
    tbinfo->relkind = sysdict->m_pg_class.m_pg_class[count].relkind;
    tbinfo->toastoid = sysdict->m_pg_class.m_pg_class[count].reltoastrelid;
    tbinfo->tbname = sysdict->m_pg_class.m_pg_class[count].relname.data;
    temp_nspoid = sysdict->m_pg_class.m_pg_class[count].relnamespace;
    tbinfo->scname = getname_from_sysdict_by_oid(sysdict, temp_nspoid, PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE);
    tbinfo->needzic = false;
    if (!pg_parser_mcxt_malloc(SYSDICT_MCXT,
                               (void**)&(tbinfo->pgattr),
                               (tbinfo->natts) * sizeof(pg_parser_sysdict_pgattributes*)))
    {
        return false;
    }
    for (i = 0; i < tbinfo->natts; i++)
    {
        tbinfo->pgattr[i] = get_attr_by_oid(sysdict, tbinfo->oid, i + 1);
        if (NULL == tbinfo->pgattr[i])
        {
            return false;
        }
    }
    return true;
}

bool pg_parser_sysdict_getTableInfo(uint32_t                     record_relfilenode,
                                    pg_parser_sysdicts*          sysdict,
                                    pg_parser_sysdict_tableInfo* tbinfo)
{
    int32_t  count = SYSDICT_INVALID_COUNT;
    int32_t  i = 0;
    uint32_t temp_nspoid = 0;
    if (NULL == sysdict)
    {
        printf("\nerror in tbinfo 1\n");
        return false;
    }

    count = getcount_from_pgclass_by_relfilenode(sysdict, record_relfilenode);
    if (count < 0)
    {
        printf("\nerror in tbinfo 2\n");
        return false;
    }
    /* Assignment */
    tbinfo->natts = sysdict->m_pg_class.m_pg_class[count].relnatts;
    if (tbinfo->natts < 0)
    {
        printf("\nerror in tbinfo 3\n");
        return false;
    }
    tbinfo->oid = sysdict->m_pg_class.m_pg_class[count].oid;
    tbinfo->relfilenode = record_relfilenode;
    tbinfo->relkind = sysdict->m_pg_class.m_pg_class[count].relkind;
    tbinfo->toastoid = sysdict->m_pg_class.m_pg_class[count].reltoastrelid;
    tbinfo->tbname = sysdict->m_pg_class.m_pg_class[count].relname.data;
    if (!tbinfo->tbname)
    {
        printf("\nerror in tbinfo 4\n");
        return false;
    }
    temp_nspoid = sysdict->m_pg_class.m_pg_class[count].relnamespace;
    tbinfo->scname = getname_from_sysdict_by_oid(sysdict, temp_nspoid, PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE);
    if (!tbinfo->scname)
    {
        printf("\nerror in tbinfo 5\n");
        return false;
    }
    tbinfo->needzic = false;
    if (!pg_parser_mcxt_malloc(SYSDICT_MCXT,
                               (void**)&(tbinfo->pgattr),
                               (tbinfo->natts) * sizeof(pg_parser_sysdict_pgattributes*)))
    {
        printf("\nerror in tbinfo 6\n");
        return false;
    }
    for (i = 0; i < tbinfo->natts; i++)
    {
        tbinfo->pgattr[i] = get_attr_by_oid(sysdict, tbinfo->oid, i + 1);
        if (NULL == tbinfo->pgattr[i])
        {
            printf("\nerror in tbinfo 7, %d\n", i);
            return false;
        }
    }
    return true;
}

bool pg_parser_sysdict_getTypeInfo(uint32_t oid, pg_parser_sysdicts* sysdict, pg_parser_sysdict_TypeInfo* typinfo)
{
    int32_t count = SYSDICT_INVALID_COUNT;
    count = getcount_from_sysdict_by_oid(sysdict, oid, PG_PARSER_SYSDICT_PG_TYPE_TYPE);
    if (SYSDICT_INVALID_COUNT == count)
    {
        return false;
    }
    typinfo->typname = sysdict->m_pg_type.m_pg_type[count].typname.data;
    typinfo->typoutput_proname = getname_from_sysdict_by_oid(sysdict,
                                                             sysdict->m_pg_type.m_pg_type[count].typoutput,
                                                             PG_PARSER_SYSDICT_PG_PROC_TYPE);
    if (NULL == typinfo->typoutput_proname)
    {
        return false;
    }
    typinfo->typrelid = sysdict->m_pg_type.m_pg_type[count].typrelid;
    return true;
}

pg_parser_sysdict_pgtype* pg_parser_sysdict_getSysdictType(uint32_t oid, pg_parser_sysdicts* sysdict)
{
    int32_t count = SYSDICT_INVALID_COUNT;
    count = getcount_from_sysdict_by_oid(sysdict, oid, PG_PARSER_SYSDICT_PG_TYPE_TYPE);
    if (SYSDICT_INVALID_COUNT == count)
    {
        return NULL;
    }
    return &(sysdict->m_pg_type.m_pg_type[count]);
}

pg_parser_sysdict_pgtype* pg_parser_sysdict_getSubTypeByRange(uint32_t oid, pg_parser_sysdicts* sysdict)
{
    int32_t count = SYSDICT_INVALID_COUNT;
    count = getcount_from_sysdict_by_oid(sysdict, oid, PG_PARSER_SYSDICT_PG_RANGE_TYPE);
    if (SYSDICT_INVALID_COUNT == count)
    {
        return NULL;
    }
    return pg_parser_sysdict_getSysdictType(sysdict->m_pg_range.m_pg_range[count].rngsubtype, sysdict);
}

bool pg_parser_sysdict_getProcInfoByOid(uint32_t oid, pg_parser_sysdicts* sysdict, char** proname, char** nspname)
{
    int32_t count = SYSDICT_INVALID_COUNT;

    count = getcount_from_sysdict_by_oid(sysdict, oid, PG_PARSER_SYSDICT_PG_PROC_TYPE);
    if (SYSDICT_INVALID_COUNT == count)
    {
        return false;
    }
    *proname = sysdict->m_pg_proc.m_pg_proc[count].proname.data;
    *nspname = getname_from_sysdict_by_oid(sysdict,
                                           sysdict->m_pg_proc.m_pg_proc[count].pronamespace,
                                           PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE);
    return true;
}

bool pg_parser_sysdict_getEnumNameByOid(uint32_t oid, pg_parser_sysdicts* sysdict, char** enumname)
{
    *enumname = getname_from_sysdict_by_oid(sysdict, oid, PG_PARSER_SYSDICT_PG_ENUM_TYPE);
    if (!(*enumname))
    {
        return false;
    }
    else
    {
        return true;
    }
}

static int32_t getcount_from_pgclass_by_relfilenode(pg_parser_sysdicts* sysdict, uint32_t relfilenode)
{
    int32_t i = 0;
    for (i = 0; i < sysdict->m_pg_class.m_count; i++)
    {
        if (sysdict->m_pg_class.m_pg_class[i].relfilenode == relfilenode)
        {
            /* Found */
            return i;
        }
    }
    /* Not found */
    return SYSDICT_INVALID_COUNT;
}

/* Process records where only one record will match */
static char* getname_from_sysdict_by_oid(pg_parser_sysdicts* sysdict, uint32_t oid, pg_parser_sysdict_type sysdict_type)
{
    int32_t count = getcount_from_sysdict_by_oid(sysdict, oid, sysdict_type);
    if (count < 0)
    {
        return NULL;
    }

    switch (sysdict_type)
    {
        case PG_PARSER_SYSDICT_PG_CLASS_TYPE:
            return sysdict->m_pg_class.m_pg_class[count].relname.data;
            break;

        case PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE:
            return sysdict->m_pg_namespace.m_pg_namespace[count].nspname.data;
            break;

        case PG_PARSER_SYSDICT_PG_TYPE_TYPE:
            return sysdict->m_pg_type.m_pg_type[count].typname.data;
            break;

        case PG_PARSER_SYSDICT_PG_PROC_TYPE:
            return sysdict->m_pg_proc.m_pg_proc[count].proname.data;
            break;

        case PG_PARSER_SYSDICT_PG_ENUM_TYPE:
            return sysdict->m_pg_enum.m_pg_enum[count].enumlabel.data;
            break;
            /* range does not need to get name, attribute, constraint, enum may have multiple
             * values, not processed here */

        default:
            break;
    }
    return NULL;
}

static int32_t getcount_from_sysdict_by_oid(pg_parser_sysdicts*    sysdict,
                                            uint32_t               oid,
                                            pg_parser_sysdict_type sysdict_type)
{
    int32_t i = 0;

    switch (sysdict_type)
    {
        case PG_PARSER_SYSDICT_PG_CLASS_TYPE:
            for (i = 0; i < sysdict->m_pg_class.m_count; i++)
            {
                if (sysdict->m_pg_class.m_pg_class[i].oid == oid)
                {
                    return i;
                }
            }
            break;

        case PG_PARSER_SYSDICT_PG_NAMESPACE_TYPE:
            for (i = 0; i < sysdict->m_pg_namespace.m_count; i++)
            {
                if (sysdict->m_pg_namespace.m_pg_namespace[i].oid == oid)
                {
                    return i;
                }
            }
            break;

        case PG_PARSER_SYSDICT_PG_TYPE_TYPE:
            for (i = 0; i < sysdict->m_pg_type.m_count; i++)
            {
                if (sysdict->m_pg_type.m_pg_type[i].oid == oid)
                {
                    return i;
                }
            }
            break;

        case PG_PARSER_SYSDICT_PG_PROC_TYPE:
            for (i = 0; i < sysdict->m_pg_proc.m_count; i++)
            {
                if (sysdict->m_pg_proc.m_pg_proc[i].oid == oid)
                {
                    return i;
                }
            }
            break;

        case PG_PARSER_SYSDICT_PG_RANGE_TYPE:
            for (i = 0; i < sysdict->m_pg_range.m_count; i++)
            {
                if (sysdict->m_pg_range.m_pg_range[i].rngtypid == oid)
                {
                    return i;
                }
            }
            break;

        case PG_PARSER_SYSDICT_PG_ENUM_TYPE:
            for (i = 0; i < sysdict->m_pg_enum.m_count; i++)
            {
                if (sysdict->m_pg_enum.m_pg_enum[i].oid == oid)
                {
                    return i;
                }
            }
            break;

        default:
            break;
    }
    return SYSDICT_INVALID_COUNT;
}

static pg_parser_sysdict_pgattributes* get_attr_by_oid(pg_parser_sysdicts* sysdict, uint32_t oid, int16_t seqnum)
{
    int32_t i = 0;
    for (i = 0; i < sysdict->m_pg_attribute.m_count; i++)
    {
        if (sysdict->m_pg_attribute.m_pg_attributes[i].attrelid == oid &&
            sysdict->m_pg_attribute.m_pg_attributes[i].attnum == seqnum)
        {
            return &(sysdict->m_pg_attribute.m_pg_attributes[i]);
        }
    }
    return NULL;
}

uint32_t get_typrelid_by_typid(pg_parser_sysdicts* sysdict, uint32_t typid)
{
    int32_t count = SYSDICT_INVALID_COUNT;
    count = getcount_from_sysdict_by_oid(sysdict, typid, PG_PARSER_SYSDICT_PG_TYPE_TYPE);
    if (count < 0)
    {
        return pg_parser_InvalidOid;
    }
    return sysdict->m_pg_type.m_pg_type[count].typrelid;
}
