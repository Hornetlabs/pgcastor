#ifndef PG_PARSER_SYSDICT_GETINFO_H
#define PG_PARSER_SYSDICT_GETINFO_H

typedef struct pg_parser_sysdict_tableInfo
{
    char                                 relkind;
    bool                                 needzic;
    int16_t                              natts;
    uint32_t                             oid;
    uint32_t                             relfilenode;
    uint32_t                             toastoid;
    char                                *tbname;
    char                                *scname;
    pg_parser_sysdict_pgattributes  **pgattr;
} pg_parser_sysdict_tableInfo;
typedef struct pg_parser_sysdict_TypeInfo
{
    uint32_t                             typrelid;
    char                                *typname;
    char                                *typoutput_proname;
} pg_parser_sysdict_TypeInfo;
extern bool pg_parser_sysdict_getTableInfo_byoid(uint32_t record_relfilenode,
                                            pg_parser_sysdicts *sysdict,
                                            pg_parser_sysdict_tableInfo *tbinfo);
extern bool pg_parser_sysdict_getTableInfo(uint32_t record_relfilenode,
                                              pg_parser_sysdicts *sysdict,
                                              pg_parser_sysdict_tableInfo *tbinfo);

extern bool pg_parser_sysdict_getTypeInfo(uint32_t oid,
                                             pg_parser_sysdicts *sysdict,
                                             pg_parser_sysdict_TypeInfo *typinfo);

extern pg_parser_sysdict_pgtype *pg_parser_sysdict_getSysdictType(uint32_t oid,
                                                                        pg_parser_sysdicts *sysdict);

extern pg_parser_sysdict_pgtype *pg_parser_sysdict_getSubTypeByRange(uint32_t oid,
                                                 pg_parser_sysdicts *sysdict);

extern bool pg_parser_sysdict_getProcInfoByOid(uint32_t oid,
                                                  pg_parser_sysdicts *sysdict,
                                                  char **proname,
                                                  char **nspname);

extern bool pg_parser_sysdict_getEnumNameByOid(uint32_t oid,
                                                  pg_parser_sysdicts *sysdict,
                                                  char **enumname);
extern uint32_t get_typrelid_by_typid(pg_parser_sysdicts *sysdict,
                                      uint32_t typid);

#if 0
bool pg_parser_sysdict_getTypeNameByOid(uint32_t oid,
                                          pg_parser_sysdict_pgtype_dict *sysdict_pg_type,
                                          char **typname);

extern bool pg_parser_sysdict_getRelNameByOid(uint32_t oid,
                                                  pg_parser_sysdict_pgclass_dict *sysdict_pg_class,
                                                  char **relname);

extern bool pg_parser_sysdict_getNspidByRelid(uint32_t relid,
                                                 pg_parser_sysdict_pgclass_dict *sysdict_pg_class,
                                                 uint32_t *nspid);

extern bool pg_parser_sysdict_getNspNameByOid(uint32_t oid,
                                                 pg_parser_sysdict_pgnamespace_dict *sysdict_pg_namespace,
                                                 char **nspname);
#endif
#endif
