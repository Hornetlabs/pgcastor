#ifndef XK_PG_PARSER_SYSDICT_PG_DATABASE_H
#define XK_PG_PARSER_SYSDICT_PG_DATABASE_H


#define DatabaseRelationId 1262

typedef struct XK_PG_PARSER_SYSDICT_PGDATABASE
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                 oid;
#endif
    xk_pg_parser_NameData    datname;
    uint32_t                 datdba;
    int32_t                  encoding;
    xk_pg_parser_NameData    datcollate;
    xk_pg_parser_NameData    datctype;
} xk_pg_parser_sysdict_pgdatabase;

typedef xk_pg_parser_sysdict_pgdatabase *xk_pg_sysdict_Form_pg_database;


#endif
