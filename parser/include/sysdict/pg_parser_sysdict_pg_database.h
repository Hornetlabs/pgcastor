#ifndef PG_PARSER_SYSDICT_PG_DATABASE_H
#define PG_PARSER_SYSDICT_PG_DATABASE_H


#define DatabaseRelationId 1262

typedef struct PG_PARSER_SYSDICT_PGDATABASE
{
    uint32_t                 oid;
    pg_parser_NameData    datname;
    uint32_t                 datdba;
    int32_t                  encoding;
    pg_parser_NameData    datcollate;
    pg_parser_NameData    datctype;
} pg_parser_sysdict_pgdatabase;

typedef pg_parser_sysdict_pgdatabase *pg_sysdict_Form_pg_database;


#endif
