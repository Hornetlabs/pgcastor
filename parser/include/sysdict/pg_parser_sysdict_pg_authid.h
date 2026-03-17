#ifndef PG_PARSER_SYSDICT_PG_AUTHID_H
#define PG_PARSER_SYSDICT_PG_AUTHID_H

#define AuthIdRelationId 1260


typedef struct PG_PARSER_SYSDICT_PGAUTHID
{
    uint32_t                oid;
    pg_parser_NameData   rolname;
    bool                    rolsuper;
    bool                    rolinherit;
    bool                    rolcreaterole;
    bool                    rolcreatedb;
    bool                    rolcanlogin;
    bool                    rolreplication;
    bool                    rolbypassrls;
    int32_t                 rolconnlimit;
} pg_parser_sysdict_pgauthid;
typedef pg_parser_sysdict_pgauthid *pg_sysdict_Form_pg_authid;

#endif

