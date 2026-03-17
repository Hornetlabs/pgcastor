#ifndef XK_PG_PARSER_SYSDICT_PG_AUTHID_H
#define XK_PG_PARSER_SYSDICT_PG_AUTHID_H

#define AuthIdRelationId 1260


typedef struct XK_PG_PARSER_SYSDICT_PGAUTHID
{
    uint32_t                oid;
    xk_pg_parser_NameData   rolname;
    bool                    rolsuper;
    bool                    rolinherit;
    bool                    rolcreaterole;
    bool                    rolcreatedb;
    bool                    rolcanlogin;
    bool                    rolreplication;
    bool                    rolbypassrls;
    int32_t                 rolconnlimit;
} xk_pg_parser_sysdict_pgauthid;
typedef xk_pg_parser_sysdict_pgauthid *xk_pg_sysdict_Form_pg_authid;

#endif

