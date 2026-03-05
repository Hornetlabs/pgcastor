#ifndef XK_PG_PARSER_SYSDICT_PG_NAMESPACE_H
#define XK_PG_PARSER_SYSDICT_PG_NAMESPACE_H

#define NamespaceRelationId 2615
#define NamespaceRelationIdChar "2615"

typedef struct XK_PG_PARSER_SYSDICT_PGNAMESPACE
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                 oid;
#endif
    xk_pg_parser_NameData    nspname;
} xk_pg_parser_sysdict_pgnamespace;

typedef xk_pg_parser_sysdict_pgnamespace *xk_pg_sysdict_Form_pg_namespace;

#define PG_CATALOG_NAMESPACE 11
#define PG_TOAST_NAMESPACE 99
#define PG_TOAST_NAMESPACE_CHAR "99"
#define PG_PUBLIC_NAMESPACE 2200

#endif
