#ifndef PG_PARSER_SYSDICT_PG_NAMESPACE_H
#define PG_PARSER_SYSDICT_PG_NAMESPACE_H

#define NamespaceRelationId     2615
#define NamespaceRelationIdChar "2615"

typedef struct PG_PARSER_SYSDICT_PGNAMESPACE
{
    uint32_t           oid;
    pg_parser_NameData nspname;
} pg_parser_sysdict_pgnamespace;

typedef pg_parser_sysdict_pgnamespace* pg_sysdict_Form_pg_namespace;

#define PG_CATALOG_NAMESPACE    11
#define PG_TOAST_NAMESPACE      99
#define PG_TOAST_NAMESPACE_CHAR "99"
#define PG_PUBLIC_NAMESPACE     2200

#endif
