#ifndef XK_PG_PARSER_SYSDICT_PG_INDEX_H
#define XK_PG_PARSER_SYSDICT_PG_INDEX_H

#define IndexRelationId 2610

typedef struct XK_PG_PARSER_SYSDICT_PGINDEX
{
    uint32_t    indrelid;       /* 表的oid */
    uint32_t    indexrelid;     /* 索引oid */
    bool        indisprimary;   /* 索引是否为主键产生的索引 */
    bool        indisreplident; /* 是否为replica identify index指定的索引 */
    uint32_t    indnatts;       /* 索引列个数 */
    uint32_t*   indkey;         /* 索引列 */
} xk_pg_parser_sysdict_pgindex;

typedef xk_pg_parser_sysdict_pgindex *xk_pg_sysdict_Form_pg_index;

#endif
