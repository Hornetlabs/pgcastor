#ifndef XK_PG_PARSER_SYSDICT_PG_PROC_H
#define XK_PG_PARSER_SYSDICT_PG_PROC_H

#define ProcedureRelationId 1255
#define ProcedureRelationIdChar "1255"
#define ProcedureRelation_Rowtype_Id 81

typedef struct XK_PG_PARSER_SYSDICT_PGPROC
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                    oid;
#endif
    xk_pg_parser_NameData       proname;
    uint32_t                    pronamespace;
    int16_t                     pronargs;
    /* 不按pg原生格式, 直接拿出数值组成固定长度的数组 */
    uint32_t                    proargtypes[XK_PG_PARSER_SYSDICT_PRIMARY_KEY_MAX];
} xk_pg_parser_sysdict_pgproc;

typedef xk_pg_parser_sysdict_pgproc *xk_pg_sysdict_Form_pg_proc;

#define XK_PG_SYSDICT_PROKIND_FUNCTION        'f'
#define XK_PG_SYSDICT_PROKIND_AGGREGATE 'a'
#define XK_PG_SYSDICT_PROKIND_WINDOW 'w'
#define XK_PG_SYSDICT_PROKIND_PROCEDURE 'p'

#define XK_PG_SYSDICT_PROVOLATILE_IMMUTABLE    'i' /* never changes for given input */
#define XK_PG_SYSDICT_PROVOLATILE_STABLE        's' /* does not change within a scan */
#define XK_PG_SYSDICT_PROVOLATILE_VOLATILE    'v' /* can change even within a scan */

#define XK_PG_SYSDICT_PROPARALLEL_SAFE        's' /* can run in worker or master */
#define XK_PG_SYSDICT_PROPARALLEL_RESTRICTED    'r' /* can run in parallel master only */
#define XK_PG_SYSDICT_PROPARALLEL_UNSAFE        'u' /* banned while in parallel mode */

#define XK_PG_SYSDICT_PROARGMODE_IN        'i'
#define XK_PG_SYSDICT_PROARGMODE_OUT        'o'
#define XK_PG_SYSDICT_PROARGMODE_INOUT    'b'
#define XK_PG_SYSDICT_PROARGMODE_VARIADIC 'v'
#define XK_PG_SYSDICT_PROARGMODE_TABLE    't'

#endif
