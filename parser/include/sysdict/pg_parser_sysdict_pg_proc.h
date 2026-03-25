#ifndef PG_PARSER_SYSDICT_PG_PROC_H
#define PG_PARSER_SYSDICT_PG_PROC_H

#define ProcedureRelationId 1255
#define ProcedureRelationIdChar "1255"
#define ProcedureRelation_Rowtype_Id 81

typedef struct PG_PARSER_SYSDICT_PGPROC
{
    uint32_t           oid;
    pg_parser_NameData proname;
    uint32_t           pronamespace;
    int16_t            pronargs;
    /* Not following pg native format, directly extract values to form fixed-length array */
    uint32_t proargtypes[PG_PARSER_SYSDICT_PRIMARY_KEY_MAX];
} pg_parser_sysdict_pgproc;

typedef pg_parser_sysdict_pgproc* pg_sysdict_Form_pg_proc;

#define PG_SYSDICT_PROKIND_FUNCTION 'f'
#define PG_SYSDICT_PROKIND_AGGREGATE 'a'
#define PG_SYSDICT_PROKIND_WINDOW 'w'
#define PG_SYSDICT_PROKIND_PROCEDURE 'p'

#define PG_SYSDICT_PROVOLATILE_IMMUTABLE 'i' /* never changes for given input */
#define PG_SYSDICT_PROVOLATILE_STABLE 's'    /* does not change within a scan */
#define PG_SYSDICT_PROVOLATILE_VOLATILE 'v'  /* can change even within a scan */

#define PG_SYSDICT_PROPARALLEL_SAFE 's'       /* can run in worker or master */
#define PG_SYSDICT_PROPARALLEL_RESTRICTED 'r' /* can run in parallel master only */
#define PG_SYSDICT_PROPARALLEL_UNSAFE 'u'     /* banned while in parallel mode */

#define PG_SYSDICT_PROARGMODE_IN 'i'
#define PG_SYSDICT_PROARGMODE_OUT 'o'
#define PG_SYSDICT_PROARGMODE_INOUT 'b'
#define PG_SYSDICT_PROARGMODE_VARIADIC 'v'
#define PG_SYSDICT_PROARGMODE_TABLE 't'

#endif
