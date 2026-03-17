#ifndef XK_PG_PARSER_SYSDICT_PG_CONSTRAINT_H
#define XK_PG_PARSER_SYSDICT_PG_CONSTRAINT_H

typedef struct XK_PG_PARSER_SYSDICT_PGCONSTRAINT
{
#if XK_PG_VERSION_NUM >= 120000
    uint32_t                    oid;
#endif

    xk_pg_parser_NameData       conname;
    uint32_t                    connamespace;
    char                        contype;
    uint32_t                    conrelid;
    int16_t                     conkeycnt;
    int16_t                     *conkey;
} xk_pg_parser_sysdict_pgconstraint;

typedef xk_pg_parser_sysdict_pgconstraint *xk_pg_sysdict_Form_pg_constraint;

#define CONSTRAINT_CHECK                'c'
#define CONSTRAINT_FOREIGN              'f'
#define CONSTRAINT_PRIMARY              'p'
#define CONSTRAINT_UNIQUE               'u'
#define CONSTRAINT_TRIGGER              't'
#define CONSTRAINT_EXCLUSION            'x'

typedef enum xk_pg_sysdict_ConstraintCategory
{
    XK_PG_SYSDICT_CONSTRAINT_RELATION,
    XK_PG_SYSDICT_CONSTRAINT_DOMAIN,
    XK_PG_SYSDICT_CONSTRAINT_ASSERTION
} xk_pg_sysdict_ConstraintCategory;

#endif
