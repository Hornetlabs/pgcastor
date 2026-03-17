#ifndef PG_PARSER_SYSDICT_PG_CONSTRAINT_H
#define PG_PARSER_SYSDICT_PG_CONSTRAINT_H

typedef struct PG_PARSER_SYSDICT_PGCONSTRAINT
{
    uint32_t                    oid;
    pg_parser_NameData       conname;
    uint32_t                    connamespace;
    char                        contype;
    uint32_t                    conrelid;
    int16_t                     conkeycnt;
    int16_t                     *conkey;
} pg_parser_sysdict_pgconstraint;

typedef pg_parser_sysdict_pgconstraint *pg_sysdict_Form_pg_constraint;

#define CONSTRAINT_CHECK                'c'
#define CONSTRAINT_FOREIGN              'f'
#define CONSTRAINT_PRIMARY              'p'
#define CONSTRAINT_UNIQUE               'u'
#define CONSTRAINT_TRIGGER              't'
#define CONSTRAINT_EXCLUSION            'x'

typedef enum pg_sysdict_ConstraintCategory
{
    PG_SYSDICT_CONSTRAINT_RELATION,
    PG_SYSDICT_CONSTRAINT_DOMAIN,
    PG_SYSDICT_CONSTRAINT_ASSERTION
} pg_sysdict_ConstraintCategory;

#endif
