#ifndef PG_PARSER_THIRDPARTY_PARSERNODE_STRUCT_H
#define PG_PARSER_THIRDPARTY_PARSERNODE_STRUCT_H

#include <sys/types.h>
#include <signal.h>
#include <time.h>
typedef int32_t pg_parser_File;

#define Min(x, y) ((x) < (y) ? (x) : (y))

/* Different from pg_class under sysdict, here pg_class structure is complete */
typedef struct pg_parser_FormData_pg_class
{
    /* oid */
    uint32_t uint32_t;

    /* class name */
    pg_parser_NameData relname;

    /* OID of namespace containing this class */
    uint32_t relnamespace;

    /* OID of entry in pg_type for table's implicit row type */
    uint32_t reltype;

    /* OID of entry in pg_type for underlying composite type */
    uint32_t reloftype;

    /* class owner */
    uint32_t relowner;

    /* access method; 0 if not a table / index */
    uint32_t relam;

    /* identifier of physical storage file */
    /* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
    uint32_t relfilenode;

    /* identifier of table space for relation (0 means default for database) */
    uint32_t reltablespace;

    /* # of blocks (not always up-to-date) */
    int32_t relpages;

    /* # of tuples (not always up-to-date) */
    float reltuples;

    /* # of all-visible blocks (not always up-to-date) */
    int32_t relallvisible;

    /* OID of toast table; 0 if none */
    uint32_t reltoastrelid;

    /* T if has (or has had) any indexes */
    bool relhasindex;

    /* T if shared across databases */
    bool relisshared;

    /* see RELPERSISTENCE_xxx constants below */
    char relpersistence;

    /* see RELKIND_xxx constants below */
    char relkind;

    /* number of user attributes */
    int16_t relnatts;

    /*
     * Class pg_attribute must contain exactly "relnatts" user attributes
     * (with attnums ranging from 1 to relnatts) for this class.  It may also
     * contain entries with negative attnums for system attributes.
     */

    /* # of CHECK constraints for class */
    int16_t relchecks;

    /* has (or has had) any rules */
    bool relhasrules;

    /* has (or has had) any TRIGGERs */
    bool relhastriggers;

    /* has (or has had) child tables or indexes */
    bool relhassubclass;

    /* row security is enabled or not */
    bool relrowsecurity;

    /* row security forced for owners or not */
    bool relforcerowsecurity;

    /* matview currently holds query results */
    bool relispopulated;

    /* see REPLICA_IDENTITY_xxx constants */
    char relreplident;

    /* is relation a partition? */
    bool relispartition;

    /* heap for rewrite during DDL, link to original rel */
    uint32_t relrewrite;

    /* all Xids < this are frozen in this rel */
    pg_parser_TransactionId relfrozenxid;

    /* all multixacts in this rel are >= this; it is really a MultiXactId */
    pg_parser_TransactionId relminmxid;
} pg_parser_FormData_pg_class;

typedef struct
{
    int32_t  vl_len_;    /* these fields must match ArrayType! */
    int32_t  ndim;       /* always 1 for int2vector */
    int32_t  dataoffset; /* always 0 for int2vector */
    uint32_t elemtype;
    int32_t  dim1;
    int32_t  lbound1;
    int16_t  values[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_int2vector;

typedef struct pg_parser_FormData_pg_index
{
    uint32_t indexrelid;     /* OID of the index */
    uint32_t indrelid;       /* OID of the relation it indexes */
    int16_t  indnatts;       /* total number of columns in index */
    int16_t  indnkeyatts;    /* number of key columns in index */
    bool     indisunique;    /* is this a unique index? */
    bool     indisprimary;   /* is this index for primary key? */
    bool     indisexclusion; /* is this index for exclusion constraint? */
    bool     indimmediate;   /* is uniqueness enforced immediately? */
    bool     indisclustered; /* is this the index last clustered by? */
    bool     indisvalid;     /* is this index valid for use by queries? */
    bool     indcheckxmin;   /* must we wait for xmin to be old? */
    bool     indisready;     /* is this index ready for inserts? */
    bool     indislive;      /* is this index alive at all? */
    bool     indisreplident; /* is this index the identity for replication? */

    /* variable-length fields start here, but we allow direct access to indkey */
    pg_parser_int2vector indkey; /* column numbers of indexed cols, or 0 */
} pg_parser_FormData_pg_index;

/* Bitmapset */
typedef uint64_t pg_parser_bitmapword; /* must be an unsigned type */
typedef struct pg_parser_Bitmapset
{
    int32_t              nwords;                       /* number of words in array */
    pg_parser_bitmapword words[FLEXIBLE_ARRAY_MEMBER]; /* really [nwords] */
} pg_parser_Bitmapset;

/* Query */

typedef enum pg_parser_CmdType
{
    CMD_UNKNOWN,
    CMD_SELECT, /* select stmt */
    CMD_UPDATE, /* update stmt */
    CMD_INSERT, /* insert stmt */
    CMD_DELETE,
    CMD_UTILITY, /* cmds like create, destroy, copy, vacuum,
                  * etc. */
    CMD_NOTHING  /* dummy command for instead nothing rules
                  * with qual */
} pg_parser_CmdType;

typedef enum pg_parser_QuerySource
{
    QSRC_ORIGINAL,          /* original parsetree (explicit query) */
    QSRC_PARSER,            /* added by parse analysis (now unused) */
    QSRC_INSTEAD_RULE,      /* added by unconditional INSTEAD rule */
    QSRC_QUAL_INSTEAD_RULE, /* added by conditional INSTEAD rule */
    QSRC_NON_INSTEAD_RULE   /* added by non-INSTEAD rule */
} pg_parser_QuerySource;

typedef struct pg_parser_FromExpr
{
    pg_parser_NodeTag type;
    pg_parser_List*   fromlist; /* pg_parser_List of join subtrees */
    pg_parser_Node*   quals;    /* qualifiers on join, if any */
} pg_parser_FromExpr;

typedef enum pg_parser_OverridingKind
{
    OVERRIDING_NOT_SET = 0,
    OVERRIDING_USER_VALUE,
    OVERRIDING_SYSTEM_VALUE
} pg_parser_OverridingKind;

typedef enum pg_parser_OnConflictAction
{
    ONCONFLICT_NONE,    /* No "ON CONFLICT" clause */
    ONCONFLICT_NOTHING, /* ON CONFLICT ... DO NOTHING */
    ONCONFLICT_UPDATE   /* ON CONFLICT ... DO UPDATE */
} pg_parser_OnConflictAction;

typedef struct pg_parser_OnConflictExpr
{
    pg_parser_NodeTag          type;
    pg_parser_OnConflictAction action; /* DO NOTHING or UPDATE? */

    /* Arbiter */
    pg_parser_List* arbiterElems; /* unique index arbiter list (of
                                   * InferenceElem's) */
    pg_parser_Node* arbiterWhere; /* unique index arbiter WHERE clause */
    uint32_t        constraint;   /* pg_constraint OID for arbiter */

    /* ON CONFLICT UPDATE */
    pg_parser_List* onConflictSet;   /* pg_parser_List of ON CONFLICT SET TargetEntrys */
    pg_parser_Node* onConflictWhere; /* qualifiers to restrict UPDATE to */
    int32_t         exclRelIndex;    /* RT index of 'excluded' relation */
    pg_parser_List* exclRelTlist;    /* tlist of the EXCLUDED pseudo relation */
} pg_parser_OnConflictExpr;

typedef struct pg_parser_Query
{
    pg_parser_NodeTag type;

    pg_parser_CmdType commandType; /* select|insert|update|delete|utility */

    pg_parser_QuerySource querySource; /* where did I come from? */

    uint64_t queryId; /* query identifier (can be set by plugins) */

    bool canSetTag; /* do I set the command result tag? */

    pg_parser_Node* utilityStmt; /* non-null if commandType == CMD_UTILITY */

    int32_t resultRelation; /* rtable index of target relation for
                             * INSERT/UPDATE/DELETE; 0 for SELECT */

    bool hasAggs;         /* has aggregates in tlist or havingQual */
    bool hasWindowFuncs;  /* has window functions in tlist */
    bool hasTargetSRFs;   /* has set-returning functions in tlist */
    bool hasSubLinks;     /* has subquery SubLink */
    bool hasDistinctOn;   /* distinctClause is from DISTINCT ON */
    bool hasRecursive;    /* WITH RECURSIVE was specified */
    bool hasModifyingCTE; /* has INSERT/UPDATE/DELETE in WITH */
    bool hasForUpdate;    /* FOR [KEY] UPDATE/SHARE was specified */
    bool hasRowSecurity;  /* rewriter has applied some RLS policy */

    pg_parser_List* cteList; /* WITH list (of pg_parser_CommonTableExpr's) */

    pg_parser_List*     rtable;   /* list of range table entries */
    pg_parser_FromExpr* jointree; /* table join tree (FROM and WHERE clauses) */

    pg_parser_List* targetList; /* target list (of TargetEntry) */

    pg_parser_OverridingKind override; /* OVERRIDING clause */

    pg_parser_OnConflictExpr* onConflict; /* ON CONFLICT DO [NOTHING | UPDATE] */

    pg_parser_List* returningList; /* return-values list (of TargetEntry) */

    pg_parser_List* groupClause; /* a list of pg_parser_SortGroupClause's */

    pg_parser_List* groupingSets; /* a list of pg_parser_GroupingSet's if present */

    pg_parser_Node* havingQual; /* qualifications applied to groups */

    pg_parser_List* windowClause; /* a list of pg_parser_WindowClause's */

    pg_parser_List* distinctClause; /* a list of pg_parser_SortGroupClause's */

    pg_parser_List* sortClause; /* a list of pg_parser_SortGroupClause's */

    pg_parser_Node* limitOffset; /* # of result tuples to skip (int8 expr) */
    pg_parser_Node* limitCount;  /* # of result tuples to return (int8 expr) */

    pg_parser_List* rowMarks; /* a list of pg_parser_RowMarkClause's */

    pg_parser_Node* setOperations; /* set-operation tree if this is top level of
                                    * a UNION/INTERSECT/EXCEPT query */

    pg_parser_List* constraintDeps; /* a list of pg_constraint OIDs that the query
                                     * depends on to be semantically valid */

    pg_parser_List* withCheckOptions; /* a list of pg_parser_WithCheckOption's (added
                                       * during rewrite) */

    /*
     * The following two fields identify the portion of the source text string
     * containing this query.  They are typically only populated in top-level
     * Queries, not in sub-queries.  When not set, they might both be zero, or
     * both be -1 meaning "unknown".
     */
    int32_t stmt_location; /* start location, or -1 if unknown */
    int32_t stmt_len;      /* length in bytes; 0 means "rest of string" */
} pg_parser_Query;

/* NotifyStmt */

typedef struct pg_parser_NotifyStmt
{
    pg_parser_NodeTag type;
    char*             conditionname; /* condition name to notify */
    char*             payload;       /* the payload string, or NULL if none */
} pg_parser_NotifyStmt;

/* DeclareCursorStmt */
typedef struct pg_parser_DeclareCursorStmt
{
    pg_parser_NodeTag type;
    char*             portalname; /* name of the portal (cursor) */
    int32_t           options;    /* bitmask of options (see above) */
    pg_parser_Node*   query;      /* the query (see comments above) */
} pg_parser_DeclareCursorStmt;

/* WithCheckOption */

typedef enum pg_parser_WCOKind
{
    WCO_VIEW_CHECK,        /* WCO on an auto-updatable view */
    WCO_RLS_INSERT_CHECK,  /* RLS INSERT WITH CHECK policy */
    WCO_RLS_UPDATE_CHECK,  /* RLS UPDATE WITH CHECK policy */
    WCO_RLS_CONFLICT_CHECK /* RLS ON CONFLICT DO UPDATE USING policy */
} pg_parser_WCOKind;

typedef struct pg_parser_WithCheckOption
{
    pg_parser_NodeTag type;
    pg_parser_WCOKind kind;     /* kind of WCO */
    char*             relname;  /* name of relation that specified the WCO */
    char*             polname;  /* name of RLS policy being checked */
    pg_parser_Node*   qual;     /* constraint qual to check */
    bool              cascaded; /* true for a cascaded WCO on a view */
} pg_parser_WithCheckOption;

/* SortGroupClause */
typedef uint32_t pg_parser_Index;

typedef struct pg_parser_SortGroupClause
{
    pg_parser_NodeTag type;
    pg_parser_Index   tleSortGroupRef; /* reference into targetlist */
    uint32_t          eqop;            /* the equality operator ('=' op) */
    uint32_t          sortop;          /* the ordering operator ('<' op), or 0 */
    bool              nulls_first;     /* do NULLs come before normal values? */
    bool              hashable;        /* can eqop be implemented by hashing? */
} pg_parser_SortGroupClause;

/* GroupingSet */
typedef enum
{
    GROUPING_SET_EMPTY,
    GROUPING_SET_SIMPLE,
    GROUPING_SET_ROLLUP,
    GROUPING_SET_CUBE,
    GROUPING_SET_SETS
} pg_parser_GroupingSetKind;

typedef struct pg_parser_GroupingSet
{
    pg_parser_NodeTag         type;
    pg_parser_GroupingSetKind kind;
    pg_parser_List*           content;
    int32_t                   location;
} pg_parser_GroupingSet;

/* WindowClause */
typedef struct pg_parser_WindowClause
{
    pg_parser_NodeTag type;
    char*             name;              /* window name (NULL in an OVER clause) */
    char*             refname;           /* referenced window name, if any */
    pg_parser_List*   partitionClause;   /* PARTITION BY list */
    pg_parser_List*   orderClause;       /* ORDER BY list */
    int32_t           frameOptions;      /* frame_clause options, see WindowDef */
    pg_parser_Node*   startOffset;       /* expression for starting bound, if any */
    pg_parser_Node*   endOffset;         /* expression for ending bound, if any */
    uint32_t          startInRangeFunc;  /* in_range function for startOffset */
    uint32_t          endInRangeFunc;    /* in_range function for endOffset */
    uint32_t          inRangeColl;       /* collation for in_range tests */
    bool              inRangeAsc;        /* use ASC sort order for in_range tests? */
    bool              inRangeNullsFirst; /* nulls sort first for in_range tests? */
    pg_parser_Index   winref;            /* ID referenced by window functions */
    bool              copiedOrder;       /* did we copy orderClause from refname? */
} pg_parser_WindowClause;

/* RowMarkClause */

typedef enum pg_parser_LockClauseStrength
{
    LCS_NONE,           /* no such clause - only used in PlanRowMark */
    LCS_FORKEYSHARE,    /* FOR KEY SHARE */
    LCS_FORSHARE,       /* FOR SHARE */
    LCS_FORNOKEYUPDATE, /* FOR NO KEY UPDATE */
    LCS_FORUPDATE       /* FOR UPDATE */
} pg_parser_LockClauseStrength;

typedef enum pg_parser_LockWaitPolicy
{
    /* Wait for the lock to become available (default behavior) */
    LockWaitBlock,
    /* Skip rows that can't be locked (SKIP LOCKED) */
    LockWaitSkip,
    /* Raise an error if a row cannot be locked (NOWAIT) */
    LockWaitError
} pg_parser_LockWaitPolicy;

typedef struct pg_parser_RowMarkClause
{
    pg_parser_NodeTag            type;
    pg_parser_Index              rti; /* range table index of target relation */
    pg_parser_LockClauseStrength strength;
    pg_parser_LockWaitPolicy     waitPolicy; /* NOWAIT and SKIP LOCKED */
    bool                         pushedDown; /* pushed down from higher query level? */
} pg_parser_RowMarkClause;

/* CommonTableExpr */

typedef enum pg_parser_CTEMaterialize
{
    CTEMaterializeDefault, /* no option specified */
    CTEMaterializeAlways,  /* MATERIALIZED */
    CTEMaterializeNever    /* NOT MATERIALIZED */
} pg_parser_CTEMaterialize;

typedef struct pg_parser_CommonTableExpr
{
    pg_parser_NodeTag        type;
    char*                    ctename;         /* query name (never qualified) */
    pg_parser_List*          aliascolnames;   /* optional list of column names */
    pg_parser_CTEMaterialize ctematerialized; /* is this an optimization fence? */
    /* SelectStmt/InsertStmt/etc before parse analysis, Query afterwards: */
    pg_parser_Node* ctequery; /* the CTE's subquery */
    int32_t         location; /* token location, or -1 if unknown */
    /* These fields are set during parse analysis: */
    bool    cterecursive;             /* is this CTE actually recursive? */
    int32_t cterefcount;              /* number of RTEs referencing this CTE
                                       * (excluding internal self-references) */
    pg_parser_List* ctecolnames;      /* list of output column names */
    pg_parser_List* ctecoltypes;      /* OID list of output column type OIDs */
    pg_parser_List* ctecoltypmods;    /* integer list of output column typmods */
    pg_parser_List* ctecolcollations; /* OID list of column collation OIDs */
} pg_parser_CommonTableExpr;

/* SetOperationStmt */

typedef enum pg_parser_SetOperation
{
    SETOP_NONE = 0,
    SETOP_UNION,
    SETOP_INTERSECT,
    SETOP_EXCEPT
} pg_parser_SetOperation;

typedef struct pg_parser_SetOperationStmt
{
    pg_parser_NodeTag      type;
    pg_parser_SetOperation op;   /* type of set op */
    bool                   all;  /* ALL specified? */
    pg_parser_Node*        larg; /* left child */
    pg_parser_Node*        rarg; /* right child */
    /* Eventually add fields for CORRESPONDING spec here */

    /* Fields derived during parse analysis: */
    pg_parser_List* colTypes;      /* OID list of output column type OIDs */
    pg_parser_List* colTypmods;    /* integer list of output column typmods */
    pg_parser_List* colCollations; /* OID list of output column collation OIDs */
    pg_parser_List* groupClauses;  /* a list of SortGroupClause's */
    /* groupClauses is NIL if UNION ALL, but must be set otherwise */
} pg_parser_SetOperationStmt;

/* Alias */
typedef struct pg_parser_Alias
{
    pg_parser_NodeTag type;
    char*             aliasname; /* aliased rel name (never qualified) */
    pg_parser_List*   colnames;  /* optional list of column aliases */
} pg_parser_Alias;

/* RangeVar */
typedef struct pg_parser_RangeVar
{
    pg_parser_NodeTag type;
    char*             catalogname;   /* the catalog (database) name, or NULL */
    char*             schemaname;    /* the schema name, or NULL */
    char*             relname;       /* the relation/sequence name */
    bool              inh;           /* expand rel by inheritance? recursively act
                                      * on children? */
    char             relpersistence; /* see RELPERSISTENCE_* in pg_class.h */
    pg_parser_Alias* alias;          /* table alias & optional column aliases */
    int32_t          location;       /* token location, or -1 if unknown */
} pg_parser_RangeVar;

/* TableFunc */
typedef struct pg_parser_TableFunc
{
    pg_parser_NodeTag    type;
    pg_parser_List*      ns_uris;       /* list of namespace URI expressions */
    pg_parser_List*      ns_names;      /* list of namespace names or NULL */
    pg_parser_Node*      docexpr;       /* input document expression */
    pg_parser_Node*      rowexpr;       /* row filter expression */
    pg_parser_List*      colnames;      /* column names (list of String) */
    pg_parser_List*      coltypes;      /* OID list of column type OIDs */
    pg_parser_List*      coltypmods;    /* integer list of column typmods */
    pg_parser_List*      colcollations; /* OID list of column collation OIDs */
    pg_parser_List*      colexprs;      /* list of column filter expressions */
    pg_parser_List*      coldefexprs;   /* list of column default expressions */
    pg_parser_Bitmapset* notnulls;      /* nullability flag for each output column */
    int32_t              ordinalitycol; /* counts from 0; -1 if none specified */
    int32_t              location;      /* token location, or -1 if unknown */
} pg_parser_TableFunc;

/* IntoClause */
typedef enum pg_parser_OnCommitAction
{
    ONCOMMIT_NOOP,          /* No ON COMMIT clause (do nothing) */
    ONCOMMIT_PRESERVE_ROWS, /* ON COMMIT PRESERVE ROWS (do nothing) */
    ONCOMMIT_DELETE_ROWS,   /* ON COMMIT DELETE ROWS */
    ONCOMMIT_DROP           /* ON COMMIT DROP */
} pg_parser_OnCommitAction;

typedef struct pg_parser_IntoClause
{
    pg_parser_NodeTag type;

    pg_parser_RangeVar*      rel;            /* target relation name */
    pg_parser_List*          colNames;       /* column names to assign, or NIL */
    char*                    accessMethod;   /* table access method */
    pg_parser_List*          options;        /* options from WITH clause */
    pg_parser_OnCommitAction onCommit;       /* what do we do at COMMIT? */
    char*                    tableSpaceName; /* table space to use, or NULL */
    pg_parser_Node*          viewQuery;      /* materialized view's SELECT query */
    bool                     skipData;       /* true for WITH NO DATA */
} pg_parser_IntoClause;

/* pg_parser_Var */

typedef struct pg_parser_Expr
{
    pg_parser_NodeTag type;
} pg_parser_Expr;

typedef int16_t pg_parser_AttrNumber;

typedef struct pg_parser_Var
{
    pg_parser_Expr  xpr;
    pg_parser_Index varno;          /* index of this var's relation in the range
                                     * table, or INNER_VAR/OUTER_VAR/INDEX_VAR */
    pg_parser_AttrNumber varattno;  /* attribute number of this var, or zero for
                                     * all attrs ("whole-row pg_parser_Var") */
    uint32_t        vartype;        /* pg_type OID for the type of this var */
    int32_t         vartypmod;      /* pg_attribute typmod value */
    uint32_t        varcollid;      /* OID of collation, or INVALIDOID if none */
    pg_parser_Index varlevelsup;    /* for subquery variables referencing outer
                                     * relations; 0 in a normal var, >0 means N
                                     * levels up */
    pg_parser_Index      varnoold;  /* original value of varno, for debugging */
    pg_parser_AttrNumber varoattno; /* original value of varattno */
    int32_t              location;  /* token location, or -1 if unknown */
} pg_parser_Var;

/* Const */
typedef struct pg_parser_Const
{
    pg_parser_Expr  xpr;
    uint32_t        consttype;   /* pg_type OID of the constant's datatype */
    int32_t         consttypmod; /* typmod value, if any */
    uint32_t        constcollid; /* OID of collation, or INVALIDOID if none */
    int32_t         constlen;    /* typlen of the constant's datatype */
    pg_parser_Datum constvalue;  /* the constant's value */
    bool            constisnull; /* whether the constant is null (if true,
                                  * constvalue is undefined) */
    bool constbyval;             /* whether this datatype is passed by value.
                                  * If true, then all the information is stored
                                  * in the Datum. If false, then the Datum
                                  * contains a pointer to the information. */
    bool    constneedfree;
    int32_t location; /* token location, or -1 if unknown */
} pg_parser_Const;

/* Param */
typedef enum pg_parser_ParamKind
{
    PARAM_EXTERN,
    PARAM_EXEC,
    PARAM_SUBLINK,
    PARAM_MULTIEXPR
} pg_parser_ParamKind;

typedef struct pg_parser_Param
{
    pg_parser_Expr      xpr;
    pg_parser_ParamKind paramkind;   /* kind of parameter. See above */
    int32_t             paramid;     /* numeric ID for parameter */
    uint32_t            paramtype;   /* pg_type OID of parameter's datatype */
    int32_t             paramtypmod; /* typmod value, if known */
    uint32_t            paramcollid; /* OID of collation, or INVALIDOID if none */
    int32_t             location;    /* token location, or -1 if unknown */
} pg_parser_Param;

/* Aggref */
#define PG_PARSER_AGGSPLITOP_COMBINE 0x01     /* substitute combinefn for transfn */
#define PG_PARSER_AGGSPLITOP_SKIPFINAL 0x02   /* skip finalfn, return state as-is */
#define PG_PARSER_AGGSPLITOP_SERIALIZE 0x04   /* apply serializefn to output */
#define PG_PARSER_AGGSPLITOP_DESERIALIZE 0x08 /* apply deserializefn to input */

typedef enum pg_parser_AggSplit
{
    /* Basic, non-split aggregation: */
    AGGSPLIT_SIMPLE = 0,
    /* Initial phase of partial aggregation, with serialization: */
    AGGSPLIT_INITIAL_SERIAL = PG_PARSER_AGGSPLITOP_SKIPFINAL | PG_PARSER_AGGSPLITOP_SERIALIZE,
    /* Final phase of partial aggregation, with deserialization: */
    AGGSPLIT_FINAL_DESERIAL = PG_PARSER_AGGSPLITOP_COMBINE | PG_PARSER_AGGSPLITOP_DESERIALIZE
} pg_parser_AggSplit;

typedef struct pg_parser_Aggref
{
    pg_parser_Expr  xpr;
    uint32_t        aggfnoid;       /* pg_proc uint32_t of the aggregate */
    uint32_t        aggtype;        /* type uint32_t of result of the aggregate */
    uint32_t        aggcollid;      /* OID of collation of result */
    uint32_t        inputcollid;    /* OID of collation that function should use */
    uint32_t        aggtranstype;   /* type uint32_t of aggregate's transition value */
    pg_parser_List* aggargtypes;    /* type Oids of direct and aggregated args */
    pg_parser_List* aggdirectargs;  /* direct arguments, if an ordered-set agg */
    pg_parser_List* args;           /* aggregated arguments and sort expressions */
    pg_parser_List* aggorder;       /* ORDER BY (list of SortGroupClause) */
    pg_parser_List* aggdistinct;    /* DISTINCT (list of SortGroupClause) */
    pg_parser_Expr* aggfilter;      /* FILTER expression, if any */
    bool            aggstar;        /* true if argument list was really '*' */
    bool            aggvariadic;    /* true if variadic arguments have been
                                     * combined into an array last argument */
    char               aggkind;     /* aggregate kind (see pg_aggregate.h) */
    pg_parser_Index    agglevelsup; /* > 0 if agg belongs to outer query */
    pg_parser_AggSplit aggsplit;    /* expected agg-splitting mode of parent Agg */
    int32_t            location;    /* token location, or -1 if unknown */
} pg_parser_Aggref;

/* GroupingFunc */
typedef struct pg_parser_GroupingFunc
{
    pg_parser_Expr  xpr;
    pg_parser_List* args;        /* arguments, not evaluated but kept for
                                  * benefit of EXPLAIN etc. */
    pg_parser_List* refs;        /* ressortgrouprefs of arguments */
    pg_parser_List* cols;        /* actual column positions set by planner */
    pg_parser_Index agglevelsup; /* same as Aggref.agglevelsup */
    int32_t         location;    /* token location */
} pg_parser_GroupingFunc;

/*
 * WindowFunc
 */
typedef struct pg_parser_WindowFunc
{
    pg_parser_Expr  xpr;
    uint32_t        winfnoid;    /* pg_proc uint32_t of the function */
    uint32_t        wintype;     /* type uint32_t of result of the window function */
    uint32_t        wincollid;   /* OID of collation of result */
    uint32_t        inputcollid; /* OID of collation that function should use */
    pg_parser_List* args;        /* arguments to the window function */
    pg_parser_Expr* aggfilter;   /* FILTER expression, if any */
    pg_parser_Index winref;      /* index of associated WindowClause */
    bool            winstar;     /* true if argument list was really '*' */
    bool            winagg;      /* is function a simple aggregate? */
    int32_t         location;    /* token location, or -1 if unknown */
} pg_parser_WindowFunc;

/* SubscriptingRef */
typedef struct pg_parser_SubscriptingRef
{
    pg_parser_Expr  xpr;
    uint32_t        refcontainertype; /* type of the container proper */
    uint32_t        refelemtype;      /* type of the container elements */
    int32_t         reftypmod;        /* typmod of the container (and elements too) */
    uint32_t        refcollid;        /* OID of collation, or INVALIDOID if none */
    pg_parser_List* refupperindexpr;  /* expressions that evaluate to upper
                                       * container indexes */
    pg_parser_List* reflowerindexpr;  /* expressions that evaluate to lower
                                       * container indexes, or NIL for single
                                       * container element */
    pg_parser_Expr* refexpr;          /* the expression that evaluates to a
                                       * container value */

    pg_parser_Expr* refassgnexpr; /* expression for the source value, or NULL if
                                   * fetch */
} pg_parser_SubscriptingRef;

/*
 * FuncExpr
 */

typedef enum pg_parser_CoercionForm
{
    COERCE_EXPLICIT_CALL, /* display as a function call */
    COERCE_EXPLICIT_CAST, /* display as an explicit cast */
    COERCE_IMPLICIT_CAST  /* implicit cast, so hide it */
} pg_parser_CoercionForm;

typedef struct pg_parser_FuncExpr
{
    pg_parser_Expr xpr;
    uint32_t       funcid;              /* PG_PROC OID of the function */
    uint32_t       funcresulttype;      /* PG_TYPE OID of result value */
    bool           funcretset;          /* true if function returns set */
    bool           funcvariadic;        /* true if variadic arguments have been
                                         * combined into an array last argument */
    pg_parser_CoercionForm funcformat;  /* how to display this function call */
    uint32_t               funccollid;  /* OID of collation of result */
    uint32_t               inputcollid; /* OID of collation that function should use */
    pg_parser_List*        args;        /* arguments to the function */
    int32_t                location;    /* token location, or -1 if unknown */
} pg_parser_FuncExpr;

/*
 * NamedArgExpr
 */
typedef struct pg_parser_NamedArgExpr
{
    pg_parser_Expr  xpr;
    pg_parser_Expr* arg;       /* the argument expression */
    char*           name;      /* the name */
    int32_t         argnumber; /* argument's number in positional notation */
    int32_t         location;  /* argument name location, or -1 if unknown */
} pg_parser_NamedArgExpr;

/*
 * OpExpr
 */
typedef struct pg_parser_OpExpr
{
    pg_parser_Expr  xpr;
    uint32_t        opno;         /* PG_OPERATOR OID of the operator */
    uint32_t        opfuncid;     /* PG_PROC OID of underlying function */
    uint32_t        opresulttype; /* PG_TYPE OID of result value */
    bool            opretset;     /* true if operator returns set */
    uint32_t        opcollid;     /* OID of collation of result */
    uint32_t        inputcollid;  /* OID of collation that operator should use */
    pg_parser_List* args;         /* arguments to the operator (1 or 2) */
    int32_t         location;     /* token location, or -1 if unknown */
} pg_parser_OpExpr;

typedef pg_parser_OpExpr pg_parser_DistinctExpr;
typedef pg_parser_OpExpr pg_parser_NullIfExpr;

/*
 * ScalarArrayOpExpr - expression node for "scalar op ANY/ALL (array)"
 */
typedef struct pg_parser_ScalarArrayOpExpr
{
    pg_parser_Expr  xpr;
    uint32_t        opno;        /* PG_OPERATOR OID of the operator */
    uint32_t        opfuncid;    /* PG_PROC OID of underlying function */
    bool            useOr;       /* true for ANY, false for ALL */
    uint32_t        inputcollid; /* OID of collation that operator should use */
    pg_parser_List* args;        /* the scalar and array operands */
    int32_t         location;    /* token location, or -1 if unknown */
} pg_parser_ScalarArrayOpExpr;

/*
 * BoolExpr - expression node for the basic Boolean operators AND, OR, NOT
 */
typedef enum pg_parser_BoolExprType
{
    AND_EXPR,
    OR_EXPR,
    NOT_EXPR
} pg_parser_BoolExprType;

typedef struct pg_parser_BoolExpr
{
    pg_parser_Expr         xpr;
    pg_parser_BoolExprType boolop;
    pg_parser_List*        args;     /* arguments to this expression */
    int32_t                location; /* token location, or -1 if unknown */
} pg_parser_BoolExpr;

/*
 * SubLink
 */
typedef enum pg_parser_SubLinkType
{
    EXISTS_SUBLINK,
    ALL_SUBLINK,
    ANY_SUBLINK,
    ROWCOMPARE_SUBLINK,
    EXPR_SUBLINK,
    MULTIEXPR_SUBLINK,
    ARRAY_SUBLINK,
    CTE_SUBLINK /* for SubPlans only */
} pg_parser_SubLinkType;

typedef struct pg_parser_SubLink
{
    pg_parser_Expr        xpr;
    pg_parser_SubLinkType subLinkType; /* see above */
    int32_t               subLinkId;   /* ID (1..n); 0 if not MULTIEXPR */
    pg_parser_Node*       testexpr;    /* outer-query test for ALL/ANY/ROWCOMPARE */
    pg_parser_List*       operName;    /* originally specified operator name */
    pg_parser_Node*       subselect;   /* subselect as Query* or raw parsetree */
    int32_t               location;    /* token location, or -1 if unknown */
} pg_parser_SubLink;

/*
 * FieldSelect
 */
typedef struct pg_parser_FieldSelect
{
    pg_parser_Expr       xpr;
    pg_parser_Expr*      arg;        /* input expression */
    pg_parser_AttrNumber fieldnum;   /* attribute number of field to extract */
    uint32_t             resulttype; /* type of the field (result type of this
                                      * node) */
    int32_t  resulttypmod;           /* output typmod (usually -1) */
    uint32_t resultcollid;           /* OID of collation of the field */
} pg_parser_FieldSelect;

/*
 * FieldStore
 */
typedef struct pg_parser_FieldStore
{
    pg_parser_Expr  xpr;
    pg_parser_Expr* arg;        /* input tuple value */
    pg_parser_List* newvals;    /* new value(s) for field(s) */
    pg_parser_List* fieldnums;  /* integer list of field attnums */
    uint32_t        resulttype; /* type of result (same as type of arg) */
    /* Like RowExpr, we deliberately omit a typmod and collation here */
} pg_parser_FieldStore;

/*
 * RelabelType
 */

typedef struct pg_parser_RelabelType
{
    pg_parser_Expr         xpr;
    pg_parser_Expr*        arg;           /* input expression */
    uint32_t               resulttype;    /* output type of coercion expression */
    int32_t                resulttypmod;  /* output typmod (usually -1) */
    uint32_t               resultcollid;  /* OID of collation, or INVALIDOID if none */
    pg_parser_CoercionForm relabelformat; /* how to display this node */
    int32_t                location;      /* token location, or -1 if unknown */
} pg_parser_RelabelType;

/*
 * CoerceViaIO
 */

typedef struct pg_parser_CoerceViaIO
{
    pg_parser_Expr  xpr;
    pg_parser_Expr* arg;        /* input expression */
    uint32_t        resulttype; /* output type of coercion */
    /* output typmod is not stored, but is presumed -1 */
    uint32_t               resultcollid; /* OID of collation, or INVALIDOID if none */
    pg_parser_CoercionForm coerceformat; /* how to display this node */
    int32_t                location;     /* token location, or -1 if unknown */
} pg_parser_CoerceViaIO;

/*
 * ArrayCoerceExpr
 */

typedef struct pg_parser_ArrayCoerceExpr
{
    pg_parser_Expr         xpr;
    pg_parser_Expr*        arg;          /* input expression (yields an array) */
    pg_parser_Expr*        elemexpr;     /* expression representing per-element work */
    uint32_t               resulttype;   /* output type of coercion (an array type) */
    int32_t                resulttypmod; /* output typmod (also element typmod) */
    uint32_t               resultcollid; /* OID of collation, or INVALIDOID if none */
    pg_parser_CoercionForm coerceformat; /* how to display this node */
    int32_t                location;     /* token location, or -1 if unknown */
} pg_parser_ArrayCoerceExpr;

/*
 * ConvertRowtypeExpr
 */

typedef struct pg_parser_ConvertRowtypeExpr
{
    pg_parser_Expr  xpr;
    pg_parser_Expr* arg;        /* input expression */
    uint32_t        resulttype; /* output type (always a composite type) */
    /* Like RowExpr, we deliberately omit a typmod and collation here */
    pg_parser_CoercionForm convertformat; /* how to display this node */
    int32_t                location;      /* token location, or -1 if unknown */
} pg_parser_ConvertRowtypeExpr;

/*
 * CollateExpr
 */
typedef struct pg_parser_CollateExpr
{
    pg_parser_Expr  xpr;
    pg_parser_Expr* arg;      /* input expression */
    uint32_t        collOid;  /* collation's OID */
    int32_t         location; /* token location, or -1 if unknown */
} pg_parser_CollateExpr;

/*
 * CaseExpr
 */
typedef struct pg_parser_CaseExpr
{
    pg_parser_Expr  xpr;
    uint32_t        casetype;   /* type of expression result */
    uint32_t        casecollid; /* OID of collation, or INVALIDOID if none */
    pg_parser_Expr* arg;        /* implicit equality comparison argument */
    pg_parser_List* args;       /* the arguments (list of WHEN clauses) */
    pg_parser_Expr* defresult;  /* the default result (ELSE clause) */
    int32_t         location;   /* token location, or -1 if unknown */
} pg_parser_CaseExpr;

/*
 * CaseWhen - one arm of a CASE expression
 */
typedef struct pg_parser_CaseWhen
{
    pg_parser_Expr  xpr;
    pg_parser_Expr* expr;     /* condition expression */
    pg_parser_Expr* result;   /* substitution result */
    int32_t         location; /* token location, or -1 if unknown */
} pg_parser_CaseWhen;

typedef struct pg_parser_CaseTestExpr
{
    pg_parser_Expr xpr;
    uint32_t       typeId;    /* type for substituted value */
    int32_t        typeMod;   /* typemod for substituted value */
    uint32_t       collation; /* collation for the substituted value */
} pg_parser_CaseTestExpr;

/*
 * ArrayExpr
 */
typedef struct pg_parser_ArrayExpr
{
    pg_parser_Expr  xpr;
    uint32_t        array_typeid;   /* type of expression result */
    uint32_t        array_collid;   /* OID of collation, or INVALIDOID if none */
    uint32_t        element_typeid; /* common type of array elements */
    pg_parser_List* elements;       /* the array elements or sub-arrays */
    bool            multidims;      /* true if elements are sub-arrays */
    int32_t         location;       /* token location, or -1 if unknown */
} pg_parser_ArrayExpr;

/*
 * RowExpr
 */
typedef struct pg_parser_RowExpr
{
    pg_parser_Expr  xpr;
    pg_parser_List* args;       /* the fields */
    uint32_t        row_typeid; /* RECORDOID or a composite type's ID */

    /*
     * row_typeid cannot be a domain over composite, only plain composite.  To
     * create a composite domain value, apply CoerceToDomain to the RowExpr.
     *
     * Note: we deliberately do NOT store a typmod.  Although a typmod will be
     * associated with specific RECORD types at runtime, it will differ for
     * different backends, and so cannot safely be stored in stored
     * parsetrees.  We must assume typmod -1 for a RowExpr node.
     *
     * We don't need to store a collation either.  The result type is
     * necessarily composite, and composite types never have a collation.
     */
    pg_parser_CoercionForm row_format; /* how to display this node */
    pg_parser_List*        colnames;   /* list of String, or NIL */
    int32_t                location;   /* token location, or -1 if unknown */
} pg_parser_RowExpr;

/*
 * RowCompareExpr
 */
typedef enum pg_parser_RowCompareType
{
    /* Values of this enum are chosen to match btree strategy numbers */
    ROWCOMPARE_LT = 1, /* BTLessStrategyNumber */
    ROWCOMPARE_LE = 2, /* BTLessEqualStrategyNumber */
    ROWCOMPARE_EQ = 3, /* BTEqualStrategyNumber */
    ROWCOMPARE_GE = 4, /* BTGreaterEqualStrategyNumber */
    ROWCOMPARE_GT = 5, /* BTGreaterStrategyNumber */
    ROWCOMPARE_NE = 6  /* no such btree strategy */
} pg_parser_RowCompareType;

typedef struct pg_parser_RowCompareExpr
{
    pg_parser_Expr           xpr;
    pg_parser_RowCompareType rctype;       /* LT LE GE or GT, never EQ or NE */
    pg_parser_List*          opnos;        /* OID list of pairwise comparison ops */
    pg_parser_List*          opfamilies;   /* OID list of containing operator families */
    pg_parser_List*          inputcollids; /* OID list of collations for comparisons */
    pg_parser_List*          largs;        /* the left-hand input arguments */
    pg_parser_List*          rargs;        /* the right-hand input arguments */
} pg_parser_RowCompareExpr;

/*
 * CoalesceExpr
 */
typedef struct pg_parser_CoalesceExpr
{
    pg_parser_Expr  xpr;
    uint32_t        coalescetype;   /* type of expression result */
    uint32_t        coalescecollid; /* OID of collation, or INVALIDOID if none */
    pg_parser_List* args;           /* the arguments */
    int32_t         location;       /* token location, or -1 if unknown */
} pg_parser_CoalesceExpr;

/*
 * MinMaxExpr - a GREATEST or LEAST function
 */
typedef enum pg_parser_MinMaxOp
{
    IS_GREATEST,
    IS_LEAST
} pg_parser_MinMaxOp;

typedef struct pg_parser_MinMaxExpr
{
    pg_parser_Expr     xpr;
    uint32_t           minmaxtype;   /* common type of arguments and result */
    uint32_t           minmaxcollid; /* OID of collation of result */
    uint32_t           inputcollid;  /* OID of collation that function should use */
    pg_parser_MinMaxOp op;           /* function to execute */
    pg_parser_List*    args;         /* the arguments */
    int32_t            location;     /* token location, or -1 if unknown */
} pg_parser_MinMaxExpr;

/*
 * SQLValueFunction
 */
typedef enum pg_parser_SQLValueFunctionOp
{
    SVFOP_CURRENT_DATE,
    SVFOP_CURRENT_TIME,
    SVFOP_CURRENT_TIME_N,
    SVFOP_CURRENT_TIMESTAMP,
    SVFOP_CURRENT_TIMESTAMP_N,
    SVFOP_LOCALTIME,
    SVFOP_LOCALTIME_N,
    SVFOP_LOCALTIMESTAMP,
    SVFOP_LOCALTIMESTAMP_N,
    SVFOP_CURRENT_ROLE,
    SVFOP_CURRENT_USER,
    SVFOP_USER,
    SVFOP_SESSION_USER,
    SVFOP_CURRENT_CATALOG,
    SVFOP_CURRENT_SCHEMA
} pg_parser_SQLValueFunctionOp;

typedef struct pg_parser_SQLValueFunction
{
    pg_parser_Expr               xpr;
    pg_parser_SQLValueFunctionOp op;   /* which function this is */
    uint32_t                     type; /* result type/typmod */
    int32_t                      typmod;
    int32_t                      location; /* token location, or -1 if unknown */
} pg_parser_SQLValueFunction;

/*
 * XmlExpr - various SQL/XML functions requiring special grammar productions
 *
 * 'name' carries the "NAME foo" argument (already XML-escaped).
 * 'named_args' and 'arg_names' represent an xml_attribute list.
 * 'args' carries all other arguments.
 *
 * Note: result type/typmod/collation are not stored, but can be deduced
 * from the XmlExprOp.  The type/typmod fields are just used for display
 * purposes, and are NOT necessarily the true result type of the node.
 */
typedef enum pg_parser_XmlExprOp
{
    IS_XMLCONCAT,    /* XMLCONCAT(args) */
    IS_XMLELEMENT,   /* XMLELEMENT(name, xml_attributes, args) */
    IS_XMLFOREST,    /* XMLFOREST(xml_attributes) */
    IS_XMLPARSE,     /* XMLPARSE(text, is_doc, preserve_ws) */
    IS_XMLPI,        /* XMLPI(name [, args]) */
    IS_XMLROOT,      /* XMLROOT(xml, version, standalone) */
    IS_XMLSERIALIZE, /* XMLSERIALIZE(is_document, xmlval) */
    IS_DOCUMENT      /* xmlval IS DOCUMENT */
} pg_parser_XmlExprOp;

typedef enum
{
    XMLOPTION_DOCUMENT,
    XMLOPTION_CONTENT
} pg_parser_XmlOptionType;

typedef struct pg_parser_XmlExpr
{
    pg_parser_Expr          xpr;
    pg_parser_XmlExprOp     op;         /* xml function ID */
    char*                   name;       /* name in xml(NAME foo ...) syntaxes */
    pg_parser_List*         named_args; /* non-XML expressions for xml_attributes */
    pg_parser_List*         arg_names;  /* parallel list of Value strings */
    pg_parser_List*         args;       /* list of expressions */
    pg_parser_XmlOptionType xmloption;  /* DOCUMENT or CONTENT */
    uint32_t                type;       /* target type/typmod for XMLSERIALIZE */
    int32_t                 typmod;
    int32_t                 location; /* token location, or -1 if unknown */
} pg_parser_XmlExpr;

/*
 * NullTest
 */

typedef enum pg_parser_NullTestType
{
    IS_NULL,
    IS_NOT_NULL
} pg_parser_NullTestType;

typedef struct pg_parser_NullTest
{
    pg_parser_Expr         xpr;
    pg_parser_Expr*        arg;          /* input expression */
    pg_parser_NullTestType nulltesttype; /* IS NULL, IS NOT NULL */
    bool                   argisrow;     /* T to perform field-by-field null checks */
    int32_t                location;     /* token location, or -1 if unknown */
} pg_parser_NullTest;

/*
 * BooleanTest
 */

typedef enum pg_parser_BoolTestType
{
    IS_TRUE,
    IS_NOT_TRUE,
    IS_FALSE,
    IS_NOT_FALSE,
    IS_UNKNOWN,
    IS_NOT_UNKNOWN
} pg_parser_BoolTestType;

typedef struct pg_parser_BooleanTest
{
    pg_parser_Expr         xpr;
    pg_parser_Expr*        arg;          /* input expression */
    pg_parser_BoolTestType booltesttype; /* test type */
    int32_t                location;     /* token location, or -1 if unknown */
} pg_parser_BooleanTest;

/*
 * CoerceToDomain
 */
typedef struct pg_parser_CoerceToDomain
{
    pg_parser_Expr         xpr;
    pg_parser_Expr*        arg;            /* input expression */
    uint32_t               resulttype;     /* domain type ID (result type) */
    int32_t                resulttypmod;   /* output typmod (currently always -1) */
    uint32_t               resultcollid;   /* OID of collation, or INVALIDOID if none */
    pg_parser_CoercionForm coercionformat; /* how to display this node */
    int32_t                location;       /* token location, or -1 if unknown */
} pg_parser_CoerceToDomain;

typedef struct pg_parser_CoerceToDomainValue
{
    pg_parser_Expr xpr;
    uint32_t       typeId;    /* type for substituted value */
    int32_t        typeMod;   /* typemod for substituted value */
    uint32_t       collation; /* collation for the substituted value */
    int32_t        location;  /* token location, or -1 if unknown */
} pg_parser_CoerceToDomainValue;

typedef struct pg_parser_SetToDefault
{
    pg_parser_Expr xpr;
    uint32_t       typeId;    /* type for substituted value */
    int32_t        typeMod;   /* typemod for substituted value */
    uint32_t       collation; /* collation for the substituted value */
    int32_t        location;  /* token location, or -1 if unknown */
} pg_parser_SetToDefault;

typedef struct pg_parser_CurrentOfExpr
{
    pg_parser_Expr  xpr;
    pg_parser_Index cvarno;       /* RT index of target relation */
    char*           cursor_name;  /* name of referenced cursor, or NULL */
    int32_t         cursor_param; /* refcursor parameter number, or 0 */
} pg_parser_CurrentOfExpr;

typedef struct pg_parser_NextValueExpr
{
    pg_parser_Expr xpr;
    uint32_t       seqid;
    uint32_t       typeId;
} pg_parser_NextValueExpr;

/*
 * InferenceElem
 */
typedef struct pg_parser_InferenceElem
{
    pg_parser_Expr  xpr;
    pg_parser_Node* expr;         /* expression to infer from, or NULL */
    uint32_t        infercollid;  /* OID of collation, or INVALIDOID */
    uint32_t        inferopclass; /* OID of att opclass, or INVALIDOID */
} pg_parser_InferenceElem;

/*
 * TargetEntry
 */
typedef struct pg_parser_TargetEntry
{
    pg_parser_Expr       xpr;
    pg_parser_Expr*      expr;            /* expression to evaluate */
    pg_parser_AttrNumber resno;           /* attribute number (see notes above) */
    char*                resname;         /* name of the column (could be NULL) */
    pg_parser_Index      ressortgroupref; /* nonzero if referenced by a sort/group
                                           * clause */
    uint32_t             resorigtbl;      /* OID of column's source table */
    pg_parser_AttrNumber resorigcol;      /* column's number in source table */
    bool                 resjunk;         /* set to true to eliminate the attribute from
                                           * final target list */
} pg_parser_TargetEntry;

/*
 * RangeTblRef
 */
typedef struct pg_parser_RangeTblRef
{
    pg_parser_NodeTag type;
    int32_t           rtindex;
} pg_parser_RangeTblRef;

/*
 * JoinExpr
 */

typedef enum pg_parser_JoinType
{
    JOIN_INNER,        /* matching tuple pairs only */
    JOIN_LEFT,         /* pairs + unmatched LHS tuples */
    JOIN_FULL,         /* pairs + unmatched LHS + unmatched RHS */
    JOIN_RIGHT,        /* pairs + unmatched RHS tuples */
    JOIN_SEMI,         /* 1 copy of each LHS row that has match(es) */
    JOIN_ANTI,         /* 1 copy of each LHS row that has no match */
    JOIN_UNIQUE_OUTER, /* LHS path must be made unique */
    JOIN_UNIQUE_INNER  /* RHS path must be made unique */
} pg_parser_JoinType;

typedef struct pg_parser_JoinExpr
{
    pg_parser_NodeTag  type;
    pg_parser_JoinType jointype;    /* type of join */
    bool               isNatural;   /* Natural join? Will need to shape table */
    pg_parser_Node*    larg;        /* left subtree */
    pg_parser_Node*    rarg;        /* right subtree */
    pg_parser_List*    usingClause; /* USING clause, if any (list of String) */
    pg_parser_Node*    quals;       /* qualifiers on join, if any */
    pg_parser_Alias*   alias;       /* user-written alias clause, if any */
    int32_t            rtindex;     /* RT index assigned for join, or 0 */
} pg_parser_JoinExpr;

typedef enum pg_parser_RTEKind
{
    RTE_RELATION,        /* ordinary relation reference */
    RTE_SUBQUERY,        /* subquery in FROM */
    RTE_JOIN,            /* join */
    RTE_FUNCTION,        /* function in FROM */
    RTE_TABLEFUNC,       /* TableFunc(.., column list) */
    RTE_VALUES,          /* VALUES (<exprlist>), (<exprlist>), ... */
    RTE_CTE,             /* common table expr (WITH list element) */
    RTE_NAMEDTUPLESTORE, /* tuplestore, e.g. for AFTER triggers */
    RTE_RESULT           /* RTE represents an empty FROM clause; such
                          * RTEs are added by the planner, they're not
                          * present during parsing or rewriting */
} pg_parser_RTEKind;

typedef struct pg_parser_TableSampleClause
{
    pg_parser_NodeTag type;
    uint32_t          tsmhandler; /* OID of the tablesample handler function */
    pg_parser_List*   args;       /* tablesample argument expression(s) */
    pg_parser_Expr*   repeatable; /* REPEATABLE expression, or NULL if none */
} pg_parser_TableSampleClause;

typedef struct pg_parser_RangeTblEntry
{
    pg_parser_NodeTag                   type;
    pg_parser_RTEKind                   rtekind;     /* see above */
    uint32_t                            relid;       /* OID of the relation */
    char                                relkind;     /* relation kind (see pg_class.relkind) */
    int32_t                             rellockmode; /* lock level that query requires on the rel */
    struct pg_parser_TableSampleClause* tablesample; /* sampling info, or NULL */

    pg_parser_Query* subquery;         /* the sub-query */
    bool             security_barrier; /* is from security_barrier view? */

    pg_parser_JoinType jointype;      /* type of join */
    pg_parser_List*    joinaliasvars; /* list of alias-var expansions */

    pg_parser_List* functions;      /* list of RangeTblFunction nodes */
    bool            funcordinality; /* is this called WITH ORDINALITY? */

    pg_parser_TableFunc* tablefunc;

    pg_parser_List* values_lists; /* list of expression lists */

    char*           ctename;        /* name of the WITH list item */
    pg_parser_Index ctelevelsup;    /* number of query levels up */
    bool            self_reference; /* is this a recursive self-reference? */

    pg_parser_List* coltypes;      /* OID list of column type OIDs */
    pg_parser_List* coltypmods;    /* integer list of column typmods */
    pg_parser_List* colcollations; /* OID list of column collation OIDs */

    char*  enrname;   /* name of ephemeral named relation */
    double enrtuples; /* estimated or actual from caller */

    pg_parser_Alias*     alias;            /* user-written alias clause, if any */
    pg_parser_Alias*     eref;             /* expanded reference names */
    bool                 lateral;          /* subquery, function, or values is LATERAL? */
    bool                 inh;              /* inheritance requested? */
    bool                 inFromCl;         /* present in FROM clause? */
    uint32_t             requiredPerms;    /* bitmask of required access permissions */
    uint32_t             checkAsUser;      /* if valid, check access as this role */
    pg_parser_Bitmapset* selectedCols;     /* columns needing SELECT permission */
    pg_parser_Bitmapset* insertedCols;     /* columns needing INSERT permission */
    pg_parser_Bitmapset* updatedCols;      /* columns needing UPDATE permission */
    pg_parser_Bitmapset* extraUpdatedCols; /* generated columns being updated */
    pg_parser_List*      securityQuals;    /* security barrier quals to apply, if any */
} pg_parser_RangeTblEntry;

typedef struct pg_parser_RangeTblFunction
{
    pg_parser_NodeTag type;

    pg_parser_Node* funcexpr;     /* expression tree for func call */
    int32_t         funccolcount; /* number of columns it contributes to RTE */
    /* These fields record the contents of a column definition list, if any: */
    pg_parser_List* funccolnames;      /* column names (list of String) */
    pg_parser_List* funccoltypes;      /* OID list of column type OIDs */
    pg_parser_List* funccoltypmods;    /* integer list of column typmods */
    pg_parser_List* funccolcollations; /* OID list of column collation OIDs */
    /* This is set during planning for use by the executor: */
    pg_parser_Bitmapset* funcparams; /* PARAM_EXEC Param IDs affecting this func */
} pg_parser_RangeTblFunction;

typedef enum pg_parser_DefElemAction
{
    DEFELEM_UNSPEC, /* no action given */
    DEFELEM_SET,
    DEFELEM_ADD,
    DEFELEM_DROP
} pg_parser_DefElemAction;

typedef struct DefElem
{
    pg_parser_NodeTag       type;
    char*                   defnamespace; /* NULL if unqualified name */
    char*                   defname;
    pg_parser_Node*         arg;       /* a (Value *) or a (TypeName *) */
    pg_parser_DefElemAction defaction; /* unspecified action, or SET/ADD/DROP */
    int32_t                 location;  /* token location, or -1 if unknown */
} pg_parser_DefElem;

typedef struct pg_parser_Plan
{
    pg_parser_NodeTag type;
    /*
     * estimated execution costs for plan (see costsize.c for more info)
     */
    double startup_cost; /* cost expended before fetching any tuples */
    double total_cost;   /* total cost (assuming all tuples fetched) */

    /*
     * planner's estimate of result size of this plan step
     */
    double  plan_rows;  /* number of rows plan is expected to emit */
    int32_t plan_width; /* average row width in bytes */

    /*
     * information needed for parallel query
     */
    bool parallel_aware; /* engage parallel-aware logic? */
    bool parallel_safe;  /* OK to use as part of parallel plan? */

    /*
     * Common structural data for all pg_parser_Plan types.
     */
    int32_t                plan_node_id; /* unique across entire final plan tree */
    pg_parser_List*        targetlist;   /* target list to be computed at this node */
    pg_parser_List*        qual;         /* implicitly-ANDed qual conditions */
    struct pg_parser_Plan* lefttree;     /* input plan tree(s) */
    struct pg_parser_Plan* righttree;
    pg_parser_List*        initPlan; /* Init pg_parser_Plan nodes (un-correlated expr
                                      * subselects) */

    /*
     * Information for management of parameter-change-driven rescanning
     *
     * extParam includes the paramIDs of all external PARAM_EXEC params
     * affecting this plan node or its children.  setParam params from the
     * node's initPlans are not included, but their extParams are.
     *
     * allParam includes all the extParam paramIDs, plus the IDs of local
     * params that affect the node (i.e., the setParams of its initplans).
     * These are _all_ the PARAM_EXEC params that affect this node.
     */
    pg_parser_Bitmapset* extParam;
    pg_parser_Bitmapset* allParam;
} pg_parser_Plan;

typedef struct pg_parser_PlannedStmt
{
    pg_parser_NodeTag      type;
    pg_parser_CmdType      commandType;        /* select|insert|update|delete|utility */
    uint64_t               queryId;            /* query identifier (copied from Query) */
    bool                   hasReturning;       /* is it insert|update|delete RETURNING? */
    bool                   hasModifyingCTE;    /* has insert|update|delete in WITH? */
    bool                   canSetTag;          /* do I set the command result tag? */
    bool                   transientPlan;      /* redo plan when TransactionXmin changes? */
    bool                   dependsOnRole;      /* is plan specific to current role? */
    bool                   parallelModeNeeded; /* parallel mode required to execute? */
    int32_t                jitFlags;           /* which forms of JIT should be performed */
    struct pg_parser_Plan* planTree;           /* tree of pg_parser_Plan nodes */
    pg_parser_List*        rtable;             /* list of RangeTblEntry nodes */
    /* rtable indexes of target relations for INSERT/UPDATE/DELETE */
    pg_parser_List* resultRelations; /* integer list of RT indexes, or NIL */
    /*
     * rtable indexes of partitioned table roots that are UPDATE/DELETE
     * targets; needed for trigger firing.
     */
    pg_parser_List* rootResultRelations;
    pg_parser_List* subplans;            /* pg_parser_Plan trees for SubPlan expressions; note
                                          * that some could be NULL */
    pg_parser_Bitmapset* rewindPlanIDs;  /* indices of subplans that require REWIND */
    pg_parser_List*      rowMarks;       /* a list of PlanRowMark's */
    pg_parser_List*      relationOids;   /* OIDs of relations the plan depends on */
    pg_parser_List*      invalItems;     /* other dependencies, as PlanInvalItems */
    pg_parser_List*      paramExecTypes; /* type OIDs for PARAM_EXEC Params */
    pg_parser_Node*      utilityStmt;    /* non-null if this is utility stmt */
    /* statement location in source string (copied from Query) */
    int32_t stmt_location; /* start location, or -1 if unknown */
    int32_t stmt_len;      /* length in bytes; 0 means "rest of string" */
} pg_parser_PlannedStmt;

typedef struct pg_parser_Result
{
    pg_parser_Plan  plan;
    pg_parser_Node* resconstantqual;
} pg_parser_Result;

typedef struct pg_parser_ProjectSet
{
    pg_parser_Plan plan;
} pg_parser_ProjectSet;

typedef struct pg_parser_PartitionPruneInfo
{
    pg_parser_NodeTag    type;
    pg_parser_List*      prune_infos;
    pg_parser_Bitmapset* other_subplans;
} pg_parser_PartitionPruneInfo;

typedef struct pg_parser_ModifyTable
{
    pg_parser_Plan             plan;
    pg_parser_CmdType          operation;            /* INSERT, UPDATE, or DELETE */
    bool                       canSetTag;            /* do we set the command tag/es_processed? */
    pg_parser_Index            nominalRelation;      /* Parent RT index for use of EXPLAIN */
    pg_parser_Index            rootRelation;         /* Root RT index, if target is partitioned */
    bool                       partColsUpdated;      /* some part key in hierarchy updated */
    pg_parser_List*            resultRelations;      /* integer list of RT indexes */
    int32_t                    resultRelIndex;       /* index of first resultRel in plan's list */
    int32_t                    rootResultRelIndex;   /* index of the partitioned table root */
    pg_parser_List*            plans;                /* plan(s) producing source data */
    pg_parser_List*            withCheckOptionLists; /* per-target-table WCO lists */
    pg_parser_List*            returningLists;       /* per-target-table RETURNING tlists */
    pg_parser_List*            fdwPrivLists;         /* per-target-table FDW private data lists */
    pg_parser_Bitmapset*       fdwDirectModifyPlans; /* indices of FDW DM plans */
    pg_parser_List*            rowMarks;             /* PlanRowMarks (non-locking only) */
    int32_t                    epqParam;             /* ID of Param for EvalPlanQual re-eval */
    pg_parser_OnConflictAction onConflictAction;     /* ON CONFLICT action */
    pg_parser_List* arbiterIndexes;  /* pg_parser_List of ON CONFLICT arbiter index OIDs  */
    pg_parser_List* onConflictSet;   /* SET for INSERT ON CONFLICT DO UPDATE */
    pg_parser_Node* onConflictWhere; /* WHERE for ON CONFLICT UPDATE */
    pg_parser_Index exclRelRTI;      /* RTI of the EXCLUDED pseudo relation */
    pg_parser_List* exclRelTlist;    /* tlist of the EXCLUDED pseudo relation */
} pg_parser_ModifyTable;

typedef struct pg_parser_Append
{
    pg_parser_Plan  plan;
    pg_parser_List* appendplans;

    /*
     * All 'appendplans' preceding this index are non-partial plans. All
     * 'appendplans' from this index onwards are partial plans.
     */
    int32_t first_partial_plan;

    /* Info for run-time subplan pruning; NULL if we're not doing that */
    struct pg_parser_PartitionPruneInfo* part_prune_info;
} pg_parser_Append;

typedef struct pg_parser_MergeAppend
{
    pg_parser_Plan  plan;
    pg_parser_List* mergeplans;
    /* these fields are just like the sort-key info in struct Sort: */
    int32_t               numCols;       /* number of sort-key columns */
    pg_parser_AttrNumber* sortColIdx;    /* their indexes in the target list */
    uint32_t*             sortOperators; /* OIDs of operators to sort them by */
    uint32_t*             collations;    /* OIDs of collations */
    bool*                 nullsFirst;    /* NULLS FIRST/LAST directions */
    /* Info for run-time subplan pruning; NULL if we're not doing that */
    struct pg_parser_PartitionPruneInfo* part_prune_info;
} pg_parser_MergeAppend;

typedef struct pg_parser_RecursiveUnion
{
    pg_parser_Plan plan;
    int32_t        wtParam; /* ID of Param representing work table */
    /* Remaining fields are zero/null in UNION ALL case */
    int32_t numCols;                    /* number of columns to check for
                                         * duplicate-ness */
    pg_parser_AttrNumber* dupColIdx;    /* their indexes in the target list */
    uint32_t*             dupOperators; /* equality operators to compare with */
    uint32_t*             dupCollations;
    int64_t               numGroups; /* estimated number of groups in input */
} pg_parser_RecursiveUnion;

typedef struct pg_parser_BitmapAnd
{
    pg_parser_Plan  plan;
    pg_parser_List* bitmapplans;
} pg_parser_BitmapAnd;

typedef struct pg_parser_BitmapOr
{
    pg_parser_Plan  plan;
    bool            isshared;
    pg_parser_List* bitmapplans;
} pg_parser_BitmapOr;

typedef struct pg_parser_Scan
{
    pg_parser_Plan  plan;
    pg_parser_Index scanrelid; /* relid is index into the range table */
} pg_parser_Scan;

typedef pg_parser_Scan pg_parser_SeqScan;

typedef struct pg_parser_SampleScan
{
    pg_parser_Scan scan;
    /* use struct pointer to avoid including parsenodes.h here */
    struct pg_parser_TableSampleClause* tablesample;
} pg_parser_SampleScan;

typedef enum pg_parser_ScanDirection
{
    BackwardScanDirection = -1,
    NoMovementScanDirection = 0,
    ForwardScanDirection = 1
} pg_parser_ScanDirection;

typedef struct pg_parser_IndexScan
{
    pg_parser_Scan          scan;
    uint32_t                indexid;          /* OID of index to scan */
    pg_parser_List*         indexqual;        /* list of index quals (usually OpExprs) */
    pg_parser_List*         indexqualorig;    /* the same in original form */
    pg_parser_List*         indexorderby;     /* list of index ORDER BY exprs */
    pg_parser_List*         indexorderbyorig; /* the same in original form */
    pg_parser_List*         indexorderbyops;  /* OIDs of sort ops for ORDER BY exprs */
    pg_parser_ScanDirection indexorderdir;    /* forward or backward or don't care */
} pg_parser_IndexScan;

typedef struct pg_parser_IndexOnlyScan
{
    pg_parser_Scan          scan;
    uint32_t                indexid;       /* OID of index to scan */
    pg_parser_List*         indexqual;     /* list of index quals (usually OpExprs) */
    pg_parser_List*         indexorderby;  /* list of index ORDER BY exprs */
    pg_parser_List*         indextlist;    /* TargetEntry list describing index's cols */
    pg_parser_ScanDirection indexorderdir; /* forward or backward or don't care */
} pg_parser_IndexOnlyScan;

typedef struct pg_parser_BitmapIndexScan
{
    pg_parser_Scan  scan;
    uint32_t        indexid;       /* OID of index to scan */
    bool            isshared;      /* Create shared bitmap if set */
    pg_parser_List* indexqual;     /* list of index quals (OpExprs) */
    pg_parser_List* indexqualorig; /* the same in original form */
} pg_parser_BitmapIndexScan;

typedef struct pg_parser_BitmapHeapScan
{
    pg_parser_Scan  scan;
    pg_parser_List* bitmapqualorig; /* index quals, in standard expr form */
} pg_parser_BitmapHeapScan;

typedef struct pg_parser_TidScan
{
    pg_parser_Scan  scan;
    pg_parser_List* tidquals; /* qual(s) involving CTID = something */
} pg_parser_TidScan;

typedef struct pg_parser_SubqueryScan
{
    pg_parser_Scan  scan;
    pg_parser_Plan* subplan;
} pg_parser_SubqueryScan;

typedef struct pg_parser_FunctionScan
{
    pg_parser_Scan  scan;
    pg_parser_List* functions;      /* list of RangeTblFunction nodes */
    bool            funcordinality; /* WITH ORDINALITY */
} pg_parser_FunctionScan;

typedef struct pg_parser_ValuesScan
{
    pg_parser_Scan  scan;
    pg_parser_List* values_lists; /* list of expression lists */
} pg_parser_ValuesScan;

typedef struct pg_parser_TableFuncScan
{
    pg_parser_Scan       scan;
    pg_parser_TableFunc* tablefunc; /* table function node */
} pg_parser_TableFuncScan;

/* ----------------
 *        CteScan node
 * ----------------
 */
typedef struct pg_parser_CteScan
{
    pg_parser_Scan scan;
    int32_t        ctePlanId; /* ID of init SubPlan for CTE */
    int32_t        cteParam;  /* ID of Param representing CTE output */
} pg_parser_CteScan;

typedef struct pg_parser_NamedTuplestoreScan
{
    pg_parser_Scan scan;
    char*          enrname; /* Name given to Ephemeral Named Relation */
} pg_parser_NamedTuplestoreScan;

typedef struct pg_parser_WorkTableScan
{
    pg_parser_Scan scan;
    int32_t        wtParam; /* ID of Param representing work table */
} pg_parser_WorkTableScan;

typedef struct pg_parser_ForeignScan
{
    pg_parser_Scan       scan;
    pg_parser_CmdType    operation;         /* SELECT/INSERT/UPDATE/DELETE */
    uint32_t             fs_server;         /* OID of foreign server */
    pg_parser_List*      fdw_exprs;         /* expressions that FDW may evaluate */
    pg_parser_List*      fdw_private;       /* private data for FDW */
    pg_parser_List*      fdw_scan_tlist;    /* optional tlist describing scan tuple */
    pg_parser_List*      fdw_recheck_quals; /* original quals not in scan.plan.qual */
    pg_parser_Bitmapset* fs_relids;         /* RTIs generated by this scan */
    bool                 fsSystemCol;       /* true if any "system column" is needed */
} pg_parser_ForeignScan;

struct pg_parser_CustomScanMethods;

typedef struct pg_parser_CustomScan
{
    pg_parser_Scan scan;
    uint32_t       flags;                   /* mask of CUSTOMPATH_* flags, see
                                             * nodes/extensible.h */
    pg_parser_List*      custom_plans;      /* list of pg_parser_Plan nodes, if any */
    pg_parser_List*      custom_exprs;      /* expressions that custom code may evaluate */
    pg_parser_List*      custom_private;    /* private data for custom code */
    pg_parser_List*      custom_scan_tlist; /* optional tlist describing scan tuple */
    pg_parser_Bitmapset* custom_relids;     /* RTIs generated by this scan */
    const struct pg_parser_CustomScanMethods* methods;
} pg_parser_CustomScan;

typedef struct pg_parser_CustomScanMethods
{
    const char* CustomName;

    /* Create execution state (CustomScanState) from a CustomScan plan node */
    pg_parser_Node* (*CreateCustomScanState)(pg_parser_CustomScan* cscan);
} pg_parser_CustomScanMethods;

typedef struct pg_parser_Join
{
    pg_parser_Plan     plan;
    pg_parser_JoinType jointype;
    bool               inner_unique;
    pg_parser_List*    joinqual; /* JOIN quals (in addition to plan.qual) */
} pg_parser_Join;

typedef struct pg_parser_NestLoop
{
    pg_parser_Join  join;
    pg_parser_List* nestParams; /* list of NestLoopParam nodes */
} pg_parser_NestLoop;

typedef struct pg_parser_NestLoopParam
{
    pg_parser_NodeTag type;
    int32_t           paramno;  /* number of the PARAM_EXEC Param to set */
    pg_parser_Var*    paramval; /* outer-relation pg_parser_Var to assign to Param */
} pg_parser_NestLoopParam;

typedef struct pg_parser_MergeJoin
{
    pg_parser_Join  join;
    bool            skip_mark_restore; /* Can we skip mark/restore calls? */
    pg_parser_List* mergeclauses;      /* mergeclauses as expression trees */
    /* these are arrays, but have the same length as the mergeclauses list: */
    uint32_t* mergeFamilies;   /* per-clause OIDs of btree opfamilies */
    uint32_t* mergeCollations; /* per-clause OIDs of collations */
    int32_t*  mergeStrategies; /* per-clause ordering (ASC or DESC) */
    bool*     mergeNullsFirst; /* per-clause nulls ordering */
} pg_parser_MergeJoin;

typedef struct pg_parser_HashJoin
{
    pg_parser_Join  join;
    pg_parser_List* hashclauses;
    pg_parser_List* hashoperators;
    pg_parser_List* hashcollations;

    /*
     * pg_parser_List of expressions to be hashed for tuples from the outer plan, to
     * perform lookups in the hashtable over the inner plan.
     */
    pg_parser_List* hashkeys;
} pg_parser_HashJoin;

typedef struct pg_parser_Material
{
    pg_parser_Plan plan;
} pg_parser_Material;

/* ----------------
 *        sort node
 * ----------------
 */
typedef struct pg_parser_Sort
{
    pg_parser_Plan        plan;
    int32_t               numCols;       /* number of sort-key columns */
    pg_parser_AttrNumber* sortColIdx;    /* their indexes in the target list */
    uint32_t*             sortOperators; /* OIDs of operators to sort them by */
    uint32_t*             collations;    /* OIDs of collations */
    bool*                 nullsFirst;    /* NULLS FIRST/LAST directions */
} pg_parser_Sort;

typedef struct pg_parser_Group
{
    pg_parser_Plan        plan;
    int32_t               numCols;      /* number of grouping columns */
    pg_parser_AttrNumber* grpColIdx;    /* their indexes in the target list */
    uint32_t*             grpOperators; /* equality operators to compare with */
    uint32_t*             grpCollations;
} pg_parser_Group;

typedef enum pg_parser_AggStrategy
{
    AGG_PLAIN,  /* simple agg across all input rows */
    AGG_SORTED, /* grouped agg, input must be sorted */
    AGG_HASHED, /* grouped agg, use internal hashtable */
    AGG_MIXED   /* grouped agg, hash and sort both used */
} pg_parser_AggStrategy;

typedef struct pg_parser_Agg
{
    pg_parser_Plan        plan;
    pg_parser_AggStrategy aggstrategy;  /* basic strategy, see nodes.h */
    pg_parser_AggSplit    aggsplit;     /* agg-splitting mode, see nodes.h */
    int32_t               numCols;      /* number of grouping columns */
    pg_parser_AttrNumber* grpColIdx;    /* their indexes in the target list */
    uint32_t*             grpOperators; /* equality operators to compare with */
    uint32_t*             grpCollations;
    int64_t               numGroups; /* estimated number of groups in input */
    pg_parser_Bitmapset*  aggParams; /* IDs of Params used in Aggref inputs */
    /* Note: planner provides numGroups & aggParams only in HASHED/MIXED case */
    pg_parser_List* groupingSets; /* grouping sets to use */
    pg_parser_List* chain;        /* chained Agg/Sort nodes */
} pg_parser_Agg;

/* ----------------
 *        window aggregate node
 * ----------------
 */
typedef struct pg_parser_WindowAgg
{
    pg_parser_Plan        plan;
    pg_parser_Index       winref;         /* ID referenced by window functions */
    int32_t               partNumCols;    /* number of columns in partition clause */
    pg_parser_AttrNumber* partColIdx;     /* their indexes in the target list */
    uint32_t*             partOperators;  /* equality operators for partition columns */
    uint32_t*             partCollations; /* collations for partition columns */
    int32_t               ordNumCols;     /* number of columns in ordering clause */
    pg_parser_AttrNumber* ordColIdx;      /* their indexes in the target list */
    uint32_t*             ordOperators;   /* equality operators for ordering columns */
    uint32_t*             ordCollations;  /* collations for ordering columns */
    int32_t               frameOptions;   /* frame_clause options, see WindowDef */
    pg_parser_Node*       startOffset;    /* expression for starting bound, if any */
    pg_parser_Node*       endOffset;      /* expression for ending bound, if any */
    /* these fields are used with RANGE offset PRECEDING/FOLLOWING: */
    uint32_t startInRangeFunc;  /* in_range function for startOffset */
    uint32_t endInRangeFunc;    /* in_range function for endOffset */
    uint32_t inRangeColl;       /* collation for in_range tests */
    bool     inRangeAsc;        /* use ASC sort order for in_range tests? */
    bool     inRangeNullsFirst; /* nulls sort first for in_range tests? */
} pg_parser_WindowAgg;

typedef struct pg_parser_Unique
{
    pg_parser_Plan        plan;
    int32_t               numCols;        /* number of columns to check for uniqueness */
    pg_parser_AttrNumber* uniqColIdx;     /* their indexes in the target list */
    uint32_t*             uniqOperators;  /* equality operators to compare with */
    uint32_t*             uniqCollations; /* collations for equality comparisons */
} pg_parser_Unique;

typedef struct pg_parser_Gather
{
    pg_parser_Plan       plan;
    int32_t              num_workers;  /* planned number of worker processes */
    int32_t              rescan_param; /* ID of Param that signals a rescan, or -1 */
    bool                 single_copy;  /* don't execute plan more than once */
    bool                 invisible;    /* suppress EXPLAIN display (for testing)? */
    pg_parser_Bitmapset* initParam;    /* param id's of initplans which are referred
                                        * at gather or one of it's child node */
} pg_parser_Gather;

typedef struct pg_parser_GatherMerge
{
    pg_parser_Plan plan;
    int32_t        num_workers;  /* planned number of worker processes */
    int32_t        rescan_param; /* ID of Param that signals a rescan, or -1 */
    /* remaining fields are just like the sort-key info in struct Sort */
    int32_t               numCols;       /* number of sort-key columns */
    pg_parser_AttrNumber* sortColIdx;    /* their indexes in the target list */
    uint32_t*             sortOperators; /* OIDs of operators to sort them by */
    uint32_t*             collations;    /* OIDs of collations */
    bool*                 nullsFirst;    /* NULLS FIRST/LAST directions */
    pg_parser_Bitmapset*  initParam;     /* param id's of initplans which are referred
                                          * at gather merge or one of it's child node */
} pg_parser_GatherMerge;

typedef struct pg_parser_Hash
{
    pg_parser_Plan plan;

    /*
     * pg_parser_List of expressions to be hashed for tuples from Hash's outer plan,
     * needed to put them into the hashtable.
     */
    pg_parser_List*      hashkeys;    /* hash keys for the hashjoin condition */
    uint32_t             skewTable;   /* outer join key's table OID, or INVALIDOID */
    pg_parser_AttrNumber skewColumn;  /* outer join key's column #, or zero */
    bool                 skewInherit; /* is outer join rel an inheritance tree? */
    /* all other info is in the parent HashJoin node */
    double rows_total; /* estimate total rows if parallel_aware */
} pg_parser_Hash;

typedef enum pg_parser_SetOpCmd
{
    SETOPCMD_INTERSECT,
    SETOPCMD_INTERSECT_ALL,
    SETOPCMD_EXCEPT,
    SETOPCMD_EXCEPT_ALL
} pg_parser_SetOpCmd;

typedef enum pg_parser_SetOpStrategy
{
    SETOP_SORTED, /* input must be sorted */
    SETOP_HASHED  /* use internal hashtable */
} pg_parser_SetOpStrategy;

typedef struct pg_parser_SetOp
{
    pg_parser_Plan          plan;
    pg_parser_SetOpCmd      cmd;        /* what to do, see nodes.h */
    pg_parser_SetOpStrategy strategy;   /* how to do it, see nodes.h */
    int32_t                 numCols;    /* number of columns to check for
                                         * duplicate-ness */
    pg_parser_AttrNumber* dupColIdx;    /* their indexes in the target list */
    uint32_t*             dupOperators; /* equality operators to compare with */
    uint32_t*             dupCollations;
    pg_parser_AttrNumber  flagColIdx; /* where is the flag column, if any */
    int32_t               firstFlag;  /* flag value for first input relation */
    int64_t               numGroups;  /* estimated number of groups in input */
} pg_parser_SetOp;

typedef struct pg_parser_LockRows
{
    pg_parser_Plan  plan;
    pg_parser_List* rowMarks; /* a list of PlanRowMark's */
    int32_t         epqParam; /* ID of Param for EvalPlanQual re-eval */
} pg_parser_LockRows;

typedef struct pg_parser_Limit
{
    pg_parser_Plan  plan;
    pg_parser_Node* limitOffset; /* OFFSET parameter, or NULL if none */
    pg_parser_Node* limitCount;  /* COUNT parameter, or NULL if none */
} pg_parser_Limit;

typedef enum pg_parser_RowMarkType
{
    ROW_MARK_EXCLUSIVE,      /* obtain exclusive tuple lock */
    ROW_MARK_NOKEYEXCLUSIVE, /* obtain no-key exclusive tuple lock */
    ROW_MARK_SHARE,          /* obtain shared tuple lock */
    ROW_MARK_KEYSHARE,       /* obtain keyshare tuple lock */
    ROW_MARK_REFERENCE,      /* just fetch the TID, don't lock it */
    ROW_MARK_COPY            /* physically copy the row value */
} pg_parser_RowMarkType;

#define pg_parser_RowMarkRequiresRowShareLock(marktype) ((marktype) <= ROW_MARK_KEYSHARE)

typedef struct pg_parser_PlanRowMark
{
    pg_parser_NodeTag            type;
    pg_parser_Index              rti;          /* range table index of markable relation */
    pg_parser_Index              prti;         /* range table index of parent relation */
    pg_parser_Index              rowmarkId;    /* unique identifier for resjunk columns */
    pg_parser_RowMarkType        markType;     /* see enum above */
    int32_t                      allMarkTypes; /* OR of (1<<markType) for all children */
    pg_parser_LockClauseStrength strength;     /* LockingClause's strength, or LCS_NONE */
    pg_parser_LockWaitPolicy     waitPolicy;   /* NOWAIT and SKIP LOCKED options */
    bool                         isParent;     /* true if this is a "dummy" parent entry */
} pg_parser_PlanRowMark;

typedef struct pg_parser_PartitionedRelPruneInfo
{
    pg_parser_NodeTag    type;
    pg_parser_Index      rtindex;       /* RT index of partition rel for this level */
    pg_parser_Bitmapset* present_parts; /* Indexes of all partitions which subplans or
                                         * subparts are present for */
    int32_t   nparts;                   /* Length of the following arrays: */
    int32_t*  subplan_map;              /* subplan index by partition index, or -1 */
    int32_t*  subpart_map;              /* subpart index by partition index, or -1 */
    uint32_t* relid_map;                /* relation OID by partition index, or 0 */

    /*
     * initial_pruning_steps shows how to prune during executor startup (i.e.,
     * without use of any PARAM_EXEC Params); it is NIL if no startup pruning
     * is required.  exec_pruning_steps shows how to prune with PARAM_EXEC
     * Params; it is NIL if no per-scan pruning is required.
     */
    pg_parser_List*      initial_pruning_steps; /* pg_parser_List of PartitionPruneStep */
    pg_parser_List*      exec_pruning_steps;    /* pg_parser_List of PartitionPruneStep */
    pg_parser_Bitmapset* execparamids;          /* All PARAM_EXEC Param IDs in
                                                 * exec_pruning_steps */
} pg_parser_PartitionedRelPruneInfo;

/*
 * Abstract pg_parser_Node type for partition pruning steps (there are no concrete
 * Nodes of this type).
 *
 * step_id is the global identifier of the step within its pruning context.
 */
typedef struct pg_parser_PartitionPruneStep
{
    pg_parser_NodeTag type;
    int32_t           step_id;
} pg_parser_PartitionPruneStep;

typedef struct pg_parser_PartitionPruneStepOp
{
    pg_parser_PartitionPruneStep step;

    uint16_t             opstrategy;
    pg_parser_List*      exprs;
    pg_parser_List*      cmpfns;
    pg_parser_Bitmapset* nullkeys;
} pg_parser_PartitionPruneStepOp;

typedef enum pg_parser_PartitionPruneCombineOp
{
    PARTPRUNE_COMBINE_UNION,
    PARTPRUNE_COMBINE_INTERSECT
} pg_parser_PartitionPruneCombineOp;

typedef struct pg_parser_PartitionPruneStepCombine
{
    pg_parser_PartitionPruneStep step;

    pg_parser_PartitionPruneCombineOp combineOp;
    pg_parser_List*                   source_stepids;
} pg_parser_PartitionPruneStepCombine;

typedef struct pg_parser_PlanInvalItem
{
    pg_parser_NodeTag type;
    int32_t           cacheId;   /* a syscache ID, see utils/syscache.h */
    uint32_t          hashValue; /* hash value of object's cache lookup key */
} pg_parser_PlanInvalItem;

typedef struct pg_parser_SubPlan
{
    pg_parser_Expr xpr;
    /* Fields copied from original SubLink: */
    pg_parser_SubLinkType subLinkType; /* see above */
    /* The combining operators, transformed to an executable expression: */
    pg_parser_Node* testexpr; /* OpExpr or RowCompareExpr expression tree */
    pg_parser_List* paramIds; /* IDs of Params embedded in the above */
    /* Identification of the Plan tree to use: */
    int32_t plan_id; /* Index (from 1) in PlannedStmt.subplans */
    /* Identification of the SubPlan for EXPLAIN and debugging purposes: */
    char* plan_name; /* A name assigned during planning */
    /* Extra data useful for determining subplan's output type: */
    uint32_t firstColType;      /* Type of first column of subplan result */
    int32_t  firstColTypmod;    /* Typmod of first column of subplan result */
    uint32_t firstColCollation; /* Collation of first column of subplan
                                 * result */
    /* Information about execution strategy: */
    bool useHashTable;   /* true to store subselect output in a hash
                          * table (implies we are doing "IN") */
    bool unknownEqFalse; /* true if it's okay to return FALSE when the
                          * spec result is UNKNOWN; this allows much
                          * simpler handling of null values */
    bool parallel_safe;  /* is the subplan parallel-safe? */
    /* Note: parallel_safe does not consider contents of testexpr or args */
    /* Information for passing params into and out of the subselect: */
    /* setParam and parParam are lists of integers (param IDs) */
    pg_parser_List* setParam; /* initplan subqueries have to set these
                               * Params for parent plan */
    pg_parser_List* parParam; /* indices of input Params from parent plan */
    pg_parser_List* args;     /* exprs to pass as parParam values */
    /* Estimated execution costs: */
    double startup_cost;  /* one-time setup cost */
    double per_call_cost; /* cost for each subplan evaluation */
} pg_parser_SubPlan;

typedef struct pg_parser_AlternativeSubPlan
{
    pg_parser_Expr  xpr;
    pg_parser_List* subplans; /* SubPlan(s) with equivalent results */
} pg_parser_AlternativeSubPlan;

/* ExtensibleNode cannot be parsed on frontend */
#if 0
typedef struct pg_parser_ExtensibleNode
{
    pg_parser_NodeTag     type;
    const char              *extnodename;    /* identifier of ExtensibleNodeMethods */
} pg_parser_ExtensibleNode;

typedef struct pg_parser_ExtensibleNodeMethods
{
    const char *extnodename;
    size_t       node_size;
    void        (*nodeCopy) (struct pg_parser_ExtensibleNode *newnode,
                             const struct pg_parser_ExtensibleNode *oldnode);
    bool        (*nodeEqual) (const struct pg_parser_ExtensibleNode *a,
                              const struct pg_parser_ExtensibleNode *b);
    void        (*nodeOut) (struct pg_parser_StringInfoData *str,
                            const struct pg_parser_ExtensibleNode *node);
    void        (*nodeRead) (struct pg_parser_ExtensibleNode *node);
} pg_parser_ExtensibleNodeMethods;
#endif

typedef struct pg_parser_PartitionBoundSpec
{
    pg_parser_NodeTag type;

    char strategy;   /* see PARTITION_STRATEGY codes above */
    bool is_default; /* is it a default partition bound? */

    /* Partitioning info for HASH strategy: */
    int32_t modulus;
    int32_t remainder;

    /* Partitioning info for LIST strategy: */
    pg_parser_List* listdatums; /* List of Consts (or A_Consts in raw tree) */

    /* Partitioning info for RANGE strategy: */
    pg_parser_List* lowerdatums; /* List of PartitionRangeDatums */
    pg_parser_List* upperdatums; /* List of PartitionRangeDatums */

    int32_t location; /* token location, or -1 if unknown */
} pg_parser_PartitionBoundSpec;

typedef enum pg_parser_PartitionRangeDatumKind
{
    PARTITION_RANGE_DATUM_MINVALUE = -1, /* less than any other value */
    PARTITION_RANGE_DATUM_VALUE = 0,     /* a specific (bounded) value */
    PARTITION_RANGE_DATUM_MAXVALUE = 1   /* greater than any other value */
} pg_parser_PartitionRangeDatumKind;

typedef struct pg_parser_PartitionRangeDatum
{
    pg_parser_NodeTag type;

    pg_parser_PartitionRangeDatumKind kind;
    pg_parser_Node*                   value; /* Const (or A_Const in raw tree), if kind is
                                              * PARTITION_RANGE_DATUM_VALUE, else NULL */

    int32_t location; /* token location, or -1 if unknown */
} pg_parser_PartitionRangeDatum;

/* string to node end */

typedef enum pg_parser_ParseExprKind
{
    EXPR_KIND_NONE = 0,             /* "not in an expression" */
    EXPR_KIND_OTHER,                /* reserved for extensions */
    EXPR_KIND_JOIN_ON,              /* JOIN ON */
    EXPR_KIND_JOIN_USING,           /* JOIN USING */
    EXPR_KIND_FROM_SUBSELECT,       /* sub-SELECT in FROM clause */
    EXPR_KIND_FROM_FUNCTION,        /* function in FROM clause */
    EXPR_KIND_WHERE,                /* WHERE */
    EXPR_KIND_HAVING,               /* HAVING */
    EXPR_KIND_FILTER,               /* FILTER */
    EXPR_KIND_WINDOW_PARTITION,     /* window definition PARTITION BY */
    EXPR_KIND_WINDOW_ORDER,         /* window definition ORDER BY */
    EXPR_KIND_WINDOW_FRAME_RANGE,   /* window frame clause with RANGE */
    EXPR_KIND_WINDOW_FRAME_ROWS,    /* window frame clause with ROWS */
    EXPR_KIND_WINDOW_FRAME_GROUPS,  /* window frame clause with GROUPS */
    EXPR_KIND_SELECT_TARGET,        /* SELECT target list item */
    EXPR_KIND_INSERT_TARGET,        /* INSERT target list item */
    EXPR_KIND_UPDATE_SOURCE,        /* UPDATE assignment source item */
    EXPR_KIND_UPDATE_TARGET,        /* UPDATE assignment target item */
    EXPR_KIND_GROUP_BY,             /* GROUP BY */
    EXPR_KIND_ORDER_BY,             /* ORDER BY */
    EXPR_KIND_DISTINCT_ON,          /* DISTINCT ON */
    EXPR_KIND_LIMIT,                /* LIMIT */
    EXPR_KIND_OFFSET,               /* OFFSET */
    EXPR_KIND_RETURNING,            /* RETURNING */
    EXPR_KIND_VALUES,               /* VALUES */
    EXPR_KIND_VALUES_SINGLE,        /* single-row VALUES (in INSERT only) */
    EXPR_KIND_CHECK_CONSTRAINT,     /* CHECK constraint for a table */
    EXPR_KIND_DOMAIN_CHECK,         /* CHECK constraint for a domain */
    EXPR_KIND_COLUMN_DEFAULT,       /* default value for a table column */
    EXPR_KIND_FUNCTION_DEFAULT,     /* default parameter value for function */
    EXPR_KIND_INDEX_EXPRESSION,     /* index expression */
    EXPR_KIND_INDEX_PREDICATE,      /* index predicate */
    EXPR_KIND_ALTER_COL_TRANSFORM,  /* transform expr in ALTER COLUMN TYPE */
    EXPR_KIND_EXECUTE_PARAMETER,    /* parameter value in EXECUTE */
    EXPR_KIND_TRIGGER_WHEN,         /* WHEN condition in CREATE TRIGGER */
    EXPR_KIND_POLICY,               /* USING or WITH CHECK expr in policy */
    EXPR_KIND_PARTITION_BOUND,      /* partition bound expression */
    EXPR_KIND_PARTITION_EXPRESSION, /* PARTITION BY expression */
    EXPR_KIND_CALL_ARGUMENT,        /* procedure argument in CALL */
    EXPR_KIND_COPY_WHERE,           /* WHERE condition in COPY FROM */
    EXPR_KIND_GENERATED_COLUMN,     /* generation expression for a column */
} pg_parser_ParseExprKind;

typedef struct pg_parser_deparse_context
{
    pg_parser_StringInfo    buf;              /* output buffer to append to */
    pg_parser_List*         namespaces;       /* List of deparse_namespace nodes */
    pg_parser_List*         windowClause;     /* Current query level's WINDOW clause */
    pg_parser_List*         windowTList;      /* targetlist for resolving WINDOW clause */
    int                     prettyFlags;      /* enabling of pretty-print functions */
    int                     wrapColumn;       /* max line length, or -1 for no limit */
    int                     indentLevel;      /* current indent level for prettyprint */
    bool                    varprefix;        /* true to print prefixes on Vars */
    pg_parser_ParseExprKind special_exprkind; /* set only for exprkinds needing special
                                               * handling */
    pg_parser_translog_convertinfo_with_zic* zicinfo;
    pg_parser_nodetree*                      nodetree;
} pg_parser_deparse_context;
#endif
