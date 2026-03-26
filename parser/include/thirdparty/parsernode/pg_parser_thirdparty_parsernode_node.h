#ifndef PG_PARSER_THIRDPARTY_PARSERNODE_NODE_H
#define PG_PARSER_THIRDPARTY_PARSERNODE_NODE_H

/*
 * The first field of every node is pg_parser_NodeTag. Each node created (with pg_parser_makeNode)
 * will have one of the following tags as the value of its first field.
 *
 * Note that inserting or deleting node types changes the numbers of other
 * node types later in the list.  This is no problem during development, since
 * the node numbers are never stored on disk.  But don't do it in a released
 * branch, because that would represent an ABI break for extensions.
 */
typedef enum pg_parser_NodeTag
{
    T_pg_parser_Invalid = 0,

    /*
     * TAGS FOR EXECUTOR NODES (execnodes.h)
     */
    T_pg_parser_IndexInfo,
    T_pg_parser_ExprContext,
    T_pg_parser_ProjectionInfo,
    T_pg_parser_JunkFilter,
    T_pg_parser_OnConflictSetState,
    T_pg_parser_ResultRelInfo,
    T_pg_parser_EState,
    T_pg_parser_TupleTableSlot,

    /*
     * TAGS FOR PLAN NODES (plannodes.h)
     */
    T_pg_parser_Plan,
    T_pg_parser_Result,
    T_pg_parser_ProjectSet,
    T_pg_parser_ModifyTable,
    T_pg_parser_Append,
    T_pg_parser_MergeAppend,
    T_pg_parser_RecursiveUnion,
    T_pg_parser_BitmapAnd,
    T_pg_parser_BitmapOr,
    T_pg_parser_Scan,
    T_pg_parser_SeqScan,
    T_pg_parser_SampleScan,
    T_pg_parser_IndexScan,
    T_pg_parser_IndexOnlyScan,
    T_pg_parser_BitmapIndexScan,
    T_pg_parser_BitmapHeapScan,
    T_pg_parser_TidScan,
    T_pg_parser_SubqueryScan,
    T_pg_parser_FunctionScan,
    T_pg_parser_ValuesScan,
    T_pg_parser_TableFuncScan,
    T_pg_parser_CteScan,
    T_pg_parser_NamedTuplestoreScan,
    T_pg_parser_WorkTableScan,
    T_pg_parser_ForeignScan,
    T_pg_parser_CustomScan,
    T_pg_parser_Join,
    T_pg_parser_NestLoop,
    T_pg_parser_MergeJoin,
    T_pg_parser_HashJoin,
    T_pg_parser_Material,
    T_pg_parser_Sort,
    T_pg_parser_Group,
    T_pg_parser_Agg,
    T_pg_parser_WindowAgg,
    T_pg_parser_Unique,
    T_pg_parser_Gather,
    T_pg_parser_GatherMerge,
    T_pg_parser_Hash,
    T_pg_parser_SetOp,
    T_pg_parser_LockRows,
    T_pg_parser_Limit,
    /* these aren't subclasses of Plan: */
    T_pg_parser_NestLoopParam,
    T_pg_parser_PlanRowMark,
    T_pg_parser_PartitionPruneInfo,
    T_pg_parser_PartitionedRelPruneInfo,
    T_pg_parser_PartitionPruneStepOp,
    T_pg_parser_PartitionPruneStepCombine,
    T_pg_parser_PlanInvalItem,

    /*
     * TAGS FOR PLAN STATE NODES (execnodes.h)
     *
     * These should correspond one-to-one with Plan node types.
     */
    T_pg_parser_PlanState,
    T_pg_parser_ResultState,
    T_pg_parser_ProjectSetState,
    T_pg_parser_ModifyTableState,
    T_pg_parser_AppendState,
    T_pg_parser_MergeAppendState,
    T_pg_parser_RecursiveUnionState,
    T_pg_parser_BitmapAndState,
    T_pg_parser_BitmapOrState,
    T_pg_parser_ScanState,
    T_pg_parser_SeqScanState,
    T_pg_parser_SampleScanState,
    T_pg_parser_IndexScanState,
    T_pg_parser_IndexOnlyScanState,
    T_pg_parser_BitmapIndexScanState,
    T_pg_parser_BitmapHeapScanState,
    T_pg_parser_TidScanState,
    T_pg_parser_SubqueryScanState,
    T_pg_parser_FunctionScanState,
    T_pg_parser_TableFuncScanState,
    T_pg_parser_ValuesScanState,
    T_pg_parser_CteScanState,
    T_pg_parser_NamedTuplestoreScanState,
    T_pg_parser_WorkTableScanState,
    T_pg_parser_ForeignScanState,
    T_pg_parser_CustomScanState,
    T_pg_parser_JoinState,
    T_pg_parser_NestLoopState,
    T_pg_parser_MergeJoinState,
    T_pg_parser_HashJoinState,
    T_pg_parser_MaterialState,
    T_pg_parser_SortState,
    T_pg_parser_GroupState,
    T_pg_parser_AggState,
    T_pg_parser_WindowAggState,
    T_pg_parser_UniqueState,
    T_pg_parser_GatherState,
    T_pg_parser_GatherMergeState,
    T_pg_parser_HashState,
    T_pg_parser_SetOpState,
    T_pg_parser_LockRowsState,
    T_pg_parser_LimitState,

    /*
     * TAGS FOR PRIMITIVE NODES (primnodes.h)
     */
    T_pg_parser_Alias,
    T_pg_parser_RangeVar,
    T_pg_parser_TableFunc,
    T_pg_parser_Expr,
    T_pg_parser_Var,
    T_pg_parser_Const,
    T_pg_parser_Param,
    T_pg_parser_Aggref,
    T_pg_parser_GroupingFunc,
    T_pg_parser_WindowFunc,
    T_pg_parser_SubscriptingRef,
    T_pg_parser_FuncExpr,
    T_pg_parser_NamedArgExpr,
    T_pg_parser_OpExpr,
    T_pg_parser_DistinctExpr,
    T_pg_parser_NullIfExpr,
    T_pg_parser_ScalarArrayOpExpr,
    T_pg_parser_BoolExpr,
    T_pg_parser_SubLink,
    T_pg_parser_SubPlan,
    T_pg_parser_AlternativeSubPlan,
    T_pg_parser_FieldSelect,
    T_pg_parser_FieldStore,
    T_pg_parser_RelabelType,
    T_pg_parser_CoerceViaIO,
    T_pg_parser_ArrayCoerceExpr,
    T_pg_parser_ConvertRowtypeExpr,
    T_pg_parser_CollateExpr,
    T_pg_parser_CaseExpr,
    T_pg_parser_CaseWhen,
    T_pg_parser_CaseTestExpr,
    T_pg_parser_ArrayExpr,
    T_pg_parser_RowExpr,
    T_pg_parser_RowCompareExpr,
    T_pg_parser_CoalesceExpr,
    T_pg_parser_MinMaxExpr,
    T_pg_parser_SQLValueFunction,
    T_pg_parser_XmlExpr,
    T_pg_parser_NullTest,
    T_pg_parser_BooleanTest,
    T_pg_parser_CoerceToDomain,
    T_pg_parser_CoerceToDomainValue,
    T_pg_parser_SetToDefault,
    T_pg_parser_CurrentOfExpr,
    T_pg_parser_NextValueExpr,
    T_pg_parser_InferenceElem,
    T_pg_parser_TargetEntry,
    T_pg_parser_RangeTblRef,
    T_pg_parser_JoinExpr,
    T_pg_parser_FromExpr,
    T_pg_parser_OnConflictExpr,
    T_pg_parser_IntoClause,

    /*
     * TAGS FOR EXPRESSION STATE NODES (execnodes.h)
     *
     * ExprState represents the evaluation state for a whole expression tree.
     * Most pg_parser_Expr-based plan nodes do not have a corresponding expression state
     * node, they're fully handled within execExpr* - but sometimes the state
     * needs to be shared with other parts of the executor, as for example
     * with AggrefExprState, which nodeAgg.c has to modify.
     */
    T_pg_parser_ExprState,
    T_pg_parser_AggrefExprState,
    T_pg_parser_WindowFuncExprState,
    T_pg_parser_SetExprState,
    T_pg_parser_SubPlanState,
    T_pg_parser_AlternativeSubPlanState,
    T_pg_parser_DomainConstraintState,

    /*
     * TAGS FOR PLANNER NODES (pathnodes.h)
     */
    T_pg_parser_PlannerInfo,
    T_pg_parser_PlannerGlobal,
    T_pg_parser_RelOptInfo,
    T_pg_parser_IndexOptInfo,
    T_pg_parser_ForeignKeyOptInfo,
    T_pg_parser_ParamPathInfo,
    T_pg_parser_Path,
    T_pg_parser_IndexPath,
    T_pg_parser_BitmapHeapPath,
    T_pg_parser_BitmapAndPath,
    T_pg_parser_BitmapOrPath,
    T_pg_parser_TidPath,
    T_pg_parser_SubqueryScanPath,
    T_pg_parser_ForeignPath,
    T_pg_parser_CustomPath,
    T_pg_parser_NestPath,
    T_pg_parser_MergePath,
    T_pg_parser_HashPath,
    T_pg_parser_AppendPath,
    T_pg_parser_MergeAppendPath,
    T_pg_parser_GroupResultPath,
    T_pg_parser_MaterialPath,
    T_pg_parser_UniquePath,
    T_pg_parser_GatherPath,
    T_pg_parser_GatherMergePath,
    T_pg_parser_ProjectionPath,
    T_pg_parser_ProjectSetPath,
    T_pg_parser_SortPath,
    T_pg_parser_GroupPath,
    T_pg_parser_UpperUniquePath,
    T_pg_parser_AggPath,
    T_pg_parser_GroupingSetsPath,
    T_pg_parser_MinMaxAggPath,
    T_pg_parser_WindowAggPath,
    T_pg_parser_SetOpPath,
    T_pg_parser_RecursiveUnionPath,
    T_pg_parser_LockRowsPath,
    T_pg_parser_ModifyTablePath,
    T_pg_parser_LimitPath,
    /* these aren't subclasses of Path: */
    T_pg_parser_EquivalenceClass,
    T_pg_parser_EquivalenceMember,
    T_pg_parser_PathKey,
    T_pg_parser_PathTarget,
    T_pg_parser_RestrictInfo,
    T_pg_parser_IndexClause,
    T_pg_parser_PlaceHolderVar,
    T_pg_parser_SpecialJoinInfo,
    T_pg_parser_AppendRelInfo,
    T_pg_parser_PlaceHolderInfo,
    T_pg_parser_MinMaxAggInfo,
    T_pg_parser_PlannerParamItem,
    T_pg_parser_RollupData,
    T_pg_parser_GroupingSetData,
    T_pg_parser_StatisticExtInfo,

    /*
     * TAGS FOR MEMORY NODES (memnodes.h)
     */
    T_pg_parser_MemoryContext,
    T_pg_parser_AllocSetContext,
    T_pg_parser_SlabContext,
    T_pg_parser_GenerationContext,

    /*
     * TAGS FOR VALUE NODES (value.h)
     */
    T_pg_parser_Value,
    T_pg_parser_Integer,
    T_pg_parser_Float,
    T_pg_parser_String,
    T_pg_parser_BitString,
    T_pg_parser_Null,

    /*
     * TAGS FOR LIST NODES (pg_list.h)
     */
    T_pg_parser_List,
    T_pg_parser_IntList,
    T_pg_parser_OidList,

    /*
     * TAGS FOR EXTENSIBLE NODES (extensible.h)
     */
    T_pg_parser_ExtensibleNode,

    /*
     * TAGS FOR STATEMENT NODES (mostly in parsenodes.h)
     */
    T_pg_parser_RawStmt,
    T_pg_parser_Query,
    T_pg_parser_PlannedStmt,
    T_pg_parser_InsertStmt,
    T_pg_parser_DeleteStmt,
    T_pg_parser_UpdateStmt,
    T_pg_parser_SelectStmt,
    T_pg_parser_AlterTableStmt,
    T_pg_parser_AlterTableCmd,
    T_pg_parser_AlterDomainStmt,
    T_pg_parser_SetOperationStmt,
    T_pg_parser_GrantStmt,
    T_pg_parser_GrantRoleStmt,
    T_pg_parser_AlterDefaultPrivilegesStmt,
    T_pg_parser_ClosePortalStmt,
    T_pg_parser_ClusterStmt,
    T_pg_parser_CopyStmt,
    T_pg_parser_CreateStmt,
    T_pg_parser_DefineStmt,
    T_pg_parser_DropStmt,
    T_pg_parser_TruncateStmt,
    T_pg_parser_CommentStmt,
    T_pg_parser_FetchStmt,
    T_pg_parser_IndexStmt,
    T_pg_parser_CreateFunctionStmt,
    T_pg_parser_AlterFunctionStmt,
    T_pg_parser_DoStmt,
    T_pg_parser_RenameStmt,
    T_pg_parser_RuleStmt,
    T_pg_parser_NotifyStmt,
    T_pg_parser_ListenStmt,
    T_pg_parser_UnlistenStmt,
    T_pg_parser_TransactionStmt,
    T_pg_parser_ViewStmt,
    T_pg_parser_LoadStmt,
    T_pg_parser_CreateDomainStmt,
    T_pg_parser_CreatedbStmt,
    T_pg_parser_DropdbStmt,
    T_pg_parser_VacuumStmt,
    T_pg_parser_ExplainStmt,
    T_pg_parser_CreateTableAsStmt,
    T_pg_parser_CreateSeqStmt,
    T_pg_parser_AlterSeqStmt,
    T_pg_parser_VariableSetStmt,
    T_pg_parser_VariableShowStmt,
    T_pg_parser_DiscardStmt,
    T_pg_parser_CreateTrigStmt,
    T_pg_parser_CreatePLangStmt,
    T_pg_parser_CreateRoleStmt,
    T_pg_parser_AlterRoleStmt,
    T_pg_parser_DropRoleStmt,
    T_pg_parser_LockStmt,
    T_pg_parser_ConstraintsSetStmt,
    T_pg_parser_ReindexStmt,
    T_pg_parser_CheckPointStmt,
    T_pg_parser_CreateSchemaStmt,
    T_pg_parser_AlterDatabaseStmt,
    T_pg_parser_AlterDatabaseSetStmt,
    T_pg_parser_AlterRoleSetStmt,
    T_pg_parser_CreateConversionStmt,
    T_pg_parser_CreateCastStmt,
    T_pg_parser_CreateOpClassStmt,
    T_pg_parser_CreateOpFamilyStmt,
    T_pg_parser_AlterOpFamilyStmt,
    T_pg_parser_PrepareStmt,
    T_pg_parser_ExecuteStmt,
    T_pg_parser_DeallocateStmt,
    T_pg_parser_DeclareCursorStmt,
    T_pg_parser_CreateTableSpaceStmt,
    T_pg_parser_DropTableSpaceStmt,
    T_pg_parser_AlterObjectDependsStmt,
    T_pg_parser_AlterObjectSchemaStmt,
    T_pg_parser_AlterOwnerStmt,
    T_pg_parser_AlterOperatorStmt,
    T_pg_parser_DropOwnedStmt,
    T_pg_parser_ReassignOwnedStmt,
    T_pg_parser_CompositeTypeStmt,
    T_pg_parser_CreateEnumStmt,
    T_pg_parser_CreateRangeStmt,
    T_pg_parser_AlterEnumStmt,
    T_pg_parser_AlterTSDictionaryStmt,
    T_pg_parser_AlterTSConfigurationStmt,
    T_pg_parser_CreateFdwStmt,
    T_pg_parser_AlterFdwStmt,
    T_pg_parser_CreateForeignServerStmt,
    T_pg_parser_AlterForeignServerStmt,
    T_pg_parser_CreateUserMappingStmt,
    T_pg_parser_AlterUserMappingStmt,
    T_pg_parser_DropUserMappingStmt,
    T_pg_parser_AlterTableSpaceOptionsStmt,
    T_pg_parser_AlterTableMoveAllStmt,
    T_pg_parser_SecLabelStmt,
    T_pg_parser_CreateForeignTableStmt,
    T_pg_parser_ImportForeignSchemaStmt,
    T_pg_parser_CreateExtensionStmt,
    T_pg_parser_AlterExtensionStmt,
    T_pg_parser_AlterExtensionContentsStmt,
    T_pg_parser_CreateEventTrigStmt,
    T_pg_parser_AlterEventTrigStmt,
    T_pg_parser_RefreshMatViewStmt,
    T_pg_parser_ReplicaIdentityStmt,
    T_pg_parser_AlterSystemStmt,
    T_pg_parser_CreatePolicyStmt,
    T_pg_parser_AlterPolicyStmt,
    T_pg_parser_CreateTransformStmt,
    T_pg_parser_CreateAmStmt,
    T_pg_parser_CreatePublicationStmt,
    T_pg_parser_AlterPublicationStmt,
    T_pg_parser_CreateSubscriptionStmt,
    T_pg_parser_AlterSubscriptionStmt,
    T_pg_parser_DropSubscriptionStmt,
    T_pg_parser_CreateStatsStmt,
    T_pg_parser_AlterCollationStmt,
    T_pg_parser_CallStmt,

    /*
     * TAGS FOR PARSE TREE NODES (parsenodes.h)
     */
    T_pg_parser_A_Expr,
    T_pg_parser_ColumnRef,
    T_pg_parser_ParamRef,
    T_pg_parser_A_Const,
    T_pg_parser_FuncCall,
    T_pg_parser_A_Star,
    T_pg_parser_A_Indices,
    T_pg_parser_A_Indirection,
    T_pg_parser_A_ArrayExpr,
    T_pg_parser_ResTarget,
    T_pg_parser_MultiAssignRef,
    T_pg_parser_TypeCast,
    T_pg_parser_CollateClause,
    T_pg_parser_SortBy,
    T_pg_parser_WindowDef,
    T_pg_parser_RangeSubselect,
    T_pg_parser_RangeFunction,
    T_pg_parser_RangeTableSample,
    T_pg_parser_RangeTableFunc,
    T_pg_parser_RangeTableFuncCol,
    T_pg_parser_TypeName,
    T_pg_parser_ColumnDef,
    T_pg_parser_IndexElem,
    T_pg_parser_Constraint,
    T_pg_parser_DefElem,
    T_pg_parser_RangeTblEntry,
    T_pg_parser_RangeTblFunction,
    T_pg_parser_TableSampleClause,
    T_pg_parser_WithCheckOption,
    T_pg_parser_SortGroupClause,
    T_pg_parser_GroupingSet,
    T_pg_parser_WindowClause,
    T_pg_parser_ObjectWithArgs,
    T_pg_parser_AccessPriv,
    T_pg_parser_CreateOpClassItem,
    T_pg_parser_TableLikeClause,
    T_pg_parser_FunctionParameter,
    T_pg_parser_LockingClause,
    T_pg_parser_RowMarkClause,
    T_pg_parser_XmlSerialize,
    T_pg_parser_WithClause,
    T_pg_parser_InferClause,
    T_pg_parser_OnConflictClause,
    T_pg_parser_CommonTableExpr,
    T_pg_parser_RoleSpec,
    T_pg_parser_TriggerTransition,
    T_pg_parser_PartitionElem,
    T_pg_parser_PartitionSpec,
    T_pg_parser_PartitionBoundSpec,
    T_pg_parser_PartitionRangeDatum,
    T_pg_parser_PartitionCmd,
    T_pg_parser_VacuumRelation,

    /*
     * TAGS FOR REPLICATION GRAMMAR PARSE NODES (replnodes.h)
     */
    T_pg_parser_IdentifySystemCmd,
    T_pg_parser_BaseBackupCmd,
    T_pg_parser_CreateReplicationSlotCmd,
    T_pg_parser_DropReplicationSlotCmd,
    T_pg_parser_StartReplicationCmd,
    T_pg_parser_TimeLineHistoryCmd,
    T_pg_parser_SQLCmd,

    /*
     * TAGS FOR RANDOM OTHER STUFF
     *
     * These are objects that aren't part of parse/plan/execute node tree
     * structures, but we give them pg_parser_NodeTags anyway for identification
     * purposes (usually because they are involved in APIs where we want to
     * pass multiple object types through the same pointer).
     */
    T_pg_parser_TriggerData,                 /* in commands/trigger.h */
    T_pg_parser_EventTriggerData,            /* in commands/evenT_pg_parser_trigger.h */
    T_pg_parser_ReturnSetInfo,               /* in nodes/execnodes.h */
    T_pg_parser_WindowObjectData,            /* private in nodeWindowAgg.c */
    T_pg_parser_TIDBitmap,                   /* in nodes/tidbitmap.h */
    T_pg_parser_InlineCodeBlock,             /* in nodes/parsenodes.h */
    T_pg_parser_FdwRoutine,                  /* in foreign/fdwapi.h */
    T_pg_parser_IndexAmRoutine,              /* in access/amapi.h */
    T_pg_parser_TableAmRoutine,              /* in access/tableam.h */
    T_pg_parser_TsmRoutine,                  /* in access/tsmapi.h */
    T_pg_parser_ForeignKeyCacheInfo,         /* in utils/rel.h */
    T_pg_parser_CallContext,                 /* in nodes/parsenodes.h */
    T_pg_parser_SupportRequestSimplify,      /* in nodes/supportnodes.h */
    T_pg_parser_SupportRequestSelectivity,   /* in nodes/supportnodes.h */
    T_pg_parser_SupportRequestCost,          /* in nodes/supportnodes.h */
    T_pg_parser_SupportRequestRows,          /* in nodes/supportnodes.h */
    T_pg_parser_SupportRequestIndexCondition /* in nodes/supportnodes.h */
} pg_parser_NodeTag;

#define PG_PARSER_PARTITION_STRATEGY_HASH  'h'
#define PG_PARSER_PARTITION_STRATEGY_LIST  'l'
#define PG_PARSER_PARTITION_STRATEGY_RANGE 'r'
/*
 * The first field of a node of any type is guaranteed to be the pg_parser_NodeTag.
 * Hence the type of any node can be gotten by casting it to pg_parser_Node. Declaring
 * a variable to be of pg_parser_Node * (instead of void *) can also facilitate
 * debugging.
 */
typedef struct pg_parser_Node
{
    pg_parser_NodeTag type;
} pg_parser_Node;

#define pg_parser_NodeTagType(nodeptr)      (((const pg_parser_Node*)(nodeptr))->type)

#define NODE_MCXT                           NULL

#define pg_parser_castNode(_type_, nodeptr) ((_type_*)(nodeptr))

#define pg_parser_newNode(size, tag)                                                          \
    ({                                                                                        \
        pg_parser_Node* _result;                                                              \
        pg_parser_AssertMacro((size) >= sizeof(pg_parser_Node)); /* need the tag, at least */ \
        pg_parser_mcxt_malloc(NODE_MCXT, (void**)&_result, size);                             \
        _result->type = (tag);                                                                \
        _result;                                                                              \
    })

#define pg_parser_makeNode(_type_) ((_type_*)pg_parser_newNode(sizeof(_type_), T_##_type_))
#endif
