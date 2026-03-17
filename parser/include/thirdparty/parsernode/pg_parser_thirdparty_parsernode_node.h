#ifndef XK_PG_PARSER_THIRDPARTY_PARSERNODE_NODE_H
#define XK_PG_PARSER_THIRDPARTY_PARSERNODE_NODE_H

/*
 * The first field of every node is xk_pg_parser_NodeTag. Each node created (with xk_pg_parser_makeNode)
 * will have one of the following tags as the value of its first field.
 *
 * Note that inserting or deleting node types changes the numbers of other
 * node types later in the list.  This is no problem during development, since
 * the node numbers are never stored on disk.  But don't do it in a released
 * branch, because that would represent an ABI break for extensions.
 */
typedef enum xk_pg_parser_NodeTag
{
    T_xk_pg_parser_Invalid = 0,

    /*
     * TAGS FOR EXECUTOR NODES (execnodes.h)
     */
    T_xk_pg_parser_IndexInfo,
    T_xk_pg_parser_ExprContext,
    T_xk_pg_parser_ProjectionInfo,
    T_xk_pg_parser_JunkFilter,
    T_xk_pg_parser_OnConflictSetState,
    T_xk_pg_parser_ResultRelInfo,
    T_xk_pg_parser_EState,
    T_xk_pg_parser_TupleTableSlot,

    /*
     * TAGS FOR PLAN NODES (plannodes.h)
     */
    T_xk_pg_parser_Plan,
    T_xk_pg_parser_Result,
    T_xk_pg_parser_ProjectSet,
    T_xk_pg_parser_ModifyTable,
    T_xk_pg_parser_Append,
    T_xk_pg_parser_MergeAppend,
    T_xk_pg_parser_RecursiveUnion,
    T_xk_pg_parser_BitmapAnd,
    T_xk_pg_parser_BitmapOr,
    T_xk_pg_parser_Scan,
    T_xk_pg_parser_SeqScan,
    T_xk_pg_parser_SampleScan,
    T_xk_pg_parser_IndexScan,
    T_xk_pg_parser_IndexOnlyScan,
    T_xk_pg_parser_BitmapIndexScan,
    T_xk_pg_parser_BitmapHeapScan,
    T_xk_pg_parser_TidScan,
    T_xk_pg_parser_SubqueryScan,
    T_xk_pg_parser_FunctionScan,
    T_xk_pg_parser_ValuesScan,
    T_xk_pg_parser_TableFuncScan,
    T_xk_pg_parser_CteScan,
    T_xk_pg_parser_NamedTuplestoreScan,
    T_xk_pg_parser_WorkTableScan,
    T_xk_pg_parser_ForeignScan,
    T_xk_pg_parser_CustomScan,
    T_xk_pg_parser_Join,
    T_xk_pg_parser_NestLoop,
    T_xk_pg_parser_MergeJoin,
    T_xk_pg_parser_HashJoin,
    T_xk_pg_parser_Material,
    T_xk_pg_parser_Sort,
    T_xk_pg_parser_Group,
    T_xk_pg_parser_Agg,
    T_xk_pg_parser_WindowAgg,
    T_xk_pg_parser_Unique,
    T_xk_pg_parser_Gather,
    T_xk_pg_parser_GatherMerge,
    T_xk_pg_parser_Hash,
    T_xk_pg_parser_SetOp,
    T_xk_pg_parser_LockRows,
    T_xk_pg_parser_Limit,
    /* these aren't subclasses of Plan: */
    T_xk_pg_parser_NestLoopParam,
    T_xk_pg_parser_PlanRowMark,
    T_xk_pg_parser_PartitionPruneInfo,
    T_xk_pg_parser_PartitionedRelPruneInfo,
    T_xk_pg_parser_PartitionPruneStepOp,
    T_xk_pg_parser_PartitionPruneStepCombine,
    T_xk_pg_parser_PlanInvalItem,

    /*
     * TAGS FOR PLAN STATE NODES (execnodes.h)
     *
     * These should correspond one-to-one with Plan node types.
     */
    T_xk_pg_parser_PlanState,
    T_xk_pg_parser_ResultState,
    T_xk_pg_parser_ProjectSetState,
    T_xk_pg_parser_ModifyTableState,
    T_xk_pg_parser_AppendState,
    T_xk_pg_parser_MergeAppendState,
    T_xk_pg_parser_RecursiveUnionState,
    T_xk_pg_parser_BitmapAndState,
    T_xk_pg_parser_BitmapOrState,
    T_xk_pg_parser_ScanState,
    T_xk_pg_parser_SeqScanState,
    T_xk_pg_parser_SampleScanState,
    T_xk_pg_parser_IndexScanState,
    T_xk_pg_parser_IndexOnlyScanState,
    T_xk_pg_parser_BitmapIndexScanState,
    T_xk_pg_parser_BitmapHeapScanState,
    T_xk_pg_parser_TidScanState,
    T_xk_pg_parser_SubqueryScanState,
    T_xk_pg_parser_FunctionScanState,
    T_xk_pg_parser_TableFuncScanState,
    T_xk_pg_parser_ValuesScanState,
    T_xk_pg_parser_CteScanState,
    T_xk_pg_parser_NamedTuplestoreScanState,
    T_xk_pg_parser_WorkTableScanState,
    T_xk_pg_parser_ForeignScanState,
    T_xk_pg_parser_CustomScanState,
    T_xk_pg_parser_JoinState,
    T_xk_pg_parser_NestLoopState,
    T_xk_pg_parser_MergeJoinState,
    T_xk_pg_parser_HashJoinState,
    T_xk_pg_parser_MaterialState,
    T_xk_pg_parser_SortState,
    T_xk_pg_parser_GroupState,
    T_xk_pg_parser_AggState,
    T_xk_pg_parser_WindowAggState,
    T_xk_pg_parser_UniqueState,
    T_xk_pg_parser_GatherState,
    T_xk_pg_parser_GatherMergeState,
    T_xk_pg_parser_HashState,
    T_xk_pg_parser_SetOpState,
    T_xk_pg_parser_LockRowsState,
    T_xk_pg_parser_LimitState,

    /*
     * TAGS FOR PRIMITIVE NODES (primnodes.h)
     */
    T_xk_pg_parser_Alias,
    T_xk_pg_parser_RangeVar,
    T_xk_pg_parser_TableFunc,
    T_xk_pg_parser_Expr,
    T_xk_pg_parser_Var,
    T_xk_pg_parser_Const,
    T_xk_pg_parser_Param,
    T_xk_pg_parser_Aggref,
    T_xk_pg_parser_GroupingFunc,
    T_xk_pg_parser_WindowFunc,
    T_xk_pg_parser_SubscriptingRef,
    T_xk_pg_parser_FuncExpr,
    T_xk_pg_parser_NamedArgExpr,
    T_xk_pg_parser_OpExpr,
    T_xk_pg_parser_DistinctExpr,
    T_xk_pg_parser_NullIfExpr,
    T_xk_pg_parser_ScalarArrayOpExpr,
    T_xk_pg_parser_BoolExpr,
    T_xk_pg_parser_SubLink,
    T_xk_pg_parser_SubPlan,
    T_xk_pg_parser_AlternativeSubPlan,
    T_xk_pg_parser_FieldSelect,
    T_xk_pg_parser_FieldStore,
    T_xk_pg_parser_RelabelType,
    T_xk_pg_parser_CoerceViaIO,
    T_xk_pg_parser_ArrayCoerceExpr,
    T_xk_pg_parser_ConvertRowtypeExpr,
    T_xk_pg_parser_CollateExpr,
    T_xk_pg_parser_CaseExpr,
    T_xk_pg_parser_CaseWhen,
    T_xk_pg_parser_CaseTestExpr,
    T_xk_pg_parser_ArrayExpr,
    T_xk_pg_parser_RowExpr,
    T_xk_pg_parser_RowCompareExpr,
    T_xk_pg_parser_CoalesceExpr,
    T_xk_pg_parser_MinMaxExpr,
    T_xk_pg_parser_SQLValueFunction,
    T_xk_pg_parser_XmlExpr,
    T_xk_pg_parser_NullTest,
    T_xk_pg_parser_BooleanTest,
    T_xk_pg_parser_CoerceToDomain,
    T_xk_pg_parser_CoerceToDomainValue,
    T_xk_pg_parser_SetToDefault,
    T_xk_pg_parser_CurrentOfExpr,
    T_xk_pg_parser_NextValueExpr,
    T_xk_pg_parser_InferenceElem,
    T_xk_pg_parser_TargetEntry,
    T_xk_pg_parser_RangeTblRef,
    T_xk_pg_parser_JoinExpr,
    T_xk_pg_parser_FromExpr,
    T_xk_pg_parser_OnConflictExpr,
    T_xk_pg_parser_IntoClause,

    /*
     * TAGS FOR EXPRESSION STATE NODES (execnodes.h)
     *
     * ExprState represents the evaluation state for a whole expression tree.
     * Most xk_pg_parser_Expr-based plan nodes do not have a corresponding expression state
     * node, they're fully handled within execExpr* - but sometimes the state
     * needs to be shared with other parts of the executor, as for example
     * with AggrefExprState, which nodeAgg.c has to modify.
     */
    T_xk_pg_parser_ExprState,
    T_xk_pg_parser_AggrefExprState,
    T_xk_pg_parser_WindowFuncExprState,
    T_xk_pg_parser_SetExprState,
    T_xk_pg_parser_SubPlanState,
    T_xk_pg_parser_AlternativeSubPlanState,
    T_xk_pg_parser_DomainConstraintState,

    /*
     * TAGS FOR PLANNER NODES (pathnodes.h)
     */
    T_xk_pg_parser_PlannerInfo,
    T_xk_pg_parser_PlannerGlobal,
    T_xk_pg_parser_RelOptInfo,
    T_xk_pg_parser_IndexOptInfo,
    T_xk_pg_parser_ForeignKeyOptInfo,
    T_xk_pg_parser_ParamPathInfo,
    T_xk_pg_parser_Path,
    T_xk_pg_parser_IndexPath,
    T_xk_pg_parser_BitmapHeapPath,
    T_xk_pg_parser_BitmapAndPath,
    T_xk_pg_parser_BitmapOrPath,
    T_xk_pg_parser_TidPath,
    T_xk_pg_parser_SubqueryScanPath,
    T_xk_pg_parser_ForeignPath,
    T_xk_pg_parser_CustomPath,
    T_xk_pg_parser_NestPath,
    T_xk_pg_parser_MergePath,
    T_xk_pg_parser_HashPath,
    T_xk_pg_parser_AppendPath,
    T_xk_pg_parser_MergeAppendPath,
    T_xk_pg_parser_GroupResultPath,
    T_xk_pg_parser_MaterialPath,
    T_xk_pg_parser_UniquePath,
    T_xk_pg_parser_GatherPath,
    T_xk_pg_parser_GatherMergePath,
    T_xk_pg_parser_ProjectionPath,
    T_xk_pg_parser_ProjectSetPath,
    T_xk_pg_parser_SortPath,
    T_xk_pg_parser_GroupPath,
    T_xk_pg_parser_UpperUniquePath,
    T_xk_pg_parser_AggPath,
    T_xk_pg_parser_GroupingSetsPath,
    T_xk_pg_parser_MinMaxAggPath,
    T_xk_pg_parser_WindowAggPath,
    T_xk_pg_parser_SetOpPath,
    T_xk_pg_parser_RecursiveUnionPath,
    T_xk_pg_parser_LockRowsPath,
    T_xk_pg_parser_ModifyTablePath,
    T_xk_pg_parser_LimitPath,
    /* these aren't subclasses of Path: */
    T_xk_pg_parser_EquivalenceClass,
    T_xk_pg_parser_EquivalenceMember,
    T_xk_pg_parser_PathKey,
    T_xk_pg_parser_PathTarget,
    T_xk_pg_parser_RestrictInfo,
    T_xk_pg_parser_IndexClause,
    T_xk_pg_parser_PlaceHolderVar,
    T_xk_pg_parser_SpecialJoinInfo,
    T_xk_pg_parser_AppendRelInfo,
    T_xk_pg_parser_PlaceHolderInfo,
    T_xk_pg_parser_MinMaxAggInfo,
    T_xk_pg_parser_PlannerParamItem,
    T_xk_pg_parser_RollupData,
    T_xk_pg_parser_GroupingSetData,
    T_xk_pg_parser_StatisticExtInfo,

    /*
     * TAGS FOR MEMORY NODES (memnodes.h)
     */
    T_xk_pg_parser_MemoryContext,
    T_xk_pg_parser_AllocSetContext,
    T_xk_pg_parser_SlabContext,
    T_xk_pg_parser_GenerationContext,

    /*
     * TAGS FOR VALUE NODES (value.h)
     */
    T_xk_pg_parser_Value,
    T_xk_pg_parser_Integer,
    T_xk_pg_parser_Float,
    T_xk_pg_parser_String,
    T_xk_pg_parser_BitString,
    T_xk_pg_parser_Null,

    /*
     * TAGS FOR LIST NODES (pg_list.h)
     */
    T_xk_pg_parser_List,
    T_xk_pg_parser_IntList,
    T_xk_pg_parser_OidList,

    /*
     * TAGS FOR EXTENSIBLE NODES (extensible.h)
     */
    T_xk_pg_parser_ExtensibleNode,

    /*
     * TAGS FOR STATEMENT NODES (mostly in parsenodes.h)
     */
    T_xk_pg_parser_RawStmt,
    T_xk_pg_parser_Query,
    T_xk_pg_parser_PlannedStmt,
    T_xk_pg_parser_InsertStmt,
    T_xk_pg_parser_DeleteStmt,
    T_xk_pg_parser_UpdateStmt,
    T_xk_pg_parser_SelectStmt,
    T_xk_pg_parser_AlterTableStmt,
    T_xk_pg_parser_AlterTableCmd,
    T_xk_pg_parser_AlterDomainStmt,
    T_xk_pg_parser_SetOperationStmt,
    T_xk_pg_parser_GrantStmt,
    T_xk_pg_parser_GrantRoleStmt,
    T_xk_pg_parser_AlterDefaultPrivilegesStmt,
    T_xk_pg_parser_ClosePortalStmt,
    T_xk_pg_parser_ClusterStmt,
    T_xk_pg_parser_CopyStmt,
    T_xk_pg_parser_CreateStmt,
    T_xk_pg_parser_DefineStmt,
    T_xk_pg_parser_DropStmt,
    T_xk_pg_parser_TruncateStmt,
    T_xk_pg_parser_CommentStmt,
    T_xk_pg_parser_FetchStmt,
    T_xk_pg_parser_IndexStmt,
    T_xk_pg_parser_CreateFunctionStmt,
    T_xk_pg_parser_AlterFunctionStmt,
    T_xk_pg_parser_DoStmt,
    T_xk_pg_parser_RenameStmt,
    T_xk_pg_parser_RuleStmt,
    T_xk_pg_parser_NotifyStmt,
    T_xk_pg_parser_ListenStmt,
    T_xk_pg_parser_UnlistenStmt,
    T_xk_pg_parser_TransactionStmt,
    T_xk_pg_parser_ViewStmt,
    T_xk_pg_parser_LoadStmt,
    T_xk_pg_parser_CreateDomainStmt,
    T_xk_pg_parser_CreatedbStmt,
    T_xk_pg_parser_DropdbStmt,
    T_xk_pg_parser_VacuumStmt,
    T_xk_pg_parser_ExplainStmt,
    T_xk_pg_parser_CreateTableAsStmt,
    T_xk_pg_parser_CreateSeqStmt,
    T_xk_pg_parser_AlterSeqStmt,
    T_xk_pg_parser_VariableSetStmt,
    T_xk_pg_parser_VariableShowStmt,
    T_xk_pg_parser_DiscardStmt,
    T_xk_pg_parser_CreateTrigStmt,
    T_xk_pg_parser_CreatePLangStmt,
    T_xk_pg_parser_CreateRoleStmt,
    T_xk_pg_parser_AlterRoleStmt,
    T_xk_pg_parser_DropRoleStmt,
    T_xk_pg_parser_LockStmt,
    T_xk_pg_parser_ConstraintsSetStmt,
    T_xk_pg_parser_ReindexStmt,
    T_xk_pg_parser_CheckPointStmt,
    T_xk_pg_parser_CreateSchemaStmt,
    T_xk_pg_parser_AlterDatabaseStmt,
    T_xk_pg_parser_AlterDatabaseSetStmt,
    T_xk_pg_parser_AlterRoleSetStmt,
    T_xk_pg_parser_CreateConversionStmt,
    T_xk_pg_parser_CreateCastStmt,
    T_xk_pg_parser_CreateOpClassStmt,
    T_xk_pg_parser_CreateOpFamilyStmt,
    T_xk_pg_parser_AlterOpFamilyStmt,
    T_xk_pg_parser_PrepareStmt,
    T_xk_pg_parser_ExecuteStmt,
    T_xk_pg_parser_DeallocateStmt,
    T_xk_pg_parser_DeclareCursorStmt,
    T_xk_pg_parser_CreateTableSpaceStmt,
    T_xk_pg_parser_DropTableSpaceStmt,
    T_xk_pg_parser_AlterObjectDependsStmt,
    T_xk_pg_parser_AlterObjectSchemaStmt,
    T_xk_pg_parser_AlterOwnerStmt,
    T_xk_pg_parser_AlterOperatorStmt,
    T_xk_pg_parser_DropOwnedStmt,
    T_xk_pg_parser_ReassignOwnedStmt,
    T_xk_pg_parser_CompositeTypeStmt,
    T_xk_pg_parser_CreateEnumStmt,
    T_xk_pg_parser_CreateRangeStmt,
    T_xk_pg_parser_AlterEnumStmt,
    T_xk_pg_parser_AlterTSDictionaryStmt,
    T_xk_pg_parser_AlterTSConfigurationStmt,
    T_xk_pg_parser_CreateFdwStmt,
    T_xk_pg_parser_AlterFdwStmt,
    T_xk_pg_parser_CreateForeignServerStmt,
    T_xk_pg_parser_AlterForeignServerStmt,
    T_xk_pg_parser_CreateUserMappingStmt,
    T_xk_pg_parser_AlterUserMappingStmt,
    T_xk_pg_parser_DropUserMappingStmt,
    T_xk_pg_parser_AlterTableSpaceOptionsStmt,
    T_xk_pg_parser_AlterTableMoveAllStmt,
    T_xk_pg_parser_SecLabelStmt,
    T_xk_pg_parser_CreateForeignTableStmt,
    T_xk_pg_parser_ImportForeignSchemaStmt,
    T_xk_pg_parser_CreateExtensionStmt,
    T_xk_pg_parser_AlterExtensionStmt,
    T_xk_pg_parser_AlterExtensionContentsStmt,
    T_xk_pg_parser_CreateEventTrigStmt,
    T_xk_pg_parser_AlterEventTrigStmt,
    T_xk_pg_parser_RefreshMatViewStmt,
    T_xk_pg_parser_ReplicaIdentityStmt,
    T_xk_pg_parser_AlterSystemStmt,
    T_xk_pg_parser_CreatePolicyStmt,
    T_xk_pg_parser_AlterPolicyStmt,
    T_xk_pg_parser_CreateTransformStmt,
    T_xk_pg_parser_CreateAmStmt,
    T_xk_pg_parser_CreatePublicationStmt,
    T_xk_pg_parser_AlterPublicationStmt,
    T_xk_pg_parser_CreateSubscriptionStmt,
    T_xk_pg_parser_AlterSubscriptionStmt,
    T_xk_pg_parser_DropSubscriptionStmt,
    T_xk_pg_parser_CreateStatsStmt,
    T_xk_pg_parser_AlterCollationStmt,
    T_xk_pg_parser_CallStmt,

    /*
     * TAGS FOR PARSE TREE NODES (parsenodes.h)
     */
    T_xk_pg_parser_A_Expr,
    T_xk_pg_parser_ColumnRef,
    T_xk_pg_parser_ParamRef,
    T_xk_pg_parser_A_Const,
    T_xk_pg_parser_FuncCall,
    T_xk_pg_parser_A_Star,
    T_xk_pg_parser_A_Indices,
    T_xk_pg_parser_A_Indirection,
    T_xk_pg_parser_A_ArrayExpr,
    T_xk_pg_parser_ResTarget,
    T_xk_pg_parser_MultiAssignRef,
    T_xk_pg_parser_TypeCast,
    T_xk_pg_parser_CollateClause,
    T_xk_pg_parser_SortBy,
    T_xk_pg_parser_WindowDef,
    T_xk_pg_parser_RangeSubselect,
    T_xk_pg_parser_RangeFunction,
    T_xk_pg_parser_RangeTableSample,
    T_xk_pg_parser_RangeTableFunc,
    T_xk_pg_parser_RangeTableFuncCol,
    T_xk_pg_parser_TypeName,
    T_xk_pg_parser_ColumnDef,
    T_xk_pg_parser_IndexElem,
    T_xk_pg_parser_Constraint,
    T_xk_pg_parser_DefElem,
    T_xk_pg_parser_RangeTblEntry,
    T_xk_pg_parser_RangeTblFunction,
    T_xk_pg_parser_TableSampleClause,
    T_xk_pg_parser_WithCheckOption,
    T_xk_pg_parser_SortGroupClause,
    T_xk_pg_parser_GroupingSet,
    T_xk_pg_parser_WindowClause,
    T_xk_pg_parser_ObjectWithArgs,
    T_xk_pg_parser_AccessPriv,
    T_xk_pg_parser_CreateOpClassItem,
    T_xk_pg_parser_TableLikeClause,
    T_xk_pg_parser_FunctionParameter,
    T_xk_pg_parser_LockingClause,
    T_xk_pg_parser_RowMarkClause,
    T_xk_pg_parser_XmlSerialize,
    T_xk_pg_parser_WithClause,
    T_xk_pg_parser_InferClause,
    T_xk_pg_parser_OnConflictClause,
    T_xk_pg_parser_CommonTableExpr,
    T_xk_pg_parser_RoleSpec,
    T_xk_pg_parser_TriggerTransition,
    T_xk_pg_parser_PartitionElem,
    T_xk_pg_parser_PartitionSpec,
    T_xk_pg_parser_PartitionBoundSpec,
    T_xk_pg_parser_PartitionRangeDatum,
    T_xk_pg_parser_PartitionCmd,
    T_xk_pg_parser_VacuumRelation,

    /*
     * TAGS FOR REPLICATION GRAMMAR PARSE NODES (replnodes.h)
     */
    T_xk_pg_parser_IdentifySystemCmd,
    T_xk_pg_parser_BaseBackupCmd,
    T_xk_pg_parser_CreateReplicationSlotCmd,
    T_xk_pg_parser_DropReplicationSlotCmd,
    T_xk_pg_parser_StartReplicationCmd,
    T_xk_pg_parser_TimeLineHistoryCmd,
    T_xk_pg_parser_SQLCmd,

    /*
     * TAGS FOR RANDOM OTHER STUFF
     *
     * These are objects that aren't part of parse/plan/execute node tree
     * structures, but we give them xk_pg_parser_NodeTags anyway for identification
     * purposes (usually because they are involved in APIs where we want to
     * pass multiple object types through the same pointer).
     */
    T_xk_pg_parser_TriggerData,                /* in commands/trigger.h */
    T_xk_pg_parser_EventTriggerData,            /* in commands/evenT_xk_pg_parser_trigger.h */
    T_xk_pg_parser_ReturnSetInfo,            /* in nodes/execnodes.h */
    T_xk_pg_parser_WindowObjectData,            /* private in nodeWindowAgg.c */
    T_xk_pg_parser_TIDBitmap,                /* in nodes/tidbitmap.h */
    T_xk_pg_parser_InlineCodeBlock,            /* in nodes/parsenodes.h */
    T_xk_pg_parser_FdwRoutine,                /* in foreign/fdwapi.h */
    T_xk_pg_parser_IndexAmRoutine,            /* in access/amapi.h */
    T_xk_pg_parser_TableAmRoutine,            /* in access/tableam.h */
    T_xk_pg_parser_TsmRoutine,                /* in access/tsmapi.h */
    T_xk_pg_parser_ForeignKeyCacheInfo,        /* in utils/rel.h */
    T_xk_pg_parser_CallContext,                /* in nodes/parsenodes.h */
    T_xk_pg_parser_SupportRequestSimplify,    /* in nodes/supportnodes.h */
    T_xk_pg_parser_SupportRequestSelectivity,    /* in nodes/supportnodes.h */
    T_xk_pg_parser_SupportRequestCost,        /* in nodes/supportnodes.h */
    T_xk_pg_parser_SupportRequestRows,        /* in nodes/supportnodes.h */
    T_xk_pg_parser_SupportRequestIndexCondition    /* in nodes/supportnodes.h */
} xk_pg_parser_NodeTag;


#define XK_PG_PARSER_PARTITION_STRATEGY_HASH    'h'
#define XK_PG_PARSER_PARTITION_STRATEGY_LIST    'l'
#define XK_PG_PARSER_PARTITION_STRATEGY_RANGE   'r'
/*
 * The first field of a node of any type is guaranteed to be the xk_pg_parser_NodeTag.
 * Hence the type of any node can be gotten by casting it to xk_pg_parser_Node. Declaring
 * a variable to be of xk_pg_parser_Node * (instead of void *) can also facilitate
 * debugging.
 */
typedef struct xk_pg_parser_Node
{
    xk_pg_parser_NodeTag type;
} xk_pg_parser_Node;

#define xk_pg_parser_NodeTagType(nodeptr) (((const xk_pg_parser_Node*)(nodeptr))->type)

#define XK_NODE_MCXT NULL

#define xk_pg_parser_castNode(_type_, nodeptr) ((_type_ *) (nodeptr))

#define xk_pg_parser_newNode(size, tag) \
({  xk_pg_parser_Node   *_result; \
    xk_pg_parser_AssertMacro((size) >= sizeof(xk_pg_parser_Node));        /* need the tag, at least */ \
    xk_pg_parser_mcxt_malloc(XK_NODE_MCXT, (void **)&_result, size);\
    _result->type = (tag); \
    _result; \
})

#define xk_pg_parser_makeNode(_type_) ((_type_ *) xk_pg_parser_newNode(sizeof(_type_), T_##_type_))
#endif
