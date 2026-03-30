#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"
#include "common/pg_parser_translog.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode_struct.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode_util.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode_value.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode_type.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode_local_func.h"

static char* get_local_typoutput_by_oid(uint32_t oid);
static char* get_local_funcname_by_oid(uint32_t oid, uint16_t* argnum);

static pg_parser_node_var* pg_parser_make_nodetree_var(pg_parser_Var* var);
static pg_parser_node_const* pg_parser_make_nodetree_const(pg_parser_Const* const, pg_parser_deparse_context* context);
static pg_parser_node_func* pg_parser_make_nodetree_func(pg_parser_FuncExpr* func, pg_parser_deparse_context* context);

static bool get_variable(pg_parser_Var* var, pg_parser_deparse_context* context);
static bool get_const_expr(pg_parser_Const* constval, pg_parser_deparse_context* context);
static bool get_func_expr(pg_parser_FuncExpr* funcval, pg_parser_deparse_context* context, bool showimplicit);

static bool get_rule_expr_paren(pg_parser_Node*            node,
                                pg_parser_deparse_context* context,
                                bool                       showimplicit,
                                pg_parser_Node*            parentNode);

static char* get_local_opname_by_oid(uint32_t oid)
{
    switch (oid)
    {
        case 76:
        case 59:
        case 258:
        case 264:
        case 2800:
        case 413:
        case 419:
        case 502:
        case 520:
        case 521:
        case 536:
        case 537:
        case 610:
        case 646:
        case 623:
        case 633:
        case 662:
        case 666:
        case 674:
        case 794:
        case 903:
        case 1060:
        case 1073:
        case 1097:
        case 1112:
        case 1554:
        case 1123:
        case 1133:
        case 1324:
        case 1334:
        case 1503:
        case 1589:
        case 1224:
        case 3366:
        case 1205:
        case 1756:
        case 1787:
        case 1807:
        case 1865:
        case 1871:
        case 1959:
        case 2064:
        case 2349:
        case 2362:
        case 2375:
        case 2388:
        case 2538:
        case 2544:
        case 2975:
        case 3225:
        case 3519:
        case 3632:
        case 3679:
        case 2991:
        case 3887:
        case 3243:
            return pg_parser_mcxt_strdup(">");

        case 37:
        case 58:
        case 95:
        case 97:
        case 255:
        case 261:
        case 2799:
        case 412:
        case 418:
        case 504:
        case 534:
        case 535:
        case 609:
        case 645:
        case 622:
        case 631:
        case 660:
        case 664:
        case 672:
        case 793:
        case 902:
        case 1058:
        case 1072:
        case 1095:
        case 1110:
        case 1552:
        case 1122:
        case 1132:
        case 1322:
        case 1332:
        case 1502:
        case 1587:
        case 1222:
        case 3364:
        case 1203:
        case 1754:
        case 1786:
        case 1806:
        case 1864:
        case 1870:
        case 1957:
        case 2062:
        case 2345:
        case 2358:
        case 2371:
        case 2384:
        case 2534:
        case 2540:
        case 2974:
        case 3224:
        case 3518:
        case 3627:
        case 3674:
        case 2990:
        case 3884:
        case 3242:
            return pg_parser_mcxt_strdup("<");

        case 15:
        case 91:
        case 92:
        case 93:
        case 94:
        case 96:
        case 98:
        case 254:
        case 260:
        case 352:
        case 353:
        case 385:
        case 387:
        case 410:
        case 416:
        case 503:
        case 532:
        case 533:
        case 607:
        case 649:
        case 620:
        case 670:
        case 792:
        case 900:
        case 974:
        case 1054:
        case 1070:
        case 1093:
        case 1108:
        case 1550:
        case 1120:
        case 1130:
        case 1320:
        case 1330:
        case 1500:
        case 1535:
        case 1616:
        case 1220:
        case 3362:
        case 1201:
        case 1752:
        case 1784:
        case 1804:
        case 1862:
        case 1868:
        case 1955:
        case 2060:
        case 2347:
        case 2360:
        case 2373:
        case 2386:
        case 2536:
        case 2542:
        case 2972:
        case 3222:
        case 3516:
        case 3629:
        case 3676:
        case 2988:
        case 3882:
        case 3240:
            return pg_parser_mcxt_strdup("=");

        case 36:
        case 85:
        case 259:
        case 265:
        case 3315:
        case 3316:
        case 402:
        case 411:
        case 417:
        case 518:
        case 519:
        case 531:
        case 538:
        case 539:
        case 608:
        case 644:
        case 621:
        case 630:
        case 643:
        case 671:
        case 713:
        case 901:
        case 1057:
        case 1071:
        case 1094:
        case 1109:
        case 1551:
        case 1121:
        case 1131:
        case 1321:
        case 1331:
        case 1501:
        case 1586:
        case 1221:
        case 3363:
        case 1202:
        case 1753:
        case 1785:
        case 1805:
        case 1863:
        case 1869:
        case 1956:
        case 2061:
        case 2350:
        case 2363:
        case 2376:
        case 2389:
        case 2539:
        case 2545:
        case 2973:
        case 3223:
        case 3517:
        case 3630:
        case 3677:
        case 2989:
        case 3883:
        case 3241:
            return pg_parser_mcxt_strdup("<>");

        case 550:
        case 551:
        case 552:
        case 553:
        case 586:
        case 591:
        case 684:
        case 688:
        case 692:
        case 818:
        case 822:
        case 731:
        case 735:
        case 736:
        case 804:
        case 906:
        case 966:
        case 1076:
        case 1100:
        case 1116:
        case 1126:
        case 1327:
        case 1337:
        case 1360:
        case 1361:
        case 1363:
        case 1366:
        case 1516:
        case 2637:
        case 2638:
        case 1758:
        case 1800:
        case 1802:
        case 1849:
        case 1916:
        case 1917:
        case 1918:
        case 1919:
        case 1920:
        case 1921:
        case 2066:
        case 2551:
        case 2552:
        case 2553:
        case 2554:
        case 2555:
        case 3898:
            return pg_parser_mcxt_strdup("+");

        case 484:
        case 554:
        case 555:
        case 556:
        case 557:
        case 558:
        case 559:
        case 584:
        case 585:
        case 587:
        case 592:
        case 685:
        case 689:
        case 693:
        case 819:
        case 823:
        case 732:
        case 737:
        case 805:
        case 907:
        case 967:
        case 1077:
        case 1099:
        case 1101:
        case 1117:
        case 1127:
        case 1328:
        case 1329:
        case 1336:
        case 1338:
        case 1399:
        case 1517:
        case 2639:
        case 2640:
        case 1751:
        case 1759:
        case 1801:
        case 1803:
        case 2067:
        case 2068:
        case 3228:
        case 3899:
        case 3285:
        case 3398:
        case 3286:
            return pg_parser_mcxt_strdup("-");

        case 514:
        case 526:
        case 544:
        case 545:
        case 589:
        case 594:
        case 686:
        case 690:
        case 694:
        case 820:
        case 824:
        case 733:
        case 738:
        case 806:
        case 843:
        case 845:
        case 908:
        case 3346:
        case 912:
        case 914:
        case 916:
        case 3349:
        case 917:
        case 918:
        case 1119:
        case 1129:
        case 1518:
        case 1583:
        case 1584:
        case 1760:
        case 3900:
            return pg_parser_mcxt_strdup("*");

        case 527:
        case 528:
        case 546:
        case 547:
        case 588:
        case 593:
        case 687:
        case 691:
        case 695:
        case 821:
        case 825:
        case 734:
        case 739:
        case 807:
        case 844:
        case 909:
        case 3347:
        case 913:
        case 915:
        case 3825:
        case 1118:
        case 1128:
        case 1519:
        case 1585:
        case 1761:
            return pg_parser_mcxt_strdup("/");

        default:
            return NULL;
    }
}

static char* get_local_typoutput_by_oid(uint32_t oid)
{
    char* result = NULL;
    switch (oid)
    {
        case BOOLOID:
            result = "boolout";
            break;
        case BYTEAOID:
            result = "byteaout";
            break;
        case CHAROID:
            result = "charout";
            break;
        case NAMEOID:
            result = "nameout";
            break;
        case INT8OID:
            result = "int8out";
            break;
        case INT2OID:
            result = "int2out";
            break;
        case INT2VECTOROID:
            result = "int2vectorout";
            break;
        case INT4OID:
            result = "int4out";
            break;
        case TEXTOID:
            result = "textout";
            break;
        case OIDOID:
            result = "oidout";
            break;
        case TIDOID:
            result = "tidout";
            break;
        case XIDOID:
            result = "xidout";
            break;
        case CIDOID:
            result = "cidout";
            break;
        case OIDVECTOROID:
            result = "oidvectorout";
            break;
        case JSONOID:
            result = "json_out";
            break;
        case XMLOID:
            result = "xml_out";
            break;
        case PGNODETREEOID:
            result = "pg_node_tree_out";
            break;
        case POINTOID:
            result = "point_out";
            break;
        case LSEGOID:
            result = "lseg_out";
            break;
        case PATHOID:
            result = "path_out";
            break;
        case BOXOID:
            result = "box_out";
            break;
        case POLYGONOID:
            result = "poly_out";
            break;
        case LINEOID:
            result = "line_out";
            break;
        case FLOAT4OID:
            result = "float4out";
            break;
        case FLOAT8OID:
            result = "float8out";
            break;
        case CIRCLEOID:
            result = "circle_out";
            break;
        case CASHOID:
            result = "cash_out";
            break;
        case MACADDROID:
            result = "macaddr_out";
            break;
        case INETOID:
            result = "inet_out";
            break;
        case CIDROID:
            result = "cidr_out";
            break;
        case MACADDR8OID:
            result = "macaddr8_out";
            break;
        case BPCHAROID:
            result = "bpcharout";
            break;
        case VARCHAROID:
            result = "varcharout";
            break;
        case DATEOID:
            result = "date_out";
            break;
        case TIMEOID:
            result = "time_out";
            break;
        case TIMESTAMPOID:
            result = "timestamp_out";
            break;
        case TIMESTAMPTZOID:
            result = "timestamptz_out";
            break;
        case INTERVALOID:
            result = "interval_out";
            break;
        case TIMETZOID:
            result = "timetz_out";
            break;
        case BITOID:
            result = "bit_out";
            break;
        case VARBITOID:
            result = "varbit_out";
            break;
        case NUMERICOID:
            result = "numeric_out";
            break;
        case UUIDOID:
            result = "uuid_out";
            break;
        case LSNOID:
            result = "pg_lsn_out";
            break;
        case TSVECTOROID:
            result = "tsvectorout";
            break;
        case TSQUERYOID:
            result = "tsqueryout";
            break;
        case JSONBOID:
            result = "jsonb_out";
            break;
        case INT4RANGEOID:
            result = "range_out";
            break;
        case NUMRANGEOID:
            result = "range_out";
            break;
        case TSRANGEOID:
            result = "range_out";
            break;
        case TSTZRANGEOID:
            result = "range_out";
            break;
        case DATERANGEOID:
            result = "range_out";
            break;
        case INT8RANGEOID:
            result = "range_out";
            break;
        case CSTRINGOID:
            result = "cstring_out";
            break;
        case BOOLARRAYOID:
            result = "array_out";
            break;
        case BYTEAARRAYOID:
            result = "array_out";
            break;
        case CHARARRAYOID:
            result = "array_out";
            break;
        case NAMEARRAYOID:
            result = "array_out";
            break;
        case INT8ARRAYOID:
            result = "array_out";
            break;
        case INT2ARRAYOID:
            result = "array_out";
            break;
        case INT4ARRAYOID:
            result = "array_out";
            break;
        case TEXTARRAYOID:
            result = "array_out";
            break;
        case OIDARRAYOID:
            result = "array_out";
            break;
        case TIDARRAYOID:
            result = "array_out";
            break;
        case XIDARRAYOID:
            result = "array_out";
            break;
        case CIDARRAYOID:
            result = "array_out";
            break;
        case JSONARRAYOID:
            result = "array_out";
            break;
        case XMLARRAYOID:
            result = "array_out";
            break;
        case POINTARRAYOID:
            result = "array_out";
            break;
        case LSEGARRAYOID:
            result = "array_out";
            break;
        case PATHARRAYOID:
            result = "array_out";
            break;
        case BOXARRAYOID:
            result = "array_out";
            break;
        case POLYGONARRAYOID:
            result = "array_out";
            break;
        case LINEARRAYOID:
            result = "array_out";
            break;
        case FLOAT4ARRAYOID:
            result = "array_out";
            break;
        case FLOAT8ARRAYOID:
            result = "array_out";
            break;
        case CIRCLEARRAYOID:
            result = "array_out";
            break;
        case MONEYARRAYOID:
            result = "array_out";
            break;
        case MACADDRARRAYOID:
            result = "array_out";
            break;
        case INETARRAYOID:
            result = "array_out";
            break;
        case CIDRARRAYOID:
            result = "array_out";
            break;
        case MACADDR8ARRAYOID:
            result = "array_out";
            break;
        case BPCHARARRAYOID:
            result = "array_out";
            break;
        case VARCHARARRAYOID:
            result = "array_out";
            break;
        case DATEARRAYOID:
            result = "array_out";
            break;
        case TIMEARRAYOID:
            result = "array_out";
            break;
        case TIMESTAMPARRAYOID:
            result = "array_out";
            break;
        case TIMESTAMPTZARRAYOID:
            result = "array_out";
            break;
        case TIMETZARRAYOID:
            result = "array_out";
            break;
        case BITARRAYOID:
            result = "array_out";
            break;
        case VARBITARRAYOID:
            result = "array_out";
            break;
        case NUMERICARRAYOID:
            result = "array_out";
            break;
        case UUIDARRAYOID:
            result = "array_out";
            break;
        case PG_LSNARRAYOID:
            result = "array_out";
            break;
        case JSONBARRAYOID:
            result = "array_out";
            break;
        case CSTRINGARRAYOID:
            result = "array_out";
            break;
        case REGCLASSOID:
            result = "regclassout";
            break;
        default:
            break;
    }
    return pg_parser_mcxt_strdup(result);
}

static char* get_local_funcname_by_oid(uint32_t oid, uint16_t* argnum)
{
    int32_t i = 0;
    for (i = 0; i < local_func_num; i++)
    {
        if (oid == pg_parser_local_func_builtins[i].m_oid)
        {
            *argnum = pg_parser_local_func_builtins[i].m_func_argnum;
            return pg_parser_mcxt_strdup(pg_parser_local_func_builtins[i].m_func_name);
        }
    }
    return NULL;
}

static pg_parser_node_var* pg_parser_make_nodetree_var(pg_parser_Var* var)
{
    pg_parser_node_var* varnode = NULL;
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&varnode, sizeof(pg_parser_node_var));
    varnode->m_attno = var->varattno;
    return varnode;
}

static pg_parser_node_const* pg_parser_make_nodetree_const(pg_parser_Const*           const_value,
                                                           pg_parser_deparse_context* context)
{
    pg_parser_node_const* constnode = NULL;
    char*                 typoutput = NULL;

    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&constnode, sizeof(pg_parser_node_const));
    constnode->m_typid = const_value->consttype;
    /* Return NULL directly for null values */
    if (const_value->constisnull)
    {
        constnode->m_char_value = pg_parser_mcxt_strdup("NULL");
        return constnode;
    }
    /* Parse non-null values, first get typout function */
    typoutput = get_local_typoutput_by_oid(const_value->consttype);

    /* When local typout function cannot be obtained, return error */
    if (!typoutput)
    {
        return NULL;
    }

    constnode->m_char_value = pg_parser_convert_attr_to_str_by_typid_typoptput(const_value->constvalue,
                                                                               const_value->consttype,
                                                                               typoutput,
                                                                               context->zicinfo);
    pg_parser_mcxt_free(NODE_MCXT, typoutput);
    if (!constnode->m_char_value)
    {
        return NULL;
    }
    /* Temporarily not doing other parsing operations, directly return here */
    return constnode;
}

static pg_parser_node_op* pg_parser_make_nodetree_op(pg_parser_OpExpr* op_value, pg_parser_deparse_context* context)
{
    pg_parser_node_op* opnode = NULL;

    PG_PARSER_UNUSED(context);
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&opnode, sizeof(pg_parser_node_op));
    opnode->m_opid = op_value->opno;
    /* Get operator name */
    opnode->m_opname = get_local_opname_by_oid(opnode->m_opid);
    /* Temporarily not doing other parsing operations, directly return here */
    return opnode;
}

static pg_parser_nodetree* pg_parser_append_nodetree_with_type(pg_parser_nodetree* nodetree,
                                                               void*               nodetree_node,
                                                               uint8_t             nodetree_type)
{
    pg_parser_nodetree* head_ptr = nodetree;
    pg_parser_nodetree* result_ptr = NULL;
    if (NULL == nodetree)
    {
        pg_parser_mcxt_malloc(NODE_MCXT, (void**)&result_ptr, sizeof(pg_parser_nodetree));
        if (PG_PARSER_NODETYPE_CHAR == nodetree_type)
        {
            result_ptr->m_node = pg_parser_mcxt_strdup((char*)nodetree_node);
        }
        else
        {
            result_ptr->m_node = nodetree_node;
        }
        result_ptr->m_node_type = nodetree_type;
        result_ptr->m_next = NULL;
        head_ptr = result_ptr;
    }
    else
    {
        pg_parser_nodetree* temp_ptr = nodetree;
        while (temp_ptr->m_next)
        {
            temp_ptr = temp_ptr->m_next;
        }
        pg_parser_mcxt_malloc(NODE_MCXT, (void**)&result_ptr, sizeof(pg_parser_nodetree));
        if (PG_PARSER_NODETYPE_CHAR == nodetree_type)
        {
            result_ptr->m_node = pg_parser_mcxt_strdup((char*)nodetree_node);
        }
        else
        {
            result_ptr->m_node = nodetree_node;
        }
        result_ptr->m_node_type = nodetree_type;
        result_ptr->m_next = NULL;
        temp_ptr->m_next = result_ptr;
    }
    return head_ptr;
}

/*
 * The ultimate goal here is to get column name, but we cannot get it by querying table
 * Therefore here we simply get column sort order,
 * Subsequent assembly processing is completed in DDL parsing, finally parsed by frontend
 */
static bool get_variable(pg_parser_Var* var, pg_parser_deparse_context* context)
{
    pg_parser_node_var* node_var = pg_parser_make_nodetree_var(var);
    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree, (void*)node_var, PG_PARSER_NODETYPE_VAR);
    if (var)
    {
        pg_parser_mcxt_free(NODE_MCXT, var);
    }
    return true;
}

static pg_parser_node_func* pg_parser_make_nodetree_func(pg_parser_FuncExpr* func, pg_parser_deparse_context* context)
{
    pg_parser_node_func* funcnode = NULL;
    PG_PARSER_UNUSED(context);
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&funcnode, sizeof(pg_parser_node_func));
    funcnode->m_funcname = get_local_funcname_by_oid(func->funcid, &(funcnode->m_argnum));
    funcnode->m_funcid = func->funcid;
    return funcnode;
}

/* We need to call type output to convert value stored in const, conversion may fail */
static bool get_const_expr(pg_parser_Const* constval, pg_parser_deparse_context* context)
{
    pg_parser_node_const* node_cons = pg_parser_make_nodetree_const(constval, context);
    if (!node_cons)
    {
        return false;
    }
    context->nodetree =
        pg_parser_append_nodetree_with_type(context->nodetree, (void*)node_cons, PG_PARSER_NODETYPE_CONST);
    if (constval)
    {
        if (constval->constneedfree)
        {
            pg_parser_mcxt_free(NODE_MCXT, pg_parser_DatumGetPointer(constval->constvalue));
        }
    }
    pg_parser_mcxt_free(NODE_MCXT, constval);
    return true;
}

#define PRETTYFLAG_PAREN       0x0001
#define PRETTYFLAG_INDENT      0x0002
#define PRETTYFLAG_SCHEMA      0x0004

#define PRETTY_PAREN(context)  ((context)->prettyFlags & PRETTYFLAG_PAREN)
#define PRETTY_INDENT(context) ((context)->prettyFlags & PRETTYFLAG_INDENT)
#define PRETTY_SCHEMA(context) ((context)->prettyFlags & PRETTYFLAG_SCHEMA)

#define IsA(nodeptr, _type_)   (pg_parser_NodeTagType(nodeptr) == T_##_type_)

static const char* get_simple_binary_op_name(pg_parser_OpExpr* expr)
{
    pg_parser_List* args = expr->args;

    if (pg_parser_list_length(args) == 2)
    {
        /* binary operator */
        const char* op;

        op = get_local_opname_by_oid(expr->opno);
        if (strlen(op) == 1)
        {
            return op;
        }
    }
    return NULL;
}

static bool isSimpleNode(pg_parser_Node* node, pg_parser_Node* parentNode, int32_t prettyFlags)
{
    if (!node)
    {
        return false;
    }

    switch (pg_parser_NodeTagType(node))
    {
        case T_pg_parser_Var:
        case T_pg_parser_Const:
            /* single words: always simple */
            return true;

        case T_pg_parser_FuncExpr:
            /* function-like: name(..) or name[..] */
            return true;

        case T_pg_parser_OpExpr:
        {
            /* depends on parent node type; needs further checking */
            if (prettyFlags & PRETTYFLAG_PAREN && IsA(parentNode, pg_parser_OpExpr))
            {
                const char* op;
                const char* parentOp;
                bool        is_lopriop;
                bool        is_hipriop;
                bool        is_lopriparent;
                bool        is_hipriparent;

                op = get_simple_binary_op_name((pg_parser_OpExpr*)node);
                if (!op)
                {
                    return false;
                }

                /* We know only the basic operators + - and * / % */
                is_lopriop = (strchr("+-", *op) != NULL);
                is_hipriop = (strchr("*/%", *op) != NULL);
                if (!(is_lopriop || is_hipriop))
                {
                    return false;
                }

                parentOp = get_simple_binary_op_name((pg_parser_OpExpr*)parentNode);
                if (!parentOp)
                {
                    return false;
                }

                is_lopriparent = (strchr("+-", *parentOp) != NULL);
                is_hipriparent = (strchr("*/%", *parentOp) != NULL);
                if (!(is_lopriparent || is_hipriparent))
                {
                    return false;
                }

                if (is_hipriop && is_lopriparent)
                {
                    return true; /* op binds tighter than parent */
                }

                if (is_lopriop && is_hipriparent)
                {
                    return false;
                }

                /*
                 * Operators are same priority --- can skip parens only if
                 * we have (a - b) - c, not a - (b - c).
                 */
                if (node == (pg_parser_Node*)pg_parser_linitial(((pg_parser_OpExpr*)parentNode)->args))
                {
                    return true;
                }

                return false;
            }
            /* else do the same stuff as for T_SubLink et al. */
        }
            /* FALLTHROUGH */

        case T_pg_parser_BoolExpr:
            switch (pg_parser_NodeTagType(parentNode))
            {
                case T_pg_parser_BoolExpr:
                    if (prettyFlags & PRETTYFLAG_PAREN)
                    {
                        pg_parser_BoolExprType type;
                        pg_parser_BoolExprType parentType;

                        type = ((pg_parser_BoolExpr*)node)->boolop;
                        parentType = ((pg_parser_BoolExpr*)parentNode)->boolop;
                        switch (type)
                        {
                            case NOT_EXPR:
                            case AND_EXPR:
                                if (parentType == AND_EXPR || parentType == OR_EXPR)
                                {
                                    return true;
                                }
                                break;
                            case OR_EXPR:
                                if (parentType == OR_EXPR)
                                {
                                    return true;
                                }
                                break;
                        }
                    }
                    return false;
                case T_pg_parser_FuncExpr:
                {
                    /* special handling for casts */
                    pg_parser_CoercionForm type = ((pg_parser_FuncExpr*)parentNode)->funcformat;

                    if (type == COERCE_EXPLICIT_CAST || type == COERCE_IMPLICIT_CAST)
                    {
                        return false;
                    }
                    return true; /* own parentheses */
                }
                default:
                    return false;
            }

        default:
            break;
    }
    /* those we don't know: in dubio complexo */
    return false;
}

static bool get_rule_expr_paren(pg_parser_Node*            node,
                                pg_parser_deparse_context* context,
                                bool                       showimplicit,
                                pg_parser_Node*            parentNode)
{
    bool need_paren;

    need_paren = PRETTY_PAREN(context) && !isSimpleNode(node, parentNode, context->prettyFlags);

    if (need_paren)
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)("("), PG_PARSER_NODETYPE_CHAR);
    }

    if (!get_rule_expr(node, context, showimplicit))
    {
        return false;
    }

    if (need_paren)
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")"), PG_PARSER_NODETYPE_CHAR);
    }
    return true;
}

static bool exprIsLengthCoercion(const pg_parser_Node* expr, int32_t* coercedTypmod)
{
    if (coercedTypmod != NULL)
    {
        *coercedTypmod = -1; /* default result on failure */
    }

    /*
     * Scalar-type length coercions are FuncExprs, array-type length coercions
     * are ArrayCoerceExprs
     */
    if (expr && IsA(expr, pg_parser_FuncExpr))
    {
        const pg_parser_FuncExpr* func = (const pg_parser_FuncExpr*)expr;
        int                       nargs;
        pg_parser_Const*          second_arg;

        /*
         * If it didn't come from a coercion context, reject.
         */
        if (func->funcformat != COERCE_EXPLICIT_CAST && func->funcformat != COERCE_IMPLICIT_CAST)
        {
            return false;
        }

        /*
         * If it's not a two-argument or three-argument function with the
         * second argument being an int4 constant, it can't have been created
         * from a length coercion (it must be a type coercion, instead).
         */
        nargs = pg_parser_list_length(func->args);
        if (nargs < 2 || nargs > 3)
        {
            return false;
        }

        second_arg = (pg_parser_Const*)pg_parser_lsecond(func->args);
        if (!IsA(second_arg, pg_parser_Const) || second_arg->consttype != INT4OID || second_arg->constisnull)
        {
            return false;
        }

        /*
         * OK, it is indeed a length-coercion function.
         */
        if (coercedTypmod != NULL)
        {
            *coercedTypmod = pg_parser_DatumGetInt32(second_arg->constvalue);
        }

        return true;
    }

    if (expr && IsA(expr, pg_parser_ArrayCoerceExpr))
    {
        const pg_parser_ArrayCoerceExpr* acoerce = (const pg_parser_ArrayCoerceExpr*)expr;

        /* It's not a length coercion unless there's a nondefault typmod */
        if (acoerce->resulttypmod < 0)
        {
            return false;
        }

        /*
         * OK, it is indeed a length-coercion expression.
         */
        if (coercedTypmod != NULL)
        {
            *coercedTypmod = acoerce->resulttypmod;
        }

        return true;
    }

    return false;
}

static bool get_coercion_expr(pg_parser_Node*            arg,
                              pg_parser_deparse_context* context,
                              uint32_t                   resulttype,
                              int32_t                    resulttypmod,
                              pg_parser_Node*            parentNode)
{
    PG_PARSER_UNUSED(resulttypmod);
    /*
     * Since parse_coerce.c doesn't immediately collapse application of
     * length-coercion functions to constants, what we'll typically see in
     * such cases is a Const with typmod -1 and a length-coercion function
     * right above it.  Avoid generating redundant output. However, beware of
     * suppressing casts when the user actually wrote something like
     * 'foo'::text::char(3).
     *
     * Note: it might seem that we are missing the possibility of needing to
     * print a COLLATE clause for such a Const.  However, a Const could only
     * have nondefault collation in a post-constant-folding tree, in which the
     * length coercion would have been folded too.  See also the special
     * handling of CollateExpr in coerce_to_target_type(): any collation
     * marking will be above the coercion node, not below it.
     */
    if (!PRETTY_PAREN(context))
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)("("), PG_PARSER_NODETYPE_CHAR);
    }
    if (!get_rule_expr_paren(arg, context, false, parentNode))
    {
        return false;
    }

    if (!PRETTY_PAREN(context))
    {
        pg_parser_node_type* node_type = NULL;
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")::"), PG_PARSER_NODETYPE_CHAR);
        pg_parser_mcxt_malloc(NODE_MCXT, (void**)&node_type, sizeof(pg_parser_node_type));
        node_type->m_typeid = resulttype;
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)(node_type), PG_PARSER_NODETYPE_TYPE);
    }
    return true;
}

static bool get_func_expr(pg_parser_FuncExpr* funcval, pg_parser_deparse_context* context, bool showimplicit)
{
    pg_parser_node_func* node_func = NULL;
    int                  nargs;
    pg_parser_ListCell*  l;
    bool                 skip_brackets = false;

    if (funcval->funcformat == COERCE_IMPLICIT_CAST && !showimplicit)
    {
        if (!get_rule_expr_paren((pg_parser_Node*)pg_parser_linitial(funcval->args),
                                 context,
                                 false,
                                 (pg_parser_Node*)funcval))
        {
            return false;
        }
        else
        {
            if (funcval)
            {
                if (funcval->args)
                {
                    pg_parser_list_free(funcval->args);
                }
                pg_parser_mcxt_free(NODE_MCXT, funcval);
            }
            return true;
        }
    }
    if (funcval->funcformat == COERCE_EXPLICIT_CAST || funcval->funcformat == COERCE_IMPLICIT_CAST)
    {
        pg_parser_Node* arg = pg_parser_linitial(funcval->args);
        uint32_t        rettype = funcval->funcresulttype;
        int32_t         coercedTypmod;

        /* Get the typmod if this is a length-coercion function */
        (void)exprIsLengthCoercion((pg_parser_Node*)funcval, &coercedTypmod);

        if (!get_coercion_expr(arg, context, rettype, coercedTypmod, (pg_parser_Node*)funcval))
        {
            return false;
        }
        else
        {
            if (funcval)
            {
                if (funcval->args)
                {
                    pg_parser_list_free(funcval->args);
                }
                pg_parser_mcxt_free(NODE_MCXT, funcval);
            }
            return true;
        }
    }
    node_func = pg_parser_make_nodetree_func(funcval, context);
    if (!node_func)
    {
        return false;
    }

    context->nodetree =
        pg_parser_append_nodetree_with_type(context->nodetree, (void*)node_func, PG_PARSER_NODETYPE_FUNC);
    if (!skip_brackets)
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)("("), PG_PARSER_NODETYPE_CHAR);
    }

    nargs = 0;
    pg_parser_foreach(l, funcval->args)
    {
        if (nargs++ > 0)
        {
            context->nodetree =
                pg_parser_append_nodetree_with_type(context->nodetree, (void*)(", "), PG_PARSER_NODETYPE_CHAR);
        }
        if (!get_rule_expr((pg_parser_Node*)pg_parser_lfirst(l), context, true))
        {
            return false;
        }
    }
    /* v902 change */
    if (!skip_brackets)
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")"), PG_PARSER_NODETYPE_CHAR);
    }

    if (funcval)
    {
        if (funcval->args)
        {
            pg_parser_list_free(funcval->args);
        }
        pg_parser_mcxt_free(NODE_MCXT, funcval);
    }

    return true;
}

static bool get_oper_expr(pg_parser_OpExpr* expr, pg_parser_deparse_context* context)
{
    pg_parser_List*    args = expr->args;
    pg_parser_node_op* opnode = NULL;

    if (!PRETTY_PAREN(context))
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)("("), PG_PARSER_NODETYPE_CHAR);
    }
    if (pg_parser_list_length(args) == 2)
    {
        /* binary operator */
        pg_parser_Node* arg1 = (pg_parser_Node*)pg_parser_linitial(args);
        pg_parser_Node* arg2 = (pg_parser_Node*)pg_parser_lsecond(args);

        if (!get_rule_expr_paren(arg1, context, true, (pg_parser_Node*)expr))
        {
            return false;
        }
        opnode = pg_parser_make_nodetree_op(expr, context);
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)opnode, PG_PARSER_NODETYPE_OP);
        if (!get_rule_expr_paren(arg2, context, true, (pg_parser_Node*)expr))
        {
            return false;
        }
    }
    /* Other cases not supported */
    if (!PRETTY_PAREN(context))
    {
        context->nodetree =
            pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")"), PG_PARSER_NODETYPE_CHAR);
    }
    if (expr)
    {
        if (expr->args)
        {
            pg_parser_list_free(expr->args);
        }
        pg_parser_mcxt_free(NODE_MCXT, expr);
    }
    return true;
}

static bool get_range_partbound_string(pg_parser_List* bound_datums, pg_parser_deparse_context* context)
{
    pg_parser_ListCell* cell;
    bool                sep = false;

    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree, (void*)("("), PG_PARSER_NODETYPE_CHAR);
    pg_parser_foreach(cell, bound_datums)
    {
        pg_parser_PartitionRangeDatum* datum =
            pg_parser_castNode(pg_parser_PartitionRangeDatum, pg_parser_lfirst(cell));
        if (sep)
        {
            context->nodetree =
                pg_parser_append_nodetree_with_type(context->nodetree, (void*)(", "), PG_PARSER_NODETYPE_CHAR);
        }
        if (datum->kind == PARTITION_RANGE_DATUM_MINVALUE)
        {
            context->nodetree =
                pg_parser_append_nodetree_with_type(context->nodetree, (void*)("MINVALUE"), PG_PARSER_NODETYPE_CHAR);
        }
        else if (datum->kind == PARTITION_RANGE_DATUM_MAXVALUE)
        {
            context->nodetree =
                pg_parser_append_nodetree_with_type(context->nodetree, (void*)("MAXVALUE"), PG_PARSER_NODETYPE_CHAR);
        }
        else
        {
            pg_parser_Const* val = pg_parser_castNode(pg_parser_Const, datum->value);

            if (!get_const_expr(val, context))
            {
                return false;
            }
        }
        if (datum)
        {
            pg_parser_mcxt_free(NODE_MCXT, datum);
        }
        sep = true;
    }
    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")"), PG_PARSER_NODETYPE_CHAR);
    return true;
}

bool get_rule_expr(pg_parser_Node* node, pg_parser_deparse_context* context, bool showimplicit)
{
    if (node == NULL)
    {
        return false;
    }

    /* Ignore interrupt and deep nesting checks, process separately based on node tag */

    switch (pg_parser_NodeTagType(node))
    {
        case T_pg_parser_Var:
            get_variable((pg_parser_Var*)node, context);
            break;

        case T_pg_parser_Const:
            if (!get_const_expr((pg_parser_Const*)node, context))
            {
                return false;
            }
            break;

        case T_pg_parser_FuncExpr:
            if (!get_func_expr((pg_parser_FuncExpr*)node, context, showimplicit))
            {
                return false;
            }
            break;

        case T_pg_parser_OpExpr:
            if (!get_oper_expr((pg_parser_OpExpr*)node, context))
            {
                return false;
            }
            break;

        case T_pg_parser_BoolExpr:
        {
            pg_parser_BoolExpr* expr = (pg_parser_BoolExpr*)node;
            pg_parser_Node*     first_arg = pg_parser_linitial(expr->args);
            pg_parser_ListCell* arg = pg_parser_lnext(pg_parser_list_head(expr->args));

            switch (expr->boolop)
            {
                case AND_EXPR:
                    if (!PRETTY_PAREN(context))
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)("("),
                                                                                PG_PARSER_NODETYPE_CHAR);
                    }
                    if (!get_rule_expr_paren(first_arg, context, false, node))
                    {
                        return false;
                    }
                    while (arg)
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)("AND"),
                                                                                PG_PARSER_NODETYPE_CHAR);
                        if (!get_rule_expr_paren((pg_parser_Node*)pg_parser_lfirst(arg), context, false, node))
                        {
                            return false;
                        }
                        arg = pg_parser_lnext(arg);
                    }
                    if (!PRETTY_PAREN(context))
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)(")"),
                                                                                PG_PARSER_NODETYPE_CHAR);
                    }
                    break;

                case OR_EXPR:
                    if (!PRETTY_PAREN(context))
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)("("),
                                                                                PG_PARSER_NODETYPE_CHAR);
                    }
                    if (!get_rule_expr_paren(first_arg, context, false, node))
                    {
                        return false;
                    }
                    while (arg)
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)("OR"),
                                                                                PG_PARSER_NODETYPE_CHAR);
                        if (!get_rule_expr_paren((pg_parser_Node*)pg_parser_lfirst(arg), context, false, node))
                        {
                            return false;
                        }
                        arg = pg_parser_lnext(arg);
                    }
                    if (!PRETTY_PAREN(context))
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)(")"),
                                                                                PG_PARSER_NODETYPE_CHAR);
                    }
                    break;

                case NOT_EXPR:
                    if (!PRETTY_PAREN(context))
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)("("),
                                                                                PG_PARSER_NODETYPE_CHAR);
                    }
                    context->nodetree =
                        pg_parser_append_nodetree_with_type(context->nodetree, (void*)("NOT"), PG_PARSER_NODETYPE_CHAR);
                    if (!get_rule_expr_paren(first_arg, context, false, node))
                    {
                        return false;
                    }
                    if (!PRETTY_PAREN(context))
                    {
                        context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                (void*)(")"),
                                                                                PG_PARSER_NODETYPE_CHAR);
                    }
                    break;

                default:
                    /* todo error handling */
                    break;
            }
            if (node)
            {
                pg_parser_mcxt_free(NODE_MCXT, node);
            }
            break;
        }
        case T_pg_parser_List:
        {
            bool                sep = false;
            pg_parser_ListCell* l;

            pg_parser_foreach(l, (pg_parser_List*)node)
            {
                if (sep)
                {
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)NULL,
                                                                            PG_PARSER_NODETYPE_SEPARATOR);
                }
                get_rule_expr((pg_parser_Node*)pg_parser_lfirst(l), context, showimplicit);
                sep = true;
            }
            if (node)
            {
                pg_parser_list_free((pg_parser_List*)node);
            }
            break;
        }
        case T_pg_parser_PartitionBoundSpec:
        {
            pg_parser_PartitionBoundSpec* spec = (pg_parser_PartitionBoundSpec*)node;
            pg_parser_ListCell*           cell;
            bool                          sep = false;

            if (spec->is_default)
            {
                context->nodetree =
                    pg_parser_append_nodetree_with_type(context->nodetree, (void*)("DEFAULT"), PG_PARSER_NODETYPE_CHAR);
                if (spec)
                {
                    if (spec->listdatums)
                    {
                        pg_parser_list_free(spec->listdatums);
                    }
                    if (spec->lowerdatums)
                    {
                        pg_parser_list_free(spec->lowerdatums);
                    }
                    if (spec->upperdatums)
                    {
                        pg_parser_list_free(spec->upperdatums);
                    }
                    pg_parser_mcxt_free(NODE_MCXT, spec);
                }
                break;
            }

            switch (spec->strategy)
            {
                case PG_PARSER_PARTITION_STRATEGY_HASH:
                {
                    char temp_char[1024] = {'\0'};
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("FOR VALUES"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    sprintf(temp_char, " WITH (modulus %d, remainder %d)", spec->modulus, spec->remainder);
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)temp_char,
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }

                case PG_PARSER_PARTITION_STRATEGY_LIST:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("FOR VALUES IN ("),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    pg_parser_foreach(cell, spec->listdatums)
                    {
                        pg_parser_Const* val = pg_parser_castNode(pg_parser_Const, pg_parser_lfirst(cell));

                        if (sep)
                        {
                            context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                                    (void*)(", "),
                                                                                    PG_PARSER_NODETYPE_CHAR);
                        }
                        get_const_expr(val, context);
                        sep = true;
                    }

                    context->nodetree =
                        pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")"), PG_PARSER_NODETYPE_CHAR);
                    break;

                case PG_PARSER_PARTITION_STRATEGY_RANGE:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("FOR VALUES FROM "),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    if (!get_range_partbound_string(spec->lowerdatums, context))
                    {
                        return false;
                    }

                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(" TO  "),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    if (!get_range_partbound_string(spec->upperdatums, context))
                    {
                        return false;
                    }

                    break;

                default:
                    /* todo error handling */
                    break;
            }
            if (spec)
            {
                if (spec->listdatums)
                {
                    pg_parser_list_free(spec->listdatums);
                }
                if (spec->lowerdatums)
                {
                    pg_parser_list_free(spec->lowerdatums);
                }
                if (spec->upperdatums)
                {
                    pg_parser_list_free(spec->upperdatums);
                }
                pg_parser_mcxt_free(NODE_MCXT, spec);
            }

            break;
        }
        case T_pg_parser_CoerceViaIO:
        {
            pg_parser_CoerceViaIO* iocoerce = (pg_parser_CoerceViaIO*)node;
            pg_parser_Node*        arg = (pg_parser_Node*)iocoerce->arg;
            if (!get_coercion_expr(arg, context, iocoerce->resulttype, -1, node))
            {
                return false;
            }
            if (iocoerce)
            {
                pg_parser_mcxt_free(NODE_MCXT, iocoerce);
            }
        }
        break;

        case T_pg_parser_SQLValueFunction:
        {
            pg_parser_SQLValueFunction* svf = (pg_parser_SQLValueFunction*)node;

            /*
             * Note: this code knows that typmod for time, timestamp, and
             * timestamptz just prints as integer.
             */
            switch (svf->op)
            {
                case SVFOP_CURRENT_DATE:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_DATE"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_CURRENT_TIME:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_TIME"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_CURRENT_TIME_N:
                {
                    char temp_str[128] = {'\0'};
                    sprintf(temp_str, "CURRENT_TIME(%d)", svf->typmod);
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(temp_str),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }
                case SVFOP_CURRENT_TIMESTAMP:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_TIMESTAMP"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_CURRENT_TIMESTAMP_N:
                {
                    char temp_str[128] = {'\0'};
                    sprintf(temp_str, "CURRENT_TIMESTAMP(%d)", svf->typmod);
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(temp_str),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }
                case SVFOP_LOCALTIME:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("LOCALTIME"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_LOCALTIME_N:
                {
                    char temp_str[128] = {'\0'};
                    sprintf(temp_str, "LOCALTIME(%d)", svf->typmod);
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(temp_str),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }
                case SVFOP_LOCALTIMESTAMP:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("LOCALTIMESTAMP"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_LOCALTIMESTAMP_N:
                {
                    char temp_str[128] = {'\0'};
                    sprintf(temp_str, "LOCALTIMESTAMP(%d)", svf->typmod);
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(temp_str),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }
                case SVFOP_CURRENT_ROLE:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_ROLE"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_CURRENT_USER:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_USER"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_USER:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("USER"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_SESSION_USER:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("SESSION_USER"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_CURRENT_CATALOG:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_CATALOG"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                case SVFOP_CURRENT_SCHEMA:
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)("CURRENT_SCHEMA"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
            }
        }
        break;
        case T_pg_parser_NullTest:
        {
            pg_parser_NullTest* ntest = (pg_parser_NullTest*)node;
            if (!PRETTY_PAREN(context))
            {
                context->nodetree =
                    pg_parser_append_nodetree_with_type(context->nodetree, (void*)("("), PG_PARSER_NODETYPE_CHAR);
            }
            get_rule_expr_paren((pg_parser_Node*)ntest->arg, context, true, node);
            /*
             * For scalar inputs, we prefer to print as IS [NOT] NULL,
             * which is shorter and traditional.  If it's a rowtype input
             * but we're applying a scalar test, must print IS [NOT]
             * DISTINCT FROM NULL to be semantically correct.
             */
            switch (ntest->nulltesttype)
            {
                case IS_NULL:
                {
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(" IS NULL"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }
                case IS_NOT_NULL:
                {
                    context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                            (void*)(" IS NOT NULL"),
                                                                            PG_PARSER_NODETYPE_CHAR);
                    break;
                }
            }
            if (!PRETTY_PAREN(context))
            {
                context->nodetree =
                    pg_parser_append_nodetree_with_type(context->nodetree, (void*)(")"), PG_PARSER_NODETYPE_CHAR);
            }
        }
        break;
        default:
            /* todo error handling */
            context->nodetree = pg_parser_append_nodetree_with_type(context->nodetree,
                                                                    (void*)("[UNSUPPORT NODE]"),
                                                                    PG_PARSER_NODETYPE_CHAR);
            pg_parser_mcxt_free(NODE_MCXT, node);
            return true;
            break;
    }
    return true;
}
