#include <ctype.h>
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

#define PARSERNODE_MCXT   NULL
#define PRETTYFLAG_INDENT 0x0002
#define RIGHT_PAREN       (1000000 + 1)
#define LEFT_PAREN        (1000000 + 2)
#define LEFT_BRACE        (1000000 + 3)
#define OTHER_TOKEN       (1000000 + 4)
/* Function declaration */
static pg_parser_nodetree* pg_parser_get_expr_worker(
    char* expr, int32_t prettyFlags, pg_parser_translog_convertinfo_with_zic* zicinfo);
static void* stringToNode(const char* str, int16_t dbtype, char* dbversion);
static void* stringToNodeInternal(const char* str, int16_t dbtype, char* dbversion);
static void* nodeRead(
    const char* token, int32_t tok_len, char** strtok_ptr, int16_t dbtype, char* dbversion);
static const char* pg_parser_strtok(int32_t* length, char** strtok_ptr);
static pg_parser_NodeTag nodeTokenType(const char* token, int32_t length);
static pg_parser_Value* makeInteger(int32_t i);
static pg_parser_Value* makeFloat(char* numericStr);
static pg_parser_Value* makeString(char* str);
static char* debackslash(const char* token, int32_t length);
static pg_parser_Node* parseNodeString(char** strtok_ptr, int16_t dbtype, char* dbversion);
static pg_parser_Datum readDatum(bool typbyval, char** strtok_ptr, bool* need_free);
static pg_parser_AttrNumber* readAttrNumberCols(int32_t numCols, char** strtok_ptr);
static uint32_t* readOidCols(int32_t numCols, char** strtok_ptr);
static int32_t* readIntCols(int32_t numCols, char** strtok_ptr);
static bool* readBoolCols(int32_t numCols, char** strtok_ptr);
static void ReadCommonScan(pg_parser_Scan* local_node,
                           char**          strtok_ptr,
                           int16_t         dbtype,
                           char*           dbversion);

pg_parser_nodetree* pg_parser_get_expr(char* expr, pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    int32_t             prettyFlags = PRETTYFLAG_INDENT;
    pg_parser_nodetree* result = pg_parser_get_expr_worker(expr, prettyFlags, zicinfo);
    if (NULL == result)
    {
        return NULL;
    }
    return result;
}

#define WRAP_COLUMN_DEFAULT 0
static pg_parser_nodetree* deparse_expression_pretty(
    pg_parser_Node*                          expr,
    pg_parser_List*                          dpcontext,
    bool                                     forceprefix,
    bool                                     showimplicit,
    int32_t                                  prettyFlags,
    int32_t                                  startIndent,
    pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    pg_parser_deparse_context context;

    context.namespaces = dpcontext;
    context.windowClause = NIL;
    context.windowTList = NIL;
    context.varprefix = forceprefix;
    context.prettyFlags = prettyFlags;
    context.wrapColumn = WRAP_COLUMN_DEFAULT;
    context.indentLevel = startIndent;
    context.special_exprkind = EXPR_KIND_NONE;
    context.zicinfo = zicinfo;
    context.nodetree = NULL;
    if (!get_rule_expr(expr, &context, showimplicit))
    {
        /* Cleanup */
        if (context.nodetree)
        {
            pg_parser_nodetree* nodetree = context.nodetree;
            pg_parser_nodetree* nodetree_now = nodetree;
            while (nodetree)
            {
                nodetree_now = nodetree;
                nodetree = nodetree->m_next;
                if (nodetree_now)
                {
                    if (nodetree_now->m_node)
                    {
                        switch (nodetree_now->m_node_type)
                        {
                            case PG_PARSER_NODETYPE_CONST:
                            {
                                pg_parser_node_const* node_const =
                                    (pg_parser_node_const*)nodetree_now->m_node;
                                if (node_const->m_char_value)
                                {
                                    pg_parser_mcxt_free(NODE_MCXT, node_const->m_char_value);
                                }
                                pg_parser_mcxt_free(NODE_MCXT, nodetree_now->m_node);
                                break;
                            }
                            case PG_PARSER_NODETYPE_FUNC:
                            {
                                pg_parser_node_func* node_func =
                                    (pg_parser_node_func*)nodetree_now->m_node;
                                if (node_func->m_funcname)
                                {
                                    pg_parser_mcxt_free(NODE_MCXT, node_func->m_funcname);
                                }
                                pg_parser_mcxt_free(NODE_MCXT, nodetree_now->m_node);
                                break;
                            }
                            case PG_PARSER_NODETYPE_OP:
                            {
                                pg_parser_node_op* node_op =
                                    (pg_parser_node_op*)nodetree_now->m_node;
                                if (node_op->m_opname)
                                {
                                    pg_parser_mcxt_free(NODE_MCXT, node_op->m_opname);
                                }
                                pg_parser_mcxt_free(NODE_MCXT, nodetree_now->m_node);
                                break;
                            }
                            default:
                                pg_parser_mcxt_free(NODE_MCXT, nodetree_now->m_node);
                                break;
                        }
                    }
                    pg_parser_mcxt_free(NODE_MCXT, nodetree_now);
                    nodetree_now = NULL;
                }
            }
        }
        return NULL;
    }
    return context.nodetree;
}

static pg_parser_nodetree* pg_parser_get_expr_worker(
    char* expr, int32_t prettyFlags, pg_parser_translog_convertinfo_with_zic* zicinfo)
{
    pg_parser_Node*     node;
    pg_parser_nodetree* result = NULL;

    /* Convert expression to node tree */
    node = (pg_parser_Node*)stringToNode(expr, zicinfo->dbtype, zicinfo->dbversion);

    if (NULL == node)
    {
        return NULL;
    }
    /* Deparse */
    result = deparse_expression_pretty(node, NIL, false, false, prettyFlags, 0, zicinfo);

    /* node has been released while parsing */
    return result;
}

static void* stringToNode(const char* str, int16_t dbtype, char* dbversion)
{
    return stringToNodeInternal(str, dbtype, dbversion);
}

static void* stringToNodeInternal(const char* str, int16_t dbtype, char* dbversion)
{
    void* retval;
    char* strtok_ptr;

    strtok_ptr = (char*)str; /* point pg_parser_strtok at the string to read */

    /*
     * If enabled, likewise save/restore the location field handling flag.
     */

    retval = nodeRead(NULL, 0, &strtok_ptr, dbtype, dbversion); /* do the reading */

    return retval;
}

static pg_parser_NodeTag nodeTokenType(const char* token, int32_t length)
{
    pg_parser_NodeTag retval;
    const char*       numptr;
    int32_t           numlen;

    /*
     * Check if the token is a number
     */
    numptr = token;
    numlen = length;
    if (*numptr == '+' || *numptr == '-')
    {
        numptr++, numlen--;
    }
    if ((numlen > 0 && isdigit((unsigned char)*numptr)) ||
        (numlen > 1 && *numptr == '.' && isdigit((unsigned char)numptr[1])))
    {
        /*
         * Yes.  Figure out whether it is integral or float; this requires
         * both a syntax check and a range check. strtoint() can do both for
         * us. We know the token will end at a character that strtoint will
         * stop at, so we do not need to modify the string.
         */
        char* endptr;

        errno = 0;
        (void)strtoint(token, &endptr, 10);
        if (endptr != token + length || errno == ERANGE)
        {
            return T_pg_parser_Float;
        }
        return T_pg_parser_Integer;
    }

    /*
     * these three cases do not need length checks, since pg_parser_strtok() will
     * always treat them as single-byte tokens
     */
    else if (*token == '(')
    {
        retval = LEFT_PAREN;
    }
    else if (*token == ')')
    {
        retval = RIGHT_PAREN;
    }
    else if (*token == '{')
    {
        retval = LEFT_BRACE;
    }
    else if (*token == '"' && length > 1 && token[length - 1] == '"')
    {
        retval = T_pg_parser_String;
    }
    else if (*token == 'b')
    {
        retval = T_pg_parser_BitString;
    }
    else
    {
        retval = OTHER_TOKEN;
    }
    return retval;
}

/*
 *    makeBitString
 *
 * Caller is responsible for passing a palloc'd string.
 */
static pg_parser_Value* makeBitString(char* str)
{
    pg_parser_Value* v = pg_parser_makeNode(pg_parser_Value);

    v->type = T_pg_parser_BitString;
    v->val.str = str;
    return v;
}

static void* nodeRead(
    const char* token, int32_t tok_len, char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    pg_parser_Node*   result;
    pg_parser_NodeTag type;

    if (token == NULL) /* need to read a token? */
    {
        token = pg_parser_strtok(&tok_len, strtok_ptr);

        if (token == NULL) /* end of input */
        {
            return NULL;
        }
    }

    type = nodeTokenType(token, tok_len);

    switch ((int32_t)type)
    {
        case LEFT_BRACE:
            result = parseNodeString(strtok_ptr, dbtype, dbversion);
            token = pg_parser_strtok(&tok_len, strtok_ptr);
            if (token == NULL || token[0] != '}')
            {
                pg_parser_mcxt_free(NODE_MCXT, result);
                return NULL;
            }
            break;
        case LEFT_PAREN:
        {
            pg_parser_List* l = NIL;

            /*----------
             * Could be an integer list:    (i int32_t int32_t ...)
             * or an OID list:                (o int32_t int32_t ...)
             * or a list of nodes/values:    (node node ...)
             *----------
             */
            token = pg_parser_strtok(&tok_len, strtok_ptr);
            if (token == NULL)
            {
                return NULL;
            }
            if (tok_len == 1 && token[0] == 'i')
            {
                /* List of integers */
                for (;;)
                {
                    int32_t val;
                    char*   endptr;

                    token = pg_parser_strtok(&tok_len, strtok_ptr);
                    if (token == NULL)
                    {
                        return NULL;
                    }
                    if (token[0] == ')')
                    {
                        break;
                    }
                    val = (int32_t)strtol(token, &endptr, 10);
                    if (endptr != token + tok_len)
                    {
                        return NULL;
                    }
                    l = pg_parser_list_lappend_int(l, val);
                }
            }
            else if (tok_len == 1 && token[0] == 'o')
            {
                /* List of OIDs */
                for (;;)
                {
                    uint32_t val;
                    char*    endptr;

                    token = pg_parser_strtok(&tok_len, strtok_ptr);
                    if (token == NULL)
                    {
                        return NULL;
                    }
                    if (token[0] == ')')
                    {
                        break;
                    }
                    val = (uint32_t)strtoul(token, &endptr, 10);
                    if (endptr != token + tok_len)
                    {
                        return NULL;
                    }
                    l = pg_parser_list_lappend_oid(l, val);
                }
            }
            else
            {
                /* List of other node types */
                for (;;)
                {
                    /* We have already scanned next token... */
                    if (token[0] == ')')
                    {
                        break;
                    }
                    l = pg_parser_list_lappend(
                        l, nodeRead(token, tok_len, strtok_ptr, dbtype, dbversion));
                    token = pg_parser_strtok(&tok_len, strtok_ptr);
                    if (token == NULL)
                    {
                        pg_parser_ListCell* cell = NULL;
                        pg_parser_foreach(cell, l)
                        {
                            void* ptr = (void*)pg_parser_lfirst(cell);
                            pg_parser_mcxt_free(NODE_MCXT, ptr);
                        }
                        pg_parser_list_free(l);
                        return NULL;
                    }
                }
            }
            result = (pg_parser_Node*)l;
            break;
        }
        case RIGHT_PAREN:
            result = NULL; /* keep compiler happy */
            break;
        case OTHER_TOKEN:
            if (tok_len == 0)
            {
                /* must be "<>" --- represents a null pointer */
                result = NULL;
            }
            else
            {
                result = NULL; /* keep compiler happy */
            }
            break;
        case T_pg_parser_Integer:

            /*
             * we know that the token terminates on a char atoi will stop at
             */
            result = (pg_parser_Node*)makeInteger(atoi(token));
            break;
        case T_pg_parser_Float:
        {
            char* fval = NULL;

            if (!pg_parser_mcxt_malloc(PARSERNODE_MCXT, (void**)&fval, tok_len + 1))
            {
                return NULL;
            }

            rmemcpy0(fval, 0, token, tok_len);
            fval[tok_len] = '\0';
            result = (pg_parser_Node*)makeFloat(fval);
        }
        break;
        case T_pg_parser_String:
            /* need to remove leading and trailing quotes, and backslashes */
            result = (pg_parser_Node*)makeString(debackslash(token + 1, tok_len - 2));
            break;
        case T_pg_parser_BitString:
        {
            char* val = NULL;

            if (!pg_parser_mcxt_malloc(PARSERNODE_MCXT, (void**)&val, tok_len))
            {
                return NULL;
            }
            /* skip leading 'b' */
            rmemcpy0(val, 0, token + 1, tok_len - 1);
            val[tok_len - 1] = '\0';
            result = (pg_parser_Node*)makeBitString(val);
            break;
        }
        default:
            result = NULL; /* keep compiler happy */
            break;
    }

    return (void*)result;
}

static const char* pg_parser_strtok(int32_t* length, char** strtok_ptr)
{
    const char* local_str; /* working pointer to string */
    const char* ret_str;   /* start of token to return */

    local_str = *strtok_ptr;

    while (*local_str == ' ' || *local_str == '\n' || *local_str == '\t')
    {
        local_str++;
    }

    if (*local_str == '\0')
    {
        *length = 0;
        *strtok_ptr = (char*)local_str;
        return NULL; /* no more tokens */
    }

    /*
     * Now pointing at start of next token.
     */
    ret_str = local_str;

    if (*local_str == '(' || *local_str == ')' || *local_str == '{' || *local_str == '}')
    {
        /* special 1-character token */
        local_str++;
    }
    else
    {
        /* Normal token, possibly containing backslashes */
        while (*local_str != '\0' && *local_str != ' ' && *local_str != '\n' &&
               *local_str != '\t' && *local_str != '(' && *local_str != ')' && *local_str != '{' &&
               *local_str != '}')
        {
            if (*local_str == '\\' && local_str[1] != '\0')
            {
                local_str += 2;
            }
            else
            {
                local_str++;
            }
        }
    }

    *length = local_str - ret_str;

    /* Recognize special case for "empty" token */
    if (*length == 2 && ret_str[0] == '<' && ret_str[1] == '>')
    {
        *length = 0;
    }

    *strtok_ptr = (char*)local_str;

    return ret_str;
}

static pg_parser_Value* makeInteger(int32_t i)
{
    pg_parser_Value* v = pg_parser_makeNode(pg_parser_Value);

    v->type = T_pg_parser_Integer;
    v->val.ival = i;
    return v;
}

static pg_parser_Value* makeFloat(char* numericStr)
{
    pg_parser_Value* v = pg_parser_makeNode(pg_parser_Value);

    v->type = T_pg_parser_Float;
    v->val.str = numericStr;
    return v;
}

static pg_parser_Value* makeString(char* str)
{
    pg_parser_Value* v = pg_parser_makeNode(pg_parser_Value);

    v->type = T_pg_parser_String;
    v->val.str = str;
    return v;
}

static char* debackslash(const char* token, int32_t length)
{
    char* result = NULL;
    char* ptr = NULL;

    pg_parser_mcxt_malloc(PARSERNODE_MCXT, (void**)&result, length + 1);

    ptr = result;

    while (length > 0)
    {
        if (*token == '\\' && length > 1)
        {
            token++, length--;
        }
        *ptr++ = *token++;
        length--;
    }
    *ptr = '\0';
    return result;
}

/* --------------------------- readfunc --------------------------- */

#define atoui(x)                       ((unsigned int)strtoul((x), NULL, 10))
#define atooid(x)                      ((uint32_t)strtoul((x), NULL, 10))
#define strtobool(x)                   ((*(x) == 't') ? true : false)
#define nullable_string(token, length) ((length) == 0 ? NULL : debackslash(token, length))

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
    nodeTypeName* local_node = pg_parser_makeNode(nodeTypeName)

/* And a few guys need only the pg_parser_strtok support fields */
#define READ_TEMP_LOCALS() \
    const char* token;     \
    int32_t     length

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)        \
    READ_LOCALS_NO_FIELDS(nodeTypeName); \
    READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname)                                          \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname)                                         \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = atoui(token)

/* Read an unsigned integer field (anything written using UINT64_FORMAT) */
#define READ_UINT64_FIELD(fldname)                                       \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = strtoul(token, NULL, 10)

/* Read a long integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname)                                         \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = atol(token)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname)                                          \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = atooid(token)

#define SKIP_OID_FIELD(fldname)                                        \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */

#define SKIP_INT_FIELD(fldname)                                        \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname)                                         \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    /* avoid overhead of calling debackslash() for one char */           \
    local_node->fldname = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype)                               \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = (enumtype)atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname)                                        \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname)                                         \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = strtobool(token)

#define SKIP_BOOL_FIELD(fldname)                                       \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */

#define SKIP_ANY_FIELD(fldname)                                        \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */

/* Read a character-string field */
#define READ_STRING_FIELD(fldname)                                       \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and possibly throw away the value) */
#ifdef WRITE_READ_PARSE_PLAN_TREES
#define READ_LOCATION_FIELD(fldname)                           \
    token = strtok(&length, strtok_ptr); /* skip :fldname */   \
    token = strtok(&length, strtok_ptr); /* get field value */ \
    local_node->fldname = restore_location_fields ? atoi(token) : -1
#else
#define READ_LOCATION_FIELD(fldname)                                                \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */              \
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */            \
    (void)token;                                   /* in case not used elsewhere */ \
    local_node->fldname = -1                       /* set field to "unknown" */
#endif

/* Read a pg_parser_Node field */
#define READ_NODE_FIELD(fldname)                                                    \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */              \
    (void)token;                                   /* in case not used elsewhere */ \
    local_node->fldname = nodeRead(NULL, 0, strtok_ptr, dbtype, dbversion)

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname)                                               \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */              \
    (void)token;                                   /* in case not used elsewhere */ \
    local_node->fldname = _readBitmapset(strtok_ptr, dbtype, dbversion)

/* Read an attribute number array */
#define READ_ATTRNUMBER_ARRAY(fldname, len)                            \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    local_node->fldname = readAttrNumberCols(len, strtok_ptr)

/* Read an oid array */
#define READ_OID_ARRAY(fldname, len)                                   \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    local_node->fldname = readOidCols(len, strtok_ptr)

/* Read an int32_t array */
#define READ_INT_ARRAY(fldname, len)                                   \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    local_node->fldname = readIntCols(len, strtok_ptr)

/* Read a bool array */
#define READ_BOOL_ARRAY(fldname, len)                                  \
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :fldname */ \
    local_node->fldname = readBoolCols(len, strtok_ptr)

/* Routine exit */
#define READ_DONE() return local_node

/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)                       ((unsigned int)strtoul((x), NULL, 10))

#define strtobool(x)                   ((*(x) == 't') ? true : false)

#define nullable_string(token, length) ((length) == 0 ? NULL : debackslash(token, length))

#define PG_PARSER_WORDNUM(x)           ((x) / 64)
#define PG_PARSER_BITNUM(x)            ((x) % 64)

#define PG_PARSER_BITMAPSET_SIZE(nwords) \
    (offsetof(pg_parser_Bitmapset, words) + (nwords) * sizeof(pg_parser_bitmapword))

static pg_parser_Bitmapset* bms_make_singleton(int x)
{
    pg_parser_Bitmapset* result = NULL;
    int                  wordnum, bitnum;

    if (x < 0)
    {
        return NULL;
    }
    wordnum = PG_PARSER_WORDNUM(x);
    bitnum = PG_PARSER_BITNUM(x);
    if (!pg_parser_mcxt_realloc(NODE_MCXT, (void**)&result, PG_PARSER_BITMAPSET_SIZE(wordnum + 1)))
    {
        return NULL;
    }
    result->nwords = wordnum + 1;
    result->words[wordnum] = ((pg_parser_bitmapword)1 << bitnum);
    return result;
}

static pg_parser_Bitmapset* bms_add_member(pg_parser_Bitmapset* a, int x)
{
    int wordnum, bitnum;

    if (x < 0)
    {
        return NULL;
    }

    if (a == NULL)
    {
        return bms_make_singleton(x);
    }
    wordnum = PG_PARSER_WORDNUM(x);
    bitnum = PG_PARSER_BITNUM(x);

    /* enlarge the set if necessary */
    if (wordnum >= a->nwords)
    {
        int oldnwords = a->nwords;
        int i;

        if (!pg_parser_mcxt_realloc(NODE_MCXT, (void**)&a, PG_PARSER_BITMAPSET_SIZE(wordnum + 1)))
        {
            return NULL;
        }
        a->nwords = wordnum + 1;
        /* zero out the enlarged portion */
        for (i = oldnwords; i < a->nwords; i++)
        {
            a->words[i] = 0;
        }
    }

    a->words[wordnum] |= ((pg_parser_bitmapword)1 << bitnum);
    return a;
}

/*
 * _readBitmapset
 */
static pg_parser_Bitmapset* _readBitmapset(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    pg_parser_Bitmapset* result = NULL;
    READ_TEMP_LOCALS();

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    token = pg_parser_strtok(&length, strtok_ptr);
    if (token == NULL)
    {
        return NULL;
    }

    if (length != 1 || token[0] != '(')
    {
        return NULL;
    }

    token = pg_parser_strtok(&length, strtok_ptr);
    if (token == NULL)
    {
        return NULL;
    }

    if (length != 1 || token[0] != 'b')
    {
        return NULL;
    }

    for (;;)
    {
        int32_t val;
        char*   endptr;

        token = pg_parser_strtok(&length, strtok_ptr);
        if (token == NULL)
        {
            return NULL;
        }
        if (length == 1 && token[0] == ')')
        {
            break;
        }
        val = (int32_t)strtol(token, &endptr, 10);
        if (endptr != token + length)
        {
            return NULL;
        }
        result = bms_add_member(result, val);
    }

    return result;
}
#define PG_PARSER_UINT64CONST(x) (x##UL)
/*
 * _readQuery
 */
static pg_parser_Query* _readQuery(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Query);

    READ_ENUM_FIELD(commandType, pg_parser_CmdType);
    READ_ENUM_FIELD(querySource, pg_parser_QuerySource);
    local_node->queryId = PG_PARSER_UINT64CONST(0); /* not saved in output format */
    READ_BOOL_FIELD(canSetTag);
    READ_NODE_FIELD(utilityStmt);
    READ_INT_FIELD(resultRelation);
    READ_BOOL_FIELD(hasAggs);
    READ_BOOL_FIELD(hasWindowFuncs);
    READ_BOOL_FIELD(hasTargetSRFs);
    READ_BOOL_FIELD(hasSubLinks);
    READ_BOOL_FIELD(hasDistinctOn);
    READ_BOOL_FIELD(hasRecursive);
    READ_BOOL_FIELD(hasModifyingCTE);
    READ_BOOL_FIELD(hasForUpdate);
    READ_BOOL_FIELD(hasRowSecurity);
    READ_NODE_FIELD(cteList);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(jointree);
    READ_NODE_FIELD(targetList);
    READ_ENUM_FIELD(override, pg_parser_OverridingKind);
    READ_NODE_FIELD(onConflict);
    READ_NODE_FIELD(returningList);
    READ_NODE_FIELD(groupClause);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(havingQual);
    READ_NODE_FIELD(windowClause);
    READ_NODE_FIELD(distinctClause);
    READ_NODE_FIELD(sortClause);
    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);
    READ_NODE_FIELD(rowMarks);
    READ_NODE_FIELD(setOperations);
    READ_NODE_FIELD(constraintDeps);
    READ_NODE_FIELD(withCheckOptions);
    READ_LOCATION_FIELD(stmt_location);
    READ_LOCATION_FIELD(stmt_len);

    READ_DONE();
}

/*
 * _readNotifyStmt
 */
static pg_parser_NotifyStmt* _readNotifyStmt(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_NotifyStmt);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_STRING_FIELD(conditionname);
    READ_STRING_FIELD(payload);

    READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static pg_parser_DeclareCursorStmt* _readDeclareCursorStmt(char**  strtok_ptr,
                                                           int16_t dbtype,
                                                           char*   dbversion)
{
    READ_LOCALS(pg_parser_DeclareCursorStmt);

    READ_STRING_FIELD(portalname);
    READ_INT_FIELD(options);
    READ_NODE_FIELD(query);

    READ_DONE();
}

/*
 * _readWithCheckOption
 */
static pg_parser_WithCheckOption* _readWithCheckOption(char**  strtok_ptr,
                                                       int16_t dbtype,
                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_WithCheckOption);

    READ_ENUM_FIELD(kind, pg_parser_WCOKind);
    READ_STRING_FIELD(relname);
    READ_STRING_FIELD(polname);
    READ_NODE_FIELD(qual);
    READ_BOOL_FIELD(cascaded);

    READ_DONE();
}

/*
 * _readSortGroupClause
 */
static pg_parser_SortGroupClause* _readSortGroupClause(char**  strtok_ptr,
                                                       int16_t dbtype,
                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_SortGroupClause);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_UINT_FIELD(tleSortGroupRef);
    READ_OID_FIELD(eqop);
    READ_OID_FIELD(sortop);
    READ_BOOL_FIELD(nulls_first);
    READ_BOOL_FIELD(hashable);

    READ_DONE();
}

/*
 * _readGroupingSet
 */
static pg_parser_GroupingSet* _readGroupingSet(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_GroupingSet);

    READ_ENUM_FIELD(kind, pg_parser_GroupingSetKind);
    READ_NODE_FIELD(content);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readWindowClause
 */
static pg_parser_WindowClause* _readWindowClause(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_WindowClause);

    READ_STRING_FIELD(name);
    READ_STRING_FIELD(refname);
    READ_NODE_FIELD(partitionClause);
    READ_NODE_FIELD(orderClause);
    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);
    READ_OID_FIELD(startInRangeFunc);
    READ_OID_FIELD(endInRangeFunc);
    READ_OID_FIELD(inRangeColl);
    READ_BOOL_FIELD(inRangeAsc);
    READ_BOOL_FIELD(inRangeNullsFirst);
    READ_UINT_FIELD(winref);
    READ_BOOL_FIELD(copiedOrder);

    READ_DONE();
}

/*
 * _readRowMarkClause
 */
static pg_parser_RowMarkClause* _readRowMarkClause(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_RowMarkClause);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_UINT_FIELD(rti);
    READ_ENUM_FIELD(strength, pg_parser_LockClauseStrength);
    READ_ENUM_FIELD(waitPolicy, pg_parser_LockWaitPolicy);
    READ_BOOL_FIELD(pushedDown);

    READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static pg_parser_CommonTableExpr* _readCommonTableExpr(char**  strtok_ptr,
                                                       int16_t dbtype,
                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_CommonTableExpr);

    READ_STRING_FIELD(ctename);
    READ_NODE_FIELD(aliascolnames);
    READ_ENUM_FIELD(ctematerialized, pg_parser_CTEMaterialize);
    READ_NODE_FIELD(ctequery);
    READ_LOCATION_FIELD(location);
    READ_BOOL_FIELD(cterecursive);
    READ_INT_FIELD(cterefcount);
    READ_NODE_FIELD(ctecolnames);
    READ_NODE_FIELD(ctecoltypes);
    READ_NODE_FIELD(ctecoltypmods);
    READ_NODE_FIELD(ctecolcollations);

    READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static pg_parser_SetOperationStmt* _readSetOperationStmt(char**  strtok_ptr,
                                                         int16_t dbtype,
                                                         char*   dbversion)
{
    READ_LOCALS(pg_parser_SetOperationStmt);

    READ_ENUM_FIELD(op, pg_parser_SetOperation);
    READ_BOOL_FIELD(all);
    READ_NODE_FIELD(larg);
    READ_NODE_FIELD(rarg);
    READ_NODE_FIELD(colTypes);
    READ_NODE_FIELD(colTypmods);
    READ_NODE_FIELD(colCollations);
    READ_NODE_FIELD(groupClauses);

    READ_DONE();
}

/*
 *    Stuff from primnodes.h.
 */

static pg_parser_Alias* _readAlias(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Alias);

    READ_STRING_FIELD(aliasname);
    READ_NODE_FIELD(colnames);

    READ_DONE();
}

static pg_parser_RangeVar* _readRangeVar(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_RangeVar);

    local_node->catalogname = NULL; /* not currently saved in output format */

    READ_STRING_FIELD(schemaname);
    READ_STRING_FIELD(relname);
    READ_BOOL_FIELD(inh);
    READ_CHAR_FIELD(relpersistence);
    READ_NODE_FIELD(alias);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readTableFunc
 */
static pg_parser_TableFunc* _readTableFunc(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_TableFunc);

    READ_NODE_FIELD(ns_uris);
    READ_NODE_FIELD(ns_names);
    READ_NODE_FIELD(docexpr);
    READ_NODE_FIELD(rowexpr);
    READ_NODE_FIELD(colnames);
    READ_NODE_FIELD(coltypes);
    READ_NODE_FIELD(coltypmods);
    READ_NODE_FIELD(colcollations);
    READ_NODE_FIELD(colexprs);
    READ_NODE_FIELD(coldefexprs);
    READ_BITMAPSET_FIELD(notnulls);
    READ_INT_FIELD(ordinalitycol);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

static pg_parser_IntoClause* _readIntoClause(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_IntoClause);

    READ_NODE_FIELD(rel);
    READ_NODE_FIELD(colNames);
    READ_STRING_FIELD(accessMethod);
    READ_NODE_FIELD(options);
    READ_ENUM_FIELD(onCommit, pg_parser_OnCommitAction);
    READ_STRING_FIELD(tableSpaceName);
    READ_NODE_FIELD(viewQuery);
    READ_BOOL_FIELD(skipData);

    READ_DONE();
}

/*
 * _readVar
 */
static pg_parser_Var* _readVar(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Var);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_UINT_FIELD(varno);
    READ_INT_FIELD(varattno);
    READ_OID_FIELD(vartype);
    READ_INT_FIELD(vartypmod);
    READ_OID_FIELD(varcollid);
    READ_UINT_FIELD(varlevelsup);
    READ_UINT_FIELD(varnoold);
    READ_INT_FIELD(varoattno);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readConst
 */
static pg_parser_Const* _readConst(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Const);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_OID_FIELD(consttype);
    READ_INT_FIELD(consttypmod);
    READ_OID_FIELD(constcollid);
    READ_INT_FIELD(constlen);
    READ_BOOL_FIELD(constbyval);
    READ_BOOL_FIELD(constisnull);
    READ_LOCATION_FIELD(location);

    token = pg_parser_strtok(&length, strtok_ptr); /* skip :constvalue */
    if (local_node->constisnull)
    {
        token = pg_parser_strtok(&length, strtok_ptr); /* skip "<>" */
    }
    else
    {
        local_node->constvalue =
            readDatum(local_node->constbyval, strtok_ptr, &local_node->constneedfree);
    }

    READ_DONE();
}

/*
 * _readParam
 */
static pg_parser_Param* _readParam(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Param);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_ENUM_FIELD(paramkind, pg_parser_ParamKind);
    READ_INT_FIELD(paramid);
    READ_OID_FIELD(paramtype);
    READ_INT_FIELD(paramtypmod);
    READ_OID_FIELD(paramcollid);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readAggref
 */
static pg_parser_Aggref* _readAggref(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Aggref);

    READ_OID_FIELD(aggfnoid);
    READ_OID_FIELD(aggtype);
    READ_OID_FIELD(aggcollid);
    READ_OID_FIELD(inputcollid);
    READ_OID_FIELD(aggtranstype);
    READ_NODE_FIELD(aggargtypes);
    READ_NODE_FIELD(aggdirectargs);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(aggorder);
    READ_NODE_FIELD(aggdistinct);
    READ_NODE_FIELD(aggfilter);
    READ_BOOL_FIELD(aggstar);
    READ_BOOL_FIELD(aggvariadic);
    READ_CHAR_FIELD(aggkind);
    READ_UINT_FIELD(agglevelsup);
    READ_ENUM_FIELD(aggsplit, pg_parser_AggSplit);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readGroupingFunc
 */
static pg_parser_GroupingFunc* _readGroupingFunc(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_GroupingFunc);

    READ_NODE_FIELD(args);
    READ_NODE_FIELD(refs);
    READ_NODE_FIELD(cols);
    READ_UINT_FIELD(agglevelsup);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readWindowFunc
 */
static pg_parser_WindowFunc* _readWindowFunc(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_WindowFunc);

    READ_OID_FIELD(winfnoid);
    READ_OID_FIELD(wintype);
    READ_OID_FIELD(wincollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(aggfilter);
    READ_UINT_FIELD(winref);
    READ_BOOL_FIELD(winstar);
    READ_BOOL_FIELD(winagg);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSubscriptingRef
 */
static pg_parser_SubscriptingRef* _readSubscriptingRef(char**  strtok_ptr,
                                                       int16_t dbtype,
                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_SubscriptingRef);

    READ_OID_FIELD(refcontainertype);
    READ_OID_FIELD(refelemtype);
    READ_INT_FIELD(reftypmod);
    READ_OID_FIELD(refcollid);
    READ_NODE_FIELD(refupperindexpr);
    READ_NODE_FIELD(reflowerindexpr);
    READ_NODE_FIELD(refexpr);
    READ_NODE_FIELD(refassgnexpr);

    READ_DONE();
}

/*
 * _readFuncExpr
 */
static pg_parser_FuncExpr* _readFuncExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_FuncExpr);

    READ_OID_FIELD(funcid);
    READ_OID_FIELD(funcresulttype);
    READ_BOOL_FIELD(funcretset);
    READ_BOOL_FIELD(funcvariadic);
    READ_ENUM_FIELD(funcformat, pg_parser_CoercionForm);
    READ_OID_FIELD(funccollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static pg_parser_NamedArgExpr* _readNamedArgExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_NamedArgExpr);

    READ_NODE_FIELD(arg);
    READ_STRING_FIELD(name);
    READ_INT_FIELD(argnumber);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readOpExpr
 */
static pg_parser_OpExpr* _readOpExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_OpExpr);

    READ_OID_FIELD(opno);
    READ_OID_FIELD(opfuncid);
    READ_OID_FIELD(opresulttype);
    READ_BOOL_FIELD(opretset);
    READ_OID_FIELD(opcollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readDistinctExpr
 */
static pg_parser_DistinctExpr* _readDistinctExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_DistinctExpr);

    READ_OID_FIELD(opno);
    READ_OID_FIELD(opfuncid);
    READ_OID_FIELD(opresulttype);
    READ_BOOL_FIELD(opretset);
    READ_OID_FIELD(opcollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNullIfExpr
 */
static pg_parser_NullIfExpr* _readNullIfExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_NullIfExpr);

    READ_OID_FIELD(opno);
    READ_OID_FIELD(opfuncid);
    READ_OID_FIELD(opresulttype);
    READ_BOOL_FIELD(opretset);
    READ_OID_FIELD(opcollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static pg_parser_ScalarArrayOpExpr* _readScalarArrayOpExpr(char**  strtok_ptr,
                                                           int16_t dbtype,
                                                           char*   dbversion)
{
    READ_LOCALS(pg_parser_ScalarArrayOpExpr);

    READ_OID_FIELD(opno);
    READ_OID_FIELD(opfuncid);
    READ_BOOL_FIELD(useOr);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readBoolExpr
 */
static pg_parser_BoolExpr* _readBoolExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_BoolExpr);

    /* do-it-yourself enum representation */
    token = pg_parser_strtok(&length, strtok_ptr); /* skip :boolop */
    token = pg_parser_strtok(&length, strtok_ptr); /* get field value */
    if (strncmp(token, "and", 3) == 0)
    {
        local_node->boolop = AND_EXPR;
    }
    else if (strncmp(token, "or", 2) == 0)
    {
        local_node->boolop = OR_EXPR;
    }
    else if (strncmp(token, "not", 3) == 0)
    {
        local_node->boolop = NOT_EXPR;
    }
    else
    {
        /* todo error handling */
    }

    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSubLink
 */
static pg_parser_SubLink* _readSubLink(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_SubLink);

    READ_ENUM_FIELD(subLinkType, pg_parser_SubLinkType);
    READ_INT_FIELD(subLinkId);
    READ_NODE_FIELD(testexpr);
    READ_NODE_FIELD(operName);
    READ_NODE_FIELD(subselect);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSubPlan is not needed since it doesn't appear in stored rules.
 */

/*
 * _readFieldSelect
 */
static pg_parser_FieldSelect* _readFieldSelect(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_FieldSelect);

    READ_NODE_FIELD(arg);
    READ_INT_FIELD(fieldnum);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);

    READ_DONE();
}

/*
 * _readFieldStore
 */
static pg_parser_FieldStore* _readFieldStore(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_FieldStore);

    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(newvals);
    READ_NODE_FIELD(fieldnums);
    READ_OID_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readRelabelType
 */
static pg_parser_RelabelType* _readRelabelType(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_RelabelType);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(relabelformat, pg_parser_CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static pg_parser_CoerceViaIO* _readCoerceViaIO(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CoerceViaIO);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coerceformat, pg_parser_CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static pg_parser_ArrayCoerceExpr* _readArrayCoerceExpr(char**  strtok_ptr,
                                                       int16_t dbtype,
                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_ArrayCoerceExpr);

    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(elemexpr);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coerceformat, pg_parser_CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static pg_parser_ConvertRowtypeExpr* _readConvertRowtypeExpr(char**  strtok_ptr,
                                                             int16_t dbtype,
                                                             char*   dbversion)
{
    READ_LOCALS(pg_parser_ConvertRowtypeExpr);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_ENUM_FIELD(convertformat, pg_parser_CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCollateExpr
 */
static pg_parser_CollateExpr* _readCollateExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CollateExpr);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(collOid);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseExpr
 */
static pg_parser_CaseExpr* _readCaseExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CaseExpr);

    READ_OID_FIELD(casetype);
    READ_OID_FIELD(casecollid);
    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(defresult);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseWhen
 */
static pg_parser_CaseWhen* _readCaseWhen(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CaseWhen);

    READ_NODE_FIELD(expr);
    READ_NODE_FIELD(result);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static pg_parser_CaseTestExpr* _readCaseTestExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CaseTestExpr);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);

    READ_DONE();
}

/*
 * _readArrayExpr
 */
static pg_parser_ArrayExpr* _readArrayExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_ArrayExpr);

    READ_OID_FIELD(array_typeid);
    READ_OID_FIELD(array_collid);
    READ_OID_FIELD(element_typeid);
    READ_NODE_FIELD(elements);
    READ_BOOL_FIELD(multidims);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readRowExpr
 */
static pg_parser_RowExpr* _readRowExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_RowExpr);

    READ_NODE_FIELD(args);
    READ_OID_FIELD(row_typeid);
    READ_ENUM_FIELD(row_format, pg_parser_CoercionForm);
    READ_NODE_FIELD(colnames);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static pg_parser_RowCompareExpr* _readRowCompareExpr(char**  strtok_ptr,
                                                     int16_t dbtype,
                                                     char*   dbversion)
{
    READ_LOCALS(pg_parser_RowCompareExpr);

    READ_ENUM_FIELD(rctype, pg_parser_RowCompareType);
    READ_NODE_FIELD(opnos);
    READ_NODE_FIELD(opfamilies);
    READ_NODE_FIELD(inputcollids);
    READ_NODE_FIELD(largs);
    READ_NODE_FIELD(rargs);

    READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static pg_parser_CoalesceExpr* _readCoalesceExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CoalesceExpr);

    READ_OID_FIELD(coalescetype);
    READ_OID_FIELD(coalescecollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static pg_parser_MinMaxExpr* _readMinMaxExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_MinMaxExpr);

    READ_OID_FIELD(minmaxtype);
    READ_OID_FIELD(minmaxcollid);
    READ_OID_FIELD(inputcollid);
    READ_ENUM_FIELD(op, pg_parser_MinMaxOp);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSQLValueFunction
 */
static pg_parser_SQLValueFunction* _readSQLValueFunction(char**  strtok_ptr,
                                                         int16_t dbtype,
                                                         char*   dbversion)
{
    READ_LOCALS(pg_parser_SQLValueFunction);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_ENUM_FIELD(op, pg_parser_SQLValueFunctionOp);
    READ_OID_FIELD(type);
    READ_INT_FIELD(typmod);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readXmlExpr
 */
static pg_parser_XmlExpr* _readXmlExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_XmlExpr);

    READ_ENUM_FIELD(op, pg_parser_XmlExprOp);
    READ_STRING_FIELD(name);
    READ_NODE_FIELD(named_args);
    READ_NODE_FIELD(arg_names);
    READ_NODE_FIELD(args);
    READ_ENUM_FIELD(xmloption, pg_parser_XmlOptionType);
    READ_OID_FIELD(type);
    READ_INT_FIELD(typmod);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readNullTest
 */
static pg_parser_NullTest* _readNullTest(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_NullTest);

    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(nulltesttype, pg_parser_NullTestType);
    READ_BOOL_FIELD(argisrow);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readBooleanTest
 */
static pg_parser_BooleanTest* _readBooleanTest(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_BooleanTest);

    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(booltesttype, pg_parser_BoolTestType);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static pg_parser_CoerceToDomain* _readCoerceToDomain(char**  strtok_ptr,
                                                     int16_t dbtype,
                                                     char*   dbversion)
{
    READ_LOCALS(pg_parser_CoerceToDomain);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coercionformat, pg_parser_CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static pg_parser_CoerceToDomainValue* _readCoerceToDomainValue(char**  strtok_ptr,
                                                               int16_t dbtype,
                                                               char*   dbversion)
{
    READ_LOCALS(pg_parser_CoerceToDomainValue);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSetToDefault
 */
static pg_parser_SetToDefault* _readSetToDefault(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_SetToDefault);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static pg_parser_CurrentOfExpr* _readCurrentOfExpr(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_CurrentOfExpr);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_UINT_FIELD(cvarno);
    READ_STRING_FIELD(cursor_name);
    READ_INT_FIELD(cursor_param);

    READ_DONE();
}

/*
 * _readNextValueExpr
 */
static pg_parser_NextValueExpr* _readNextValueExpr(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_NextValueExpr);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_OID_FIELD(seqid);
    READ_OID_FIELD(typeId);

    READ_DONE();
}

/*
 * _readInferenceElem
 */
static pg_parser_InferenceElem* _readInferenceElem(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_InferenceElem);

    READ_NODE_FIELD(expr);
    READ_OID_FIELD(infercollid);
    READ_OID_FIELD(inferopclass);

    READ_DONE();
}

/*
 * _readTargetEntry
 */
static pg_parser_TargetEntry* _readTargetEntry(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_TargetEntry);

    READ_NODE_FIELD(expr);
    READ_INT_FIELD(resno);
    READ_STRING_FIELD(resname);
    READ_UINT_FIELD(ressortgroupref);
    READ_OID_FIELD(resorigtbl);
    READ_INT_FIELD(resorigcol);
    READ_BOOL_FIELD(resjunk);

    READ_DONE();
}

/*
 * _readRangeTblRef
 */
static pg_parser_RangeTblRef* _readRangeTblRef(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_RangeTblRef);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_INT_FIELD(rtindex);

    READ_DONE();
}

/*
 * _readJoinExpr
 */
static pg_parser_JoinExpr* _readJoinExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_JoinExpr);

    READ_ENUM_FIELD(jointype, pg_parser_JoinType);
    READ_BOOL_FIELD(isNatural);
    READ_NODE_FIELD(larg);
    READ_NODE_FIELD(rarg);
    READ_NODE_FIELD(usingClause);
    READ_NODE_FIELD(quals);
    READ_NODE_FIELD(alias);
    READ_INT_FIELD(rtindex);

    READ_DONE();
}

/*
 * _readFromExpr
 */
static pg_parser_FromExpr* _readFromExpr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_FromExpr);

    READ_NODE_FIELD(fromlist);
    READ_NODE_FIELD(quals);

    READ_DONE();
}

/*
 * _readOnConflictExpr
 */
static pg_parser_OnConflictExpr* _readOnConflictExpr(char**  strtok_ptr,
                                                     int16_t dbtype,
                                                     char*   dbversion)
{
    READ_LOCALS(pg_parser_OnConflictExpr);

    READ_ENUM_FIELD(action, pg_parser_OnConflictAction);
    READ_NODE_FIELD(arbiterElems);
    READ_NODE_FIELD(arbiterWhere);
    READ_OID_FIELD(constraint);
    READ_NODE_FIELD(onConflictSet);
    READ_NODE_FIELD(onConflictWhere);
    READ_INT_FIELD(exclRelIndex);
    READ_NODE_FIELD(exclRelTlist);

    READ_DONE();
}

/*
 *    Stuff from parsenodes.h.
 */

/*
 * _readRangeTblEntry
 */
static pg_parser_RangeTblEntry* _readRangeTblEntry(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_RangeTblEntry);

    /* put alias + eref first to make dump more legible */
    READ_NODE_FIELD(alias);
    READ_NODE_FIELD(eref);
    READ_ENUM_FIELD(rtekind, pg_parser_RTEKind);

    switch (local_node->rtekind)
    {
        case RTE_RELATION:
            READ_OID_FIELD(relid);
            READ_CHAR_FIELD(relkind);
            READ_INT_FIELD(rellockmode);
            READ_NODE_FIELD(tablesample);
            break;
        case RTE_SUBQUERY:
            READ_NODE_FIELD(subquery);
            READ_BOOL_FIELD(security_barrier);
            break;
        case RTE_JOIN:
            READ_ENUM_FIELD(jointype, pg_parser_JoinType);
            READ_NODE_FIELD(joinaliasvars);
            break;
        case RTE_FUNCTION:
            READ_NODE_FIELD(functions);
            READ_BOOL_FIELD(funcordinality);
            break;
        case RTE_TABLEFUNC:
            READ_NODE_FIELD(tablefunc);
            /* The RTE must have a copy of the column type info, if any */
            if (local_node->tablefunc)
            {
                pg_parser_TableFunc* tf = local_node->tablefunc;

                local_node->coltypes = tf->coltypes;
                local_node->coltypmods = tf->coltypmods;
                local_node->colcollations = tf->colcollations;
            }
            break;
        case RTE_VALUES:
            READ_NODE_FIELD(values_lists);
            READ_NODE_FIELD(coltypes);
            READ_NODE_FIELD(coltypmods);
            READ_NODE_FIELD(colcollations);
            break;
        case RTE_CTE:
            READ_STRING_FIELD(ctename);
            READ_UINT_FIELD(ctelevelsup);
            READ_BOOL_FIELD(self_reference);
            READ_NODE_FIELD(coltypes);
            READ_NODE_FIELD(coltypmods);
            READ_NODE_FIELD(colcollations);
            break;
        case RTE_NAMEDTUPLESTORE:
            READ_STRING_FIELD(enrname);
            READ_FLOAT_FIELD(enrtuples);
            READ_OID_FIELD(relid);
            READ_NODE_FIELD(coltypes);
            READ_NODE_FIELD(coltypmods);
            READ_NODE_FIELD(colcollations);
            break;
        case RTE_RESULT:
            /* no extra fields */
            break;
        default:
            /* todo error handling */
            break;
    }

    READ_BOOL_FIELD(lateral);
    READ_BOOL_FIELD(inh);
    READ_BOOL_FIELD(inFromCl);
    READ_UINT_FIELD(requiredPerms);
    READ_OID_FIELD(checkAsUser);
    READ_BITMAPSET_FIELD(selectedCols);
    READ_BITMAPSET_FIELD(insertedCols);
    READ_BITMAPSET_FIELD(updatedCols);
    READ_BITMAPSET_FIELD(extraUpdatedCols);
    READ_NODE_FIELD(securityQuals);

    READ_DONE();
}

/*
 * _readRangeTblFunction
 */
static pg_parser_RangeTblFunction* _readRangeTblFunction(char**  strtok_ptr,
                                                         int16_t dbtype,
                                                         char*   dbversion)
{
    READ_LOCALS(pg_parser_RangeTblFunction);

    READ_NODE_FIELD(funcexpr);
    READ_INT_FIELD(funccolcount);
    READ_NODE_FIELD(funccolnames);
    READ_NODE_FIELD(funccoltypes);
    READ_NODE_FIELD(funccoltypmods);
    READ_NODE_FIELD(funccolcollations);
    READ_BITMAPSET_FIELD(funcparams);

    READ_DONE();
}

/*
 * _readTableSampleClause
 */
static pg_parser_TableSampleClause* _readTableSampleClause(char**  strtok_ptr,
                                                           int16_t dbtype,
                                                           char*   dbversion)
{
    READ_LOCALS(pg_parser_TableSampleClause);

    READ_OID_FIELD(tsmhandler);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(repeatable);

    READ_DONE();
}

/*
 * _readDefElem
 */
static pg_parser_DefElem* _readDefElem(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_DefElem);

    READ_STRING_FIELD(defnamespace);
    READ_STRING_FIELD(defname);
    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(defaction, pg_parser_DefElemAction);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 *    Stuff from plannodes.h.
 */

/*
 * _readPlannedStmt
 */
static pg_parser_PlannedStmt* _readPlannedStmt(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_PlannedStmt);

    READ_ENUM_FIELD(commandType, pg_parser_CmdType);
    READ_UINT64_FIELD(queryId);
    READ_BOOL_FIELD(hasReturning);
    READ_BOOL_FIELD(hasModifyingCTE);
    READ_BOOL_FIELD(canSetTag);
    READ_BOOL_FIELD(transientPlan);
    READ_BOOL_FIELD(dependsOnRole);
    READ_BOOL_FIELD(parallelModeNeeded);
    READ_INT_FIELD(jitFlags);
    READ_NODE_FIELD(planTree);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(resultRelations);
    READ_NODE_FIELD(rootResultRelations);
    READ_NODE_FIELD(subplans);
    READ_BITMAPSET_FIELD(rewindPlanIDs);
    READ_NODE_FIELD(rowMarks);
    READ_NODE_FIELD(relationOids);
    READ_NODE_FIELD(invalItems);
    READ_NODE_FIELD(paramExecTypes);
    READ_NODE_FIELD(utilityStmt);
    READ_LOCATION_FIELD(stmt_location);
    READ_LOCATION_FIELD(stmt_len);

    READ_DONE();
}

/*
 * ReadCommonPlan
 *    Assign the basic stuff of all nodes that inherit from Plan
 */
static void ReadCommonPlan(pg_parser_Plan* local_node,
                           char**          strtok_ptr,
                           int16_t         dbtype,
                           char*           dbversion)
{
    READ_TEMP_LOCALS();

    READ_FLOAT_FIELD(startup_cost);
    READ_FLOAT_FIELD(total_cost);
    READ_FLOAT_FIELD(plan_rows);
    READ_INT_FIELD(plan_width);
    READ_BOOL_FIELD(parallel_aware);
    READ_BOOL_FIELD(parallel_safe);
    READ_INT_FIELD(plan_node_id);
    READ_NODE_FIELD(targetlist);
    READ_NODE_FIELD(qual);
    READ_NODE_FIELD(lefttree);
    READ_NODE_FIELD(righttree);
    READ_NODE_FIELD(initPlan);
    READ_BITMAPSET_FIELD(extParam);
    READ_BITMAPSET_FIELD(allParam);
}

/*
 * _readPlan
 */
static pg_parser_Plan* _readPlan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS_NO_FIELDS(pg_parser_Plan);

    ReadCommonPlan(local_node, strtok_ptr, dbtype, dbversion);

    READ_DONE();
}

/*
 * _readResult
 */
static pg_parser_Result* _readResult(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Result);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(resconstantqual);

    READ_DONE();
}

/*
 * _readProjectSet
 */
static pg_parser_ProjectSet* _readProjectSet(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS_NO_FIELDS(pg_parser_ProjectSet);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_DONE();
}

/*
 * _readModifyTable
 */
static pg_parser_ModifyTable* _readModifyTable(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_ModifyTable);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_ENUM_FIELD(operation, pg_parser_CmdType);
    READ_BOOL_FIELD(canSetTag);
    READ_UINT_FIELD(nominalRelation);
    READ_UINT_FIELD(rootRelation);
    READ_BOOL_FIELD(partColsUpdated);
    READ_NODE_FIELD(resultRelations);
    READ_INT_FIELD(resultRelIndex);
    READ_INT_FIELD(rootResultRelIndex);
    READ_NODE_FIELD(plans);
    READ_NODE_FIELD(withCheckOptionLists);
    READ_NODE_FIELD(returningLists);
    READ_NODE_FIELD(fdwPrivLists);
    READ_BITMAPSET_FIELD(fdwDirectModifyPlans);
    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);
    READ_ENUM_FIELD(onConflictAction, pg_parser_OnConflictAction);
    READ_NODE_FIELD(arbiterIndexes);
    READ_NODE_FIELD(onConflictSet);
    READ_NODE_FIELD(onConflictWhere);
    READ_UINT_FIELD(exclRelRTI);
    READ_NODE_FIELD(exclRelTlist);

    READ_DONE();
}

/*
 * _readAppend
 */
static pg_parser_Append* _readAppend(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Append);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(appendplans);
    READ_INT_FIELD(first_partial_plan);
    READ_NODE_FIELD(part_prune_info);

    READ_DONE();
}

/*
 * _readMergeAppend
 */
static pg_parser_MergeAppend* _readMergeAppend(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_MergeAppend);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(mergeplans);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
    READ_OID_ARRAY(sortOperators, local_node->numCols);
    READ_OID_ARRAY(collations, local_node->numCols);
    READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
    READ_NODE_FIELD(part_prune_info);

    READ_DONE();
}

/*
 * _readRecursiveUnion
 */
static pg_parser_RecursiveUnion* _readRecursiveUnion(char**  strtok_ptr,
                                                     int16_t dbtype,
                                                     char*   dbversion)
{
    READ_LOCALS(pg_parser_RecursiveUnion);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(wtParam);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
    READ_OID_ARRAY(dupOperators, local_node->numCols);
    READ_OID_ARRAY(dupCollations, local_node->numCols);
    READ_LONG_FIELD(numGroups);

    READ_DONE();
}

/*
 * _readBitmapAnd
 */
static pg_parser_BitmapAnd* _readBitmapAnd(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_BitmapAnd);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}

/*
 * _readBitmapOr
 */
static pg_parser_BitmapOr* _readBitmapOr(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_BitmapOr);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_BOOL_FIELD(isshared);
    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}

/*
 * ReadCommonScan
 *    Assign the basic stuff of all nodes that inherit from Scan
 */
static void ReadCommonScan(pg_parser_Scan* local_node,
                           char**          strtok_ptr,
                           int16_t         dbtype,
                           char*           dbversion)
{
    READ_TEMP_LOCALS();

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_UINT_FIELD(scanrelid);
}

/*
 * _readScan
 */
static pg_parser_Scan* _readScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS_NO_FIELDS(pg_parser_Scan);

    ReadCommonScan(local_node, strtok_ptr, dbtype, dbversion);

    READ_DONE();
}

/*
 * _readSeqScan
 */
static pg_parser_SeqScan* _readSeqScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS_NO_FIELDS(pg_parser_SeqScan);

    ReadCommonScan(local_node, strtok_ptr, dbtype, dbversion);

    READ_DONE();
}

/*
 * _readSampleScan
 */
static pg_parser_SampleScan* _readSampleScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_SampleScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(tablesample);

    READ_DONE();
}

/*
 * _readIndexScan
 */
static pg_parser_IndexScan* _readIndexScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_IndexScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_OID_FIELD(indexid);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indexorderbyorig);
    READ_NODE_FIELD(indexorderbyops);
    READ_ENUM_FIELD(indexorderdir, pg_parser_ScanDirection);

    READ_DONE();
}

/*
 * _readIndexOnlyScan
 */
static pg_parser_IndexOnlyScan* _readIndexOnlyScan(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_IndexOnlyScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_OID_FIELD(indexid);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indextlist);
    READ_ENUM_FIELD(indexorderdir, pg_parser_ScanDirection);

    READ_DONE();
}

/*
 * _readBitmapIndexScan
 */
static pg_parser_BitmapIndexScan* _readBitmapIndexScan(char**  strtok_ptr,
                                                       int16_t dbtype,
                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_BitmapIndexScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_OID_FIELD(indexid);
    READ_BOOL_FIELD(isshared);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);

    READ_DONE();
}

/*
 * _readBitmapHeapScan
 */
static pg_parser_BitmapHeapScan* _readBitmapHeapScan(char**  strtok_ptr,
                                                     int16_t dbtype,
                                                     char*   dbversion)
{
    READ_LOCALS(pg_parser_BitmapHeapScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(bitmapqualorig);

    READ_DONE();
}

/*
 * _readTidScan
 */
static pg_parser_TidScan* _readTidScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_TidScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(tidquals);

    READ_DONE();
}

/*
 * _readSubqueryScan
 */
static pg_parser_SubqueryScan* _readSubqueryScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_SubqueryScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(subplan);

    READ_DONE();
}

/*
 * _readFunctionScan
 */
static pg_parser_FunctionScan* _readFunctionScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_FunctionScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(functions);
    READ_BOOL_FIELD(funcordinality);

    READ_DONE();
}

/*
 * _readValuesScan
 */
static pg_parser_ValuesScan* _readValuesScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_ValuesScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(values_lists);

    READ_DONE();
}

/*
 * _readTableFuncScan
 */
static pg_parser_TableFuncScan* _readTableFuncScan(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_TableFuncScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(tablefunc);

    READ_DONE();
}

/*
 * _readCteScan
 */
static pg_parser_CteScan* _readCteScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_CteScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(ctePlanId);
    READ_INT_FIELD(cteParam);

    READ_DONE();
}

/*
 * _readNamedTuplestoreScan
 */
static pg_parser_NamedTuplestoreScan* _readNamedTuplestoreScan(char**  strtok_ptr,
                                                               int16_t dbtype,
                                                               char*   dbversion)
{
    READ_LOCALS(pg_parser_NamedTuplestoreScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_STRING_FIELD(enrname);

    READ_DONE();
}

/*
 * _readWorkTableScan
 */
static pg_parser_WorkTableScan* _readWorkTableScan(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_WorkTableScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(wtParam);

    READ_DONE();
}

/*
 * _readForeignScan
 */
static pg_parser_ForeignScan* _readForeignScan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_ForeignScan);

    ReadCommonScan(&local_node->scan, strtok_ptr, dbtype, dbversion);

    READ_ENUM_FIELD(operation, pg_parser_CmdType);
    READ_OID_FIELD(fs_server);
    READ_NODE_FIELD(fdw_exprs);
    READ_NODE_FIELD(fdw_private);
    READ_NODE_FIELD(fdw_scan_tlist);
    READ_NODE_FIELD(fdw_recheck_quals);
    READ_BITMAPSET_FIELD(fs_relids);
    READ_BOOL_FIELD(fsSystemCol);

    READ_DONE();
}

/*
 * ReadCommonJoin
 *    Assign the basic stuff of all nodes that inherit from Join
 */
static void ReadCommonJoin(pg_parser_Join* local_node,
                           char**          strtok_ptr,
                           int16_t         dbtype,
                           char*           dbversion)
{
    READ_TEMP_LOCALS();

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_ENUM_FIELD(jointype, pg_parser_JoinType);
    READ_BOOL_FIELD(inner_unique);
    READ_NODE_FIELD(joinqual);
}

/*
 * _readJoin
 */
static pg_parser_Join* _readJoin(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS_NO_FIELDS(pg_parser_Join);

    ReadCommonJoin(local_node, strtok_ptr, dbtype, dbversion);

    READ_DONE();
}

/*
 * _readNestLoop
 */
static pg_parser_NestLoop* _readNestLoop(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_NestLoop);

    ReadCommonJoin(&local_node->join, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(nestParams);

    READ_DONE();
}

/*
 * _readMergeJoin
 */
static pg_parser_MergeJoin* _readMergeJoin(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    int32_t numCols;

    READ_LOCALS(pg_parser_MergeJoin);

    ReadCommonJoin(&local_node->join, strtok_ptr, dbtype, dbversion);

    READ_BOOL_FIELD(skip_mark_restore);
    READ_NODE_FIELD(mergeclauses);

    numCols = pg_parser_list_length(local_node->mergeclauses);

    READ_OID_ARRAY(mergeFamilies, numCols);
    READ_OID_ARRAY(mergeCollations, numCols);
    READ_INT_ARRAY(mergeStrategies, numCols);
    READ_BOOL_ARRAY(mergeNullsFirst, numCols);

    READ_DONE();
}

/*
 * _readHashJoin
 */
static pg_parser_HashJoin* _readHashJoin(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_HashJoin);

    ReadCommonJoin(&local_node->join, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(hashclauses);
    READ_NODE_FIELD(hashoperators);
    READ_NODE_FIELD(hashcollations);
    READ_NODE_FIELD(hashkeys);

    READ_DONE();
}

/*
 * _readMaterial
 */
static pg_parser_Material* _readMaterial(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS_NO_FIELDS(pg_parser_Material);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_DONE();
}

/*
 * _readSort
 */
static pg_parser_Sort* _readSort(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Sort);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
    READ_OID_ARRAY(sortOperators, local_node->numCols);
    READ_OID_ARRAY(collations, local_node->numCols);
    READ_BOOL_ARRAY(nullsFirst, local_node->numCols);

    READ_DONE();
}

/*
 * _readGroup
 */
static pg_parser_Group* _readGroup(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Group);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);
    READ_OID_ARRAY(grpOperators, local_node->numCols);
    READ_OID_ARRAY(grpCollations, local_node->numCols);

    READ_DONE();
}

/*
 * _readAgg
 */
static pg_parser_Agg* _readAgg(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Agg);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_ENUM_FIELD(aggstrategy, pg_parser_AggStrategy);
    READ_ENUM_FIELD(aggsplit, pg_parser_AggSplit);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);
    READ_OID_ARRAY(grpOperators, local_node->numCols);
    READ_OID_ARRAY(grpCollations, local_node->numCols);
    READ_LONG_FIELD(numGroups);
    READ_BITMAPSET_FIELD(aggParams);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(chain);

    READ_DONE();
}

/*
 * _readWindowAgg
 */
static pg_parser_WindowAgg* _readWindowAgg(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_WindowAgg);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_UINT_FIELD(winref);
    READ_INT_FIELD(partNumCols);
    READ_ATTRNUMBER_ARRAY(partColIdx, local_node->partNumCols);
    READ_OID_ARRAY(partOperators, local_node->partNumCols);
    READ_OID_ARRAY(partCollations, local_node->partNumCols);
    READ_INT_FIELD(ordNumCols);
    READ_ATTRNUMBER_ARRAY(ordColIdx, local_node->ordNumCols);
    READ_OID_ARRAY(ordOperators, local_node->ordNumCols);
    READ_OID_ARRAY(ordCollations, local_node->ordNumCols);
    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);
    READ_OID_FIELD(startInRangeFunc);
    READ_OID_FIELD(endInRangeFunc);
    READ_OID_FIELD(inRangeColl);
    READ_BOOL_FIELD(inRangeAsc);
    READ_BOOL_FIELD(inRangeNullsFirst);

    READ_DONE();
}

/*
 * _readUnique
 */
static pg_parser_Unique* _readUnique(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Unique);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->numCols);
    READ_OID_ARRAY(uniqOperators, local_node->numCols);
    READ_OID_ARRAY(uniqCollations, local_node->numCols);

    READ_DONE();
}

/*
 * _readGather
 */
static pg_parser_Gather* _readGather(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Gather);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(num_workers);
    READ_INT_FIELD(rescan_param);
    READ_BOOL_FIELD(single_copy);
    READ_BOOL_FIELD(invisible);
    READ_BITMAPSET_FIELD(initParam);

    READ_DONE();
}

/*
 * _readGatherMerge
 */
static pg_parser_GatherMerge* _readGatherMerge(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_GatherMerge);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_INT_FIELD(num_workers);
    READ_INT_FIELD(rescan_param);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
    READ_OID_ARRAY(sortOperators, local_node->numCols);
    READ_OID_ARRAY(collations, local_node->numCols);
    READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
    READ_BITMAPSET_FIELD(initParam);

    READ_DONE();
}

/*
 * _readHash
 */
static pg_parser_Hash* _readHash(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Hash);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(hashkeys);
    READ_OID_FIELD(skewTable);
    READ_INT_FIELD(skewColumn);
    READ_BOOL_FIELD(skewInherit);
    READ_FLOAT_FIELD(rows_total);

    READ_DONE();
}

/*
 * _readSetOp
 */
static pg_parser_SetOp* _readSetOp(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_SetOp);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_ENUM_FIELD(cmd, pg_parser_SetOpCmd);
    READ_ENUM_FIELD(strategy, pg_parser_SetOpStrategy);
    READ_INT_FIELD(numCols);
    READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
    READ_OID_ARRAY(dupOperators, local_node->numCols);
    READ_OID_ARRAY(dupCollations, local_node->numCols);
    READ_INT_FIELD(flagColIdx);
    READ_INT_FIELD(firstFlag);
    READ_LONG_FIELD(numGroups);

    READ_DONE();
}

/*
 * _readLockRows
 */
static pg_parser_LockRows* _readLockRows(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_LockRows);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);

    READ_DONE();
}

/*
 * _readLimit
 */
static pg_parser_Limit* _readLimit(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_Limit);

    ReadCommonPlan(&local_node->plan, strtok_ptr, dbtype, dbversion);

    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);

    READ_DONE();
}

/*
 * _readNestLoopParam
 */
static pg_parser_NestLoopParam* _readNestLoopParam(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_NestLoopParam);

    READ_INT_FIELD(paramno);
    READ_NODE_FIELD(paramval);

    READ_DONE();
}

/*
 * _readPlanRowMark
 */
static pg_parser_PlanRowMark* _readPlanRowMark(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_PlanRowMark);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_UINT_FIELD(rti);
    READ_UINT_FIELD(prti);
    READ_UINT_FIELD(rowmarkId);
    READ_ENUM_FIELD(markType, pg_parser_RowMarkType);
    READ_INT_FIELD(allMarkTypes);
    READ_ENUM_FIELD(strength, pg_parser_LockClauseStrength);
    READ_ENUM_FIELD(waitPolicy, pg_parser_LockWaitPolicy);
    READ_BOOL_FIELD(isParent);

    READ_DONE();
}

static pg_parser_PartitionPruneInfo* _readPartitionPruneInfo(char**  strtok_ptr,
                                                             int16_t dbtype,
                                                             char*   dbversion)
{
    READ_LOCALS(pg_parser_PartitionPruneInfo);

    READ_NODE_FIELD(prune_infos);
    READ_BITMAPSET_FIELD(other_subplans);

    READ_DONE();
}

static pg_parser_PartitionedRelPruneInfo* _readPartitionedRelPruneInfo(char**  strtok_ptr,
                                                                       int16_t dbtype,
                                                                       char*   dbversion)
{
    READ_LOCALS(pg_parser_PartitionedRelPruneInfo);

    READ_UINT_FIELD(rtindex);
    READ_BITMAPSET_FIELD(present_parts);
    READ_INT_FIELD(nparts);
    READ_INT_ARRAY(subplan_map, local_node->nparts);
    READ_INT_ARRAY(subpart_map, local_node->nparts);
    READ_OID_ARRAY(relid_map, local_node->nparts);
    READ_NODE_FIELD(initial_pruning_steps);
    READ_NODE_FIELD(exec_pruning_steps);
    READ_BITMAPSET_FIELD(execparamids);

    READ_DONE();
}

static pg_parser_PartitionPruneStepOp* _readPartitionPruneStepOp(char**  strtok_ptr,
                                                                 int16_t dbtype,
                                                                 char*   dbversion)
{
    READ_LOCALS(pg_parser_PartitionPruneStepOp);

    READ_INT_FIELD(step.step_id);
    READ_INT_FIELD(opstrategy);
    READ_NODE_FIELD(exprs);
    READ_NODE_FIELD(cmpfns);
    READ_BITMAPSET_FIELD(nullkeys);

    READ_DONE();
}

static pg_parser_PartitionPruneStepCombine* _readPartitionPruneStepCombine(char**  strtok_ptr,
                                                                           int16_t dbtype,
                                                                           char*   dbversion)
{
    READ_LOCALS(pg_parser_PartitionPruneStepCombine);

    READ_INT_FIELD(step.step_id);
    READ_ENUM_FIELD(combineOp, pg_parser_PartitionPruneCombineOp);
    READ_NODE_FIELD(source_stepids);

    READ_DONE();
}

/*
 * _readPlanInvalItem
 */
static pg_parser_PlanInvalItem* _readPlanInvalItem(char**  strtok_ptr,
                                                   int16_t dbtype,
                                                   char*   dbversion)
{
    READ_LOCALS(pg_parser_PlanInvalItem);

    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    READ_INT_FIELD(cacheId);
    READ_UINT_FIELD(hashValue);

    READ_DONE();
}

/*
 * _readSubPlan
 */
static pg_parser_SubPlan* _readSubPlan(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    READ_LOCALS(pg_parser_SubPlan);

    READ_ENUM_FIELD(subLinkType, pg_parser_SubLinkType);
    READ_NODE_FIELD(testexpr);
    READ_NODE_FIELD(paramIds);
    READ_INT_FIELD(plan_id);
    READ_STRING_FIELD(plan_name);
    READ_OID_FIELD(firstColType);
    READ_INT_FIELD(firstColTypmod);
    READ_OID_FIELD(firstColCollation);
    READ_BOOL_FIELD(useHashTable);
    READ_BOOL_FIELD(unknownEqFalse);
    READ_BOOL_FIELD(parallel_safe);
    READ_NODE_FIELD(setParam);
    READ_NODE_FIELD(parParam);
    READ_NODE_FIELD(args);
    READ_FLOAT_FIELD(startup_cost);
    READ_FLOAT_FIELD(per_call_cost);

    READ_DONE();
}

/*
 * _readAlternativeSubPlan
 */
static pg_parser_AlternativeSubPlan* _readAlternativeSubPlan(char**  strtok_ptr,
                                                             int16_t dbtype,
                                                             char*   dbversion)
{
    READ_LOCALS(pg_parser_AlternativeSubPlan);

    READ_NODE_FIELD(subplans);

    READ_DONE();
}

/*
 * _readPartitionBoundSpec
 */
static pg_parser_PartitionBoundSpec* _readPartitionBoundSpec(char**  strtok_ptr,
                                                             int16_t dbtype,
                                                             char*   dbversion)
{
    READ_LOCALS(pg_parser_PartitionBoundSpec);

    READ_CHAR_FIELD(strategy);
    READ_BOOL_FIELD(is_default);
    READ_INT_FIELD(modulus);
    READ_INT_FIELD(remainder);
    READ_NODE_FIELD(listdatums);
    READ_NODE_FIELD(lowerdatums);
    READ_NODE_FIELD(upperdatums);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readPartitionRangeDatum
 */
static pg_parser_PartitionRangeDatum* _readPartitionRangeDatum(char**  strtok_ptr,
                                                               int16_t dbtype,
                                                               char*   dbversion)
{
    READ_LOCALS(pg_parser_PartitionRangeDatum);

    READ_ENUM_FIELD(kind, pg_parser_PartitionRangeDatumKind);
    READ_NODE_FIELD(value);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_parser_strtok().
 */
static pg_parser_Node* parseNodeString(char** strtok_ptr, int16_t dbtype, char* dbversion)
{
    void* return_value;

    READ_TEMP_LOCALS();

    token = pg_parser_strtok(&length, strtok_ptr);

#define MATCH(tokname, namelen) (length == namelen && memcmp(token, tokname, namelen) == 0)

    if (MATCH("QUERY", 5))
    {
        return_value = _readQuery(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("WITHCHECKOPTION", 15))
    {
        return_value = _readWithCheckOption(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SORTGROUPCLAUSE", 15))
    {
        return_value = _readSortGroupClause(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("GROUPINGSET", 11))
    {
        return_value = _readGroupingSet(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("WINDOWCLAUSE", 12))
    {
        return_value = _readWindowClause(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ROWMARKCLAUSE", 13))
    {
        return_value = _readRowMarkClause(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("COMMONTABLEEXPR", 15))
    {
        return_value = _readCommonTableExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SETOPERATIONSTMT", 16))
    {
        return_value = _readSetOperationStmt(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ALIAS", 5))
    {
        return_value = _readAlias(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RANGEVAR", 8))
    {
        return_value = _readRangeVar(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("INTOCLAUSE", 10))
    {
        return_value = _readIntoClause(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("TABLEFUNC", 9))
    {
        return_value = _readTableFunc(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("VAR", 3))
    {
        return_value = _readVar(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CONST", 5))
    {
        return_value = _readConst(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PARAM", 5))
    {
        return_value = _readParam(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("AGGREF", 6))
    {
        return_value = _readAggref(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("GROUPINGFUNC", 12))
    {
        return_value = _readGroupingFunc(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("WINDOWFUNC", 10))
    {
        return_value = _readWindowFunc(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SUBSCRIPTINGREF", 15))
    {
        return_value = _readSubscriptingRef(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("FUNCEXPR", 8))
    {
        return_value = _readFuncExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NAMEDARGEXPR", 12))
    {
        return_value = _readNamedArgExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("OPEXPR", 6))
    {
        return_value = _readOpExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("DISTINCTEXPR", 12))
    {
        return_value = _readDistinctExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NULLIFEXPR", 10))
    {
        return_value = _readNullIfExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SCALARARRAYOPEXPR", 17))
    {
        return_value = _readScalarArrayOpExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("BOOLEXPR", 8))
    {
        return_value = _readBoolExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SUBLINK", 7))
    {
        return_value = _readSubLink(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("FIELDSELECT", 11))
    {
        return_value = _readFieldSelect(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("FIELDSTORE", 10))
    {
        return_value = _readFieldStore(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RELABELTYPE", 11))
    {
        return_value = _readRelabelType(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("COERCEVIAIO", 11))
    {
        return_value = _readCoerceViaIO(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ARRAYCOERCEEXPR", 15))
    {
        return_value = _readArrayCoerceExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CONVERTROWTYPEEXPR", 18))
    {
        return_value = _readConvertRowtypeExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("COLLATE", 7))
    {
        return_value = _readCollateExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CASE", 4))
    {
        return_value = _readCaseExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("WHEN", 4))
    {
        return_value = _readCaseWhen(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CASETESTEXPR", 12))
    {
        return_value = _readCaseTestExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ARRAY", 5))
    {
        return_value = _readArrayExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ROW", 3))
    {
        return_value = _readRowExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ROWCOMPARE", 10))
    {
        return_value = _readRowCompareExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("COALESCE", 8))
    {
        return_value = _readCoalesceExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("MINMAX", 6))
    {
        return_value = _readMinMaxExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SQLVALUEFUNCTION", 16))
    {
        return_value = _readSQLValueFunction(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("XMLEXPR", 7))
    {
        return_value = _readXmlExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NULLTEST", 8))
    {
        return_value = _readNullTest(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("BOOLEANTEST", 11))
    {
        return_value = _readBooleanTest(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("COERCETODOMAIN", 14))
    {
        return_value = _readCoerceToDomain(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("COERCETODOMAINVALUE", 19))
    {
        return_value = _readCoerceToDomainValue(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SETTODEFAULT", 12))
    {
        return_value = _readSetToDefault(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CURRENTOFEXPR", 13))
    {
        return_value = _readCurrentOfExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NEXTVALUEEXPR", 13))
    {
        return_value = _readNextValueExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("INFERENCEELEM", 13))
    {
        return_value = _readInferenceElem(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("TARGETENTRY", 11))
    {
        return_value = _readTargetEntry(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RANGETBLREF", 11))
    {
        return_value = _readRangeTblRef(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("JOINEXPR", 8))
    {
        return_value = _readJoinExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("FROMEXPR", 8))
    {
        return_value = _readFromExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ONCONFLICTEXPR", 14))
    {
        return_value = _readOnConflictExpr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RTE", 3))
    {
        return_value = _readRangeTblEntry(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RANGETBLFUNCTION", 16))
    {
        return_value = _readRangeTblFunction(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("TABLESAMPLECLAUSE", 17))
    {
        return_value = _readTableSampleClause(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NOTIFY", 6))
    {
        return_value = _readNotifyStmt(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("DEFELEM", 7))
    {
        return_value = _readDefElem(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("DECLARECURSOR", 13))
    {
        return_value = _readDeclareCursorStmt(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PLANNEDSTMT", 11))
    {
        return_value = _readPlannedStmt(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PLAN", 4))
    {
        return_value = _readPlan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RESULT", 6))
    {
        return_value = _readResult(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PROJECTSET", 10))
    {
        return_value = _readProjectSet(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("MODIFYTABLE", 11))
    {
        return_value = _readModifyTable(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("APPEND", 6))
    {
        return_value = _readAppend(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("MERGEAPPEND", 11))
    {
        return_value = _readMergeAppend(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("RECURSIVEUNION", 14))
    {
        return_value = _readRecursiveUnion(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("BITMAPAND", 9))
    {
        return_value = _readBitmapAnd(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("BITMAPOR", 8))
    {
        return_value = _readBitmapOr(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SCAN", 4))
    {
        return_value = _readScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SEQSCAN", 7))
    {
        return_value = _readSeqScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SAMPLESCAN", 10))
    {
        return_value = _readSampleScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("INDEXSCAN", 9))
    {
        return_value = _readIndexScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("INDEXONLYSCAN", 13))
    {
        return_value = _readIndexOnlyScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("BITMAPINDEXSCAN", 15))
    {
        return_value = _readBitmapIndexScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("BITMAPHEAPSCAN", 14))
    {
        return_value = _readBitmapHeapScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("TIDSCAN", 7))
    {
        return_value = _readTidScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SUBQUERYSCAN", 12))
    {
        return_value = _readSubqueryScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("FUNCTIONSCAN", 12))
    {
        return_value = _readFunctionScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("VALUESSCAN", 10))
    {
        return_value = _readValuesScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("TABLEFUNCSCAN", 13))
    {
        return_value = _readTableFuncScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CTESCAN", 7))
    {
        return_value = _readCteScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NAMEDTUPLESTORESCAN", 19))
    {
        return_value = _readNamedTuplestoreScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("WORKTABLESCAN", 13))
    {
        return_value = _readWorkTableScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("FOREIGNSCAN", 11))
    {
        return_value = _readForeignScan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("CUSTOMSCAN", 10))
    {
        /* todo error handling */
        return_value = NULL;
    }
    else if (MATCH("JOIN", 4))
    {
        return_value = _readJoin(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NESTLOOP", 8))
    {
        return_value = _readNestLoop(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("MERGEJOIN", 9))
    {
        return_value = _readMergeJoin(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("HASHJOIN", 8))
    {
        return_value = _readHashJoin(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("MATERIAL", 8))
    {
        return_value = _readMaterial(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SORT", 4))
    {
        return_value = _readSort(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("GROUP", 5))
    {
        return_value = _readGroup(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("AGG", 3))
    {
        return_value = _readAgg(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("WINDOWAGG", 9))
    {
        return_value = _readWindowAgg(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("UNIQUE", 6))
    {
        return_value = _readUnique(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("GATHER", 6))
    {
        return_value = _readGather(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("GATHERMERGE", 11))
    {
        return_value = _readGatherMerge(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("HASH", 4))
    {
        return_value = _readHash(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SETOP", 5))
    {
        return_value = _readSetOp(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("LOCKROWS", 8))
    {
        return_value = _readLockRows(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("LIMIT", 5))
    {
        return_value = _readLimit(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("NESTLOOPPARAM", 13))
    {
        return_value = _readNestLoopParam(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PLANROWMARK", 11))
    {
        return_value = _readPlanRowMark(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PARTITIONPRUNEINFO", 18))
    {
        return_value = _readPartitionPruneInfo(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PARTITIONEDRELPRUNEINFO", 23))
    {
        return_value = _readPartitionedRelPruneInfo(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PARTITIONPRUNESTEPOP", 20))
    {
        return_value = _readPartitionPruneStepOp(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PARTITIONPRUNESTEPCOMBINE", 25))
    {
        return_value = _readPartitionPruneStepCombine(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PLANINVALITEM", 13))
    {
        return_value = _readPlanInvalItem(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("SUBPLAN", 7))
    {
        return_value = _readSubPlan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("ALTERNATIVESUBPLAN", 18))
    {
        return_value = _readAlternativeSubPlan(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("EXTENSIBLENODE", 14))
    {
        /* todo error handling */
        return_value = NULL;
    }
    else if (MATCH("PARTITIONBOUNDSPEC", 18))
    {
        return_value = _readPartitionBoundSpec(strtok_ptr, dbtype, dbversion);
    }
    else if (MATCH("PARTITIONRANGEDATUM", 19))
    {
        return_value = _readPartitionRangeDatum(strtok_ptr, dbtype, dbversion);
    }
    else
    {
        /* todo error handling */
        return_value = NULL; /* keep compiler quiet */
    }

    return (pg_parser_Node*)return_value;
}

/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
static pg_parser_Datum readDatum(bool typbyval, char** strtok_ptr, bool* need_free)
{
    size_t          length, i;
    int32_t         tokenLength;
    const char*     token;
    pg_parser_Datum res;
    char*           s;

    /*
     * read the actual length of the value
     */
    token = pg_parser_strtok(&tokenLength, strtok_ptr);
    length = atoui(token);

    token = pg_parser_strtok(&tokenLength, strtok_ptr); /* read the '[' */
    if (token == NULL || token[0] != '[')
    {
        /* todo error handling */
        return (pg_parser_Datum)0;
    }

    if (typbyval)
    {
        if (length > (size_t)sizeof(pg_parser_Datum))
        {
            /* todo error handling */
            return (pg_parser_Datum)0;
        }
        res = (pg_parser_Datum)0;
        s = (char*)(&res);
        for (i = 0; i < (size_t)sizeof(pg_parser_Datum); i++)
        {
            token = pg_parser_strtok(&tokenLength, strtok_ptr);
            s[i] = (char)atoi(token);
        }
    }
    else if (length <= 0)
    {
        res = (pg_parser_Datum)NULL;
    }
    else
    {
        pg_parser_mcxt_malloc(NODE_MCXT, (void**)&s, length);
        *need_free = true;
        for (i = 0; i < length; i++)
        {
            token = pg_parser_strtok(&tokenLength, strtok_ptr);
            s[i] = (char)atoi(token);
        }
        res = pg_parser_PointerGetDatum(s);
    }

    token = pg_parser_strtok(&tokenLength, strtok_ptr); /* read the ']' */
    if (token == NULL || token[0] != ']')
    {
        /* todo error handling */
        return (pg_parser_Datum)0;
    }

    return res;
}

/*
 * readAttrNumberCols
 */
static pg_parser_AttrNumber* readAttrNumberCols(int32_t numCols, char** strtok_ptr)
{
    int32_t               tokenLength, i;
    const char*           token;
    pg_parser_AttrNumber* attr_vals;

    if (numCols <= 0)
    {
        return NULL;
    }
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&attr_vals, numCols * sizeof(pg_parser_AttrNumber));
    for (i = 0; i < numCols; i++)
    {
        token = pg_parser_strtok(&tokenLength, strtok_ptr);
        attr_vals[i] = atoi(token);
    }

    return attr_vals;
}

/*
 * readOidCols
 */
static uint32_t* readOidCols(int32_t numCols, char** strtok_ptr)
{
    int32_t     tokenLength, i;
    const char* token;
    uint32_t*   oid_vals;

    if (numCols <= 0)
    {
        return NULL;
    }
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&oid_vals, numCols * sizeof(uint32_t));
    for (i = 0; i < numCols; i++)
    {
        token = pg_parser_strtok(&tokenLength, strtok_ptr);
        oid_vals[i] = atooid(token);
    }

    return oid_vals;
}

/*
 * readIntCols
 */
static int32_t* readIntCols(int32_t numCols, char** strtok_ptr)
{
    int32_t     tokenLength, i;
    const char* token;
    int32_t*    int_vals;

    if (numCols <= 0)
    {
        return NULL;
    }
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&int_vals, numCols * sizeof(int32_t));

    for (i = 0; i < numCols; i++)
    {
        token = pg_parser_strtok(&tokenLength, strtok_ptr);
        int_vals[i] = atoi(token);
    }

    return int_vals;
}

/*
 * readBoolCols
 */
static bool* readBoolCols(int32_t numCols, char** strtok_ptr)
{
    int32_t     tokenLength, i;
    const char* token;
    bool*       bool_vals;

    if (numCols <= 0)
    {
        return NULL;
    }
    pg_parser_mcxt_malloc(NODE_MCXT, (void**)&bool_vals, numCols * sizeof(bool));
    for (i = 0; i < numCols; i++)
    {
        token = pg_parser_strtok(&tokenLength, strtok_ptr);
        bool_vals[i] = strtobool(token);
    }

    return bool_vals;
}
