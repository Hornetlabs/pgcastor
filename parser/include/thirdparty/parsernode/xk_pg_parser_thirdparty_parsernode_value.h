#ifndef XK_PG_PARSER_THIRDPARTY_PARSERNODE_VALUE_H
#define XK_PG_PARSER_THIRDPARTY_PARSERNODE_VALUE_H

typedef struct xk_pg_parser_Value
{
    xk_pg_parser_NodeTag type;            /* tag appropriately (eg. T_String) */
    union ValUnion
    {
        int         ival;        /* machine integer */
        char       *str;        /* string */
    }val;
} xk_pg_parser_Value;

#endif
