#ifndef PG_PARSER_THIRDPARTY_PARSERNODE_VALUE_H
#define PG_PARSER_THIRDPARTY_PARSERNODE_VALUE_H

typedef struct pg_parser_Value
{
    pg_parser_NodeTag type; /* tag appropriately (eg. T_String) */

    union ValUnion
    {
        int   ival; /* machine integer */
        char* str;  /* string */
    } val;
} pg_parser_Value;

#endif
