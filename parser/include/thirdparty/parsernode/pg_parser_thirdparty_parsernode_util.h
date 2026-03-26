#ifndef PG_PARSER_THIRDPARTY_PARSERNODE_UTIL_H
#define PG_PARSER_THIRDPARTY_PARSERNODE_UTIL_H

/* Definition */
#define NIL ((pg_parser_List*)NULL)

extern bool get_rule_expr(pg_parser_Node*            node,
                          pg_parser_deparse_context* context,
                          bool                       showimplicit);

#endif
