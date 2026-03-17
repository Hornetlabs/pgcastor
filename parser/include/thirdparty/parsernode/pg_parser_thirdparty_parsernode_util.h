#ifndef XK_PG_PARSER_THIRDPARTY_PARSERNODE_UTIL_H
#define XK_PG_PARSER_THIRDPARTY_PARSERNODE_UTIL_H

/* 定义 */
#define NIL ((xk_pg_parser_List *) NULL)

extern bool get_rule_expr(xk_pg_parser_Node *node,
                          xk_pg_parser_deparse_context *context,
                          bool showimplicit);

#endif
