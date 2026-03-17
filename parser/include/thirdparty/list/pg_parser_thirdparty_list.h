#ifndef XK_PG_PARSER_THIRDPARTY_LIST_H
#define XK_PG_PARSER_THIRDPARTY_LIST_H

#include "thirdparty/parsernode/xk_pg_parser_thirdparty_parsernode_node.h"

typedef struct xk_pg_parser_ListCell xk_pg_parser_ListCell;

typedef struct xk_pg_parser_List
{
    xk_pg_parser_NodeTag     type;
    int32_t                  length;
    xk_pg_parser_ListCell   *head;
    xk_pg_parser_ListCell   *tail;
} xk_pg_parser_List;

struct xk_pg_parser_ListCell
{
    union
    {
        void       *ptr_value;
        int         int_value;
        uint32_t    oid_value;
    }data;
    xk_pg_parser_ListCell *next;
};

static inline xk_pg_parser_ListCell *xk_pg_parser_list_head(const xk_pg_parser_List *l)
{
    return l ? l->head : NULL;
}
static inline int xk_pg_parser_list_length(const xk_pg_parser_List *l)
{
    return l ? l->length : 0;
}

#define XK_PG_PARSER_NIL ((xk_pg_parser_List *) NULL)
#define xk_pg_parser_lnext(lc)       ((lc)->next)
#define xk_pg_parser_lfirst(lc)      ((lc)->data.ptr_value)
#define xk_pg_parser_lfirst_int(lc)  ((lc)->data.int_value)
#define xk_pg_parser_lfirst_oid(lc)  ((lc)->data.oid_value)
#define xk_pg_parser_lsecond(l)      xk_pg_parser_lfirst(xk_pg_parser_lnext(xk_pg_parser_list_head(l)))
#define xk_pg_parser_foreach(cell, l)    \
    for ((cell) = xk_pg_parser_list_head(l); (cell) != NULL; (cell) = xk_pg_parser_lnext(cell))

#define xk_pg_parser_linitial(l) xk_pg_parser_lfirst(xk_pg_parser_list_head(l))

extern xk_pg_parser_List *xk_pg_parser_list_lappend(xk_pg_parser_List *list, void *datum);
extern xk_pg_parser_List *xk_pg_parser_list_lappend_int(xk_pg_parser_List *list, int datum);
extern xk_pg_parser_List *xk_pg_parser_list_lappend_oid(xk_pg_parser_List *list, uint32_t datum);
extern void xk_pg_parser_list_free(xk_pg_parser_List *list);
extern xk_pg_parser_ListCell *xk_pg_parser_list_nth_cell(const xk_pg_parser_List *list, int n);
extern void *xk_pg_parser_list_nth(const xk_pg_parser_List *list, int n);
extern xk_pg_parser_List *xk_pg_parser_lcons(void *datum, xk_pg_parser_List *list);
extern xk_pg_parser_List *xk_pg_parser_list_delete_first(xk_pg_parser_List *list);
#endif
