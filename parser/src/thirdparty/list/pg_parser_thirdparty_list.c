#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/list/xk_pg_parser_thirdparty_list.h"

#define XK_LIST_MCXT NULL

static xk_pg_parser_List *new_list(xk_pg_parser_NodeTag type)
{
    xk_pg_parser_List       *new_list;
    xk_pg_parser_ListCell   *new_head;

    xk_pg_parser_mcxt_malloc(XK_LIST_MCXT, (void **) &new_head, sizeof(xk_pg_parser_ListCell));
    /* new_head->data is left undefined! */
    xk_pg_parser_mcxt_malloc(XK_LIST_MCXT, (void **) &new_list, sizeof(xk_pg_parser_List));
    new_head->next = NULL;
    new_list->length = 1;
    new_list->head = new_head;
    new_list->tail = new_head;
    new_list->type = type;

    return new_list;
}

static void new_tail_cell(xk_pg_parser_List *list)
{
    xk_pg_parser_ListCell   *new_tail;

    xk_pg_parser_mcxt_malloc(XK_LIST_MCXT, (void **) &new_tail, sizeof(*new_tail));
    new_tail->next = NULL;

    list->tail->next = new_tail;
    list->tail = new_tail;
    list->length++;
}


xk_pg_parser_List *xk_pg_parser_list_lappend(xk_pg_parser_List *list, void *datum)
{

    if (list == XK_PG_PARSER_NIL)
        list = new_list(T_xk_pg_parser_List);
    else
        new_tail_cell(list);

    xk_pg_parser_lfirst(list->tail) = datum;
    return list;
}

xk_pg_parser_List *xk_pg_parser_list_lappend_int(xk_pg_parser_List *list, int datum)
{
    if (list == XK_PG_PARSER_NIL)
        list = new_list(T_xk_pg_parser_IntList);
    else
        new_tail_cell(list);

    xk_pg_parser_lfirst_int(list->tail) = datum;
    return list;
}

xk_pg_parser_List *xk_pg_parser_list_lappend_oid(xk_pg_parser_List *list, uint32_t datum)
{
    if (list == XK_PG_PARSER_NIL)
        list = new_list(T_xk_pg_parser_OidList);
    else
        new_tail_cell(list);

    xk_pg_parser_lfirst_oid(list->tail) = datum;
    return list;
}

static inline xk_pg_parser_ListCell *list_head(const xk_pg_parser_List *l)
{
    return l ? l->head : NULL;
}

static void list_free_private(xk_pg_parser_List *list)
{
    xk_pg_parser_ListCell   *cell;

    cell = list_head(list);
    while (cell != NULL)
    {
        xk_pg_parser_ListCell   *tmp = cell;

        cell = xk_pg_parser_lnext(cell);
        xk_pg_parser_mcxt_free(XK_LIST_MCXT, tmp);
    }

    if (list)
        xk_pg_parser_mcxt_free(XK_LIST_MCXT, list);
}

void xk_pg_parser_list_free(xk_pg_parser_List *list)
{
    list_free_private(list);
}

xk_pg_parser_ListCell *xk_pg_parser_list_nth_cell(const xk_pg_parser_List *list, int n)
{
    xk_pg_parser_ListCell   *match;

    /* Does the caller actually mean to fetch the tail? */
    if (n == list->length - 1)
        return list->tail;

    for (match = list->head; n-- > 0; match = match->next)
        ;

    return match;
}

void *xk_pg_parser_list_nth(const xk_pg_parser_List *list, int n)
{
    return xk_pg_parser_lfirst(xk_pg_parser_list_nth_cell(list, n));
}

static void new_head_cell(xk_pg_parser_List *list)
{
    xk_pg_parser_ListCell   *new_head;

    xk_pg_parser_mcxt_malloc(XK_LIST_MCXT, (void**)&new_head, sizeof(*new_head));
    new_head->next = list->head;

    list->head = new_head;
    list->length++;
}

xk_pg_parser_List *xk_pg_parser_lcons(void *datum, xk_pg_parser_List *list)
{
    if (list == XK_PG_PARSER_NIL)
        list = new_list(T_xk_pg_parser_List);
    else
        new_head_cell(list);

    xk_pg_parser_lfirst(list->head) = datum;
    return list;
}

static xk_pg_parser_List *xk_pg_parser_list_delete_cell(xk_pg_parser_List *list,
                                    xk_pg_parser_ListCell *cell,
                                    xk_pg_parser_ListCell *prev)
{
    /*
     * If we're about to delete the last node from the list, free the whole
     * list instead and return NIL, which is the only valid representation of
     * a zero-length list.
     */
    if (list->length == 1)
    {
        xk_pg_parser_list_free(list);
        return XK_PG_PARSER_NIL;
    }

    /*
     * Otherwise, adjust the necessary list links, deallocate the particular
     * node we have just removed, and return the list we were given.
     */
    list->length--;

    if (prev)
        prev->next = cell->next;
    else
        list->head = cell->next;

    if (list->tail == cell)
        list->tail = prev;

    xk_pg_parser_mcxt_free(XK_LIST_MCXT, cell);
    return list;
}

xk_pg_parser_List *xk_pg_parser_list_delete_first(xk_pg_parser_List *list)
{

    if (list == XK_PG_PARSER_NIL)
        return XK_PG_PARSER_NIL;                /* would an error be better? */

    return xk_pg_parser_list_delete_cell(list, xk_pg_parser_list_head(list), NULL);
}

