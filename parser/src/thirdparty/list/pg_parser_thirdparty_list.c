#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/list/pg_parser_thirdparty_list.h"

#define LIST_MCXT NULL

static pg_parser_List* new_list(pg_parser_NodeTag type)
{
    pg_parser_List*     new_list;
    pg_parser_ListCell* new_head;

    pg_parser_mcxt_malloc(LIST_MCXT, (void**)&new_head, sizeof(pg_parser_ListCell));
    /* new_head->data is left undefined! */
    pg_parser_mcxt_malloc(LIST_MCXT, (void**)&new_list, sizeof(pg_parser_List));
    new_head->next = NULL;
    new_list->length = 1;
    new_list->head = new_head;
    new_list->tail = new_head;
    new_list->type = type;

    return new_list;
}

static void new_tail_cell(pg_parser_List* list)
{
    pg_parser_ListCell* new_tail;

    pg_parser_mcxt_malloc(LIST_MCXT, (void**)&new_tail, sizeof(*new_tail));
    new_tail->next = NULL;

    list->tail->next = new_tail;
    list->tail = new_tail;
    list->length++;
}

pg_parser_List* pg_parser_list_lappend(pg_parser_List* list, void* datum)
{
    if (list == PG_PARSER_NIL)
    {
        list = new_list(T_pg_parser_List);
    }
    else
    {
        new_tail_cell(list);
    }

    pg_parser_lfirst(list->tail) = datum;
    return list;
}

pg_parser_List* pg_parser_list_lappend_int(pg_parser_List* list, int datum)
{
    if (list == PG_PARSER_NIL)
    {
        list = new_list(T_pg_parser_IntList);
    }
    else
    {
        new_tail_cell(list);
    }

    pg_parser_lfirst_int(list->tail) = datum;
    return list;
}

pg_parser_List* pg_parser_list_lappend_oid(pg_parser_List* list, uint32_t datum)
{
    if (list == PG_PARSER_NIL)
    {
        list = new_list(T_pg_parser_OidList);
    }
    else
    {
        new_tail_cell(list);
    }

    pg_parser_lfirst_oid(list->tail) = datum;
    return list;
}

static inline pg_parser_ListCell* list_head(const pg_parser_List* l)
{
    return l ? l->head : NULL;
}

static void list_free_private(pg_parser_List* list)
{
    pg_parser_ListCell* cell;

    cell = list_head(list);
    while (cell != NULL)
    {
        pg_parser_ListCell* tmp = cell;

        cell = pg_parser_lnext(cell);
        pg_parser_mcxt_free(LIST_MCXT, tmp);
    }

    if (list)
    {
        pg_parser_mcxt_free(LIST_MCXT, list);
    }
}

void pg_parser_list_free(pg_parser_List* list)
{
    list_free_private(list);
}

pg_parser_ListCell* pg_parser_list_nth_cell(const pg_parser_List* list, int n)
{
    pg_parser_ListCell* match;

    /* Does the caller actually mean to fetch the tail? */
    if (n == list->length - 1)
    {
        return list->tail;
    }

    for (match = list->head; n-- > 0; match = match->next)
        ;

    return match;
}

void* pg_parser_list_nth(const pg_parser_List* list, int n)
{
    return pg_parser_lfirst(pg_parser_list_nth_cell(list, n));
}

static void new_head_cell(pg_parser_List* list)
{
    pg_parser_ListCell* new_head;

    pg_parser_mcxt_malloc(LIST_MCXT, (void**)&new_head, sizeof(*new_head));
    new_head->next = list->head;

    list->head = new_head;
    list->length++;
}

pg_parser_List* pg_parser_lcons(void* datum, pg_parser_List* list)
{
    if (list == PG_PARSER_NIL)
    {
        list = new_list(T_pg_parser_List);
    }
    else
    {
        new_head_cell(list);
    }

    pg_parser_lfirst(list->head) = datum;
    return list;
}

static pg_parser_List* pg_parser_list_delete_cell(pg_parser_List*     list,
                                                  pg_parser_ListCell* cell,
                                                  pg_parser_ListCell* prev)
{
    /*
     * If we're about to delete the last node from the list, free the whole
     * list instead and return NIL, which is the only valid representation of
     * a zero-length list.
     */
    if (list->length == 1)
    {
        pg_parser_list_free(list);
        return PG_PARSER_NIL;
    }

    /*
     * Otherwise, adjust the necessary list links, deallocate the particular
     * node we have just removed, and return the list we were given.
     */
    list->length--;

    if (prev)
    {
        prev->next = cell->next;
    }
    else
    {
        list->head = cell->next;
    }

    if (list->tail == cell)
    {
        list->tail = prev;
    }

    pg_parser_mcxt_free(LIST_MCXT, cell);
    return list;
}

pg_parser_List* pg_parser_list_delete_first(pg_parser_List* list)
{
    if (list == PG_PARSER_NIL)
    {
        return PG_PARSER_NIL; /* would an error be better? */
    }

    return pg_parser_list_delete_cell(list, pg_parser_list_head(list), NULL);
}
