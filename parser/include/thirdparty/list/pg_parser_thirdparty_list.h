#ifndef PG_PARSER_THIRDPARTY_LIST_H
#define PG_PARSER_THIRDPARTY_LIST_H

#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode_node.h"

typedef struct pg_parser_ListCell pg_parser_ListCell;

typedef struct pg_parser_List
{
    pg_parser_NodeTag   type;
    int32_t             length;
    pg_parser_ListCell* head;
    pg_parser_ListCell* tail;
} pg_parser_List;

struct pg_parser_ListCell
{
    union
    {
        void*    ptr_value;
        int      int_value;
        uint32_t oid_value;
    } data;

    pg_parser_ListCell* next;
};

static inline pg_parser_ListCell* pg_parser_list_head(const pg_parser_List* l)
{
    return l ? l->head : NULL;
}

static inline int pg_parser_list_length(const pg_parser_List* l)
{
    return l ? l->length : 0;
}

#define PG_PARSER_NIL              ((pg_parser_List*)NULL)
#define pg_parser_lnext(lc)        ((lc)->next)
#define pg_parser_lfirst(lc)       ((lc)->data.ptr_value)
#define pg_parser_lfirst_int(lc)   ((lc)->data.int_value)
#define pg_parser_lfirst_oid(lc)   ((lc)->data.oid_value)
#define pg_parser_lsecond(l)       pg_parser_lfirst(pg_parser_lnext(pg_parser_list_head(l)))
#define pg_parser_foreach(cell, l) for ((cell) = pg_parser_list_head(l); (cell) != NULL; (cell) = pg_parser_lnext(cell))

#define pg_parser_linitial(l)      pg_parser_lfirst(pg_parser_list_head(l))

extern pg_parser_List* pg_parser_list_lappend(pg_parser_List* list, void* datum);
extern pg_parser_List* pg_parser_list_lappend_int(pg_parser_List* list, int datum);
extern pg_parser_List* pg_parser_list_lappend_oid(pg_parser_List* list, uint32_t datum);
extern void pg_parser_list_free(pg_parser_List* list);
extern pg_parser_ListCell* pg_parser_list_nth_cell(const pg_parser_List* list, int n);
extern void* pg_parser_list_nth(const pg_parser_List* list, int n);
extern pg_parser_List* pg_parser_lcons(void* datum, pg_parser_List* list);
extern pg_parser_List* pg_parser_list_delete_first(pg_parser_List* list);
#endif
