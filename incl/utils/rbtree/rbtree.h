#ifndef _RBTREE_H_
#define _RBTREE_H_

typedef enum
{
    RBTREE_COLOR_BLACK = 0,
    RBTREE_COLOR_RED
} RBTREE_COLOR;

typedef struct RBTREE_NODE
{
    int8_t              color;  /* Node color */
    void*               data;   /* Structure     */
    struct RBTREE_NODE* left;   /* Left child node */
    struct RBTREE_NODE* right;  /* Right child node */
    struct RBTREE_NODE* parent; /* Parent node   */
} rbtreenode;

typedef int  (*treenodedatacmp)(void* v1, void* v2);
typedef void (*treenodedatafree)(void* v1);
typedef void (*treenodedebug)(void* v1);

typedef struct RBTREE
{
    rbtreenode*      root;     /* Root node of red-black tree */
    rbtreenode*      sentinel; /* Sentinel node */
    treenodedatacmp  cmpare;   /* Compare function */
    treenodedatafree free;
    treenodedebug    debug;
} rbtree;

typedef struct TABLEOPTYPE
{
    Oid   relid;  /* tableoid */
    uint8 optype; /* optype */
} tableoptype;

typedef struct TABLEOP2PREPARESTMT
{
    tableoptype tableop;
    rbtree*     rbtree;
    char*       debugstr;
} tableop2preparestmt;

/*
 *insert node
 *  */
bool rbtree_insert(rbtree* rbtree, void* data);

void rbtree_delete(rbtree* rbtree, rbtreenode* node);

/*
 * init
 *   Initialize a sentinel node
 * */
rbtree* rbtree_init(treenodedatacmp datacmp, treenodedatafree datafree, treenodedebug debug);

rbtreenode* rbtree_get_value(rbtree* tree, void* data);

void rbtree_free(rbtree* tree);

/* for debug */
void rbtree_debug(rbtree* tree);

#endif