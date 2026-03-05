#ifndef _RBTREE_H_
#define _RBTREE_H_

typedef enum 
{
    RIPPLE_RBTREE_COLOR_BLACK = 0,
    RIPPLE_RBTREE_COLOR_RED
} RIPPLE_RBTREE_COLOR;

typedef struct RBTREE_NODE 
{
    int8_t                          color;                          /* 节点颜色 */
    void*                           data;                           /* 结构     */
    struct RBTREE_NODE*             left;                           /* 左子节点 */
    struct RBTREE_NODE*             right;                          /* 右子节点 */
    struct RBTREE_NODE*             parent;                         /* 父节点   */
} rbtreenode;

typedef int (*treenodedatacmp) (void* v1, void* v2);
typedef void (*treenodedatafree)(void* v1);
typedef void (*treenodedebug)(void* v1);

typedef struct RBTREE
{
    rbtreenode                  *root;                              /* 红黑树的根节点 */
    rbtreenode                  *sentinel;                          /* 哨兵节点 */
    treenodedatacmp             cmpare;                             /* 比较函数 */
    treenodedatafree            free;
    treenodedebug               debug;
} rbtree;

typedef struct TABLEOPTYPE
{
    Oid                         relid;                              /* tableoid */
    uint8                       optype;                             /* optype */
} tableoptype;

typedef struct RIPPLE_TABLEOP2PREPARESTMT
{
    tableoptype                 tableop;
    rbtree*                     rbtree;
    char*                       debugstr;
} ripple_tableop2preparestmt;

/*
 *insert node
 *  */
bool rbtree_insert(rbtree *rbtree, void* data);

void rbtree_delete(rbtree *rbtree, rbtreenode *node);

/*
 * init
 *   初始化一个哨兵节点
 * */
rbtree* rbtree_init(treenodedatacmp datacmp, treenodedatafree datafree, treenodedebug debug);

rbtreenode* rbtree_get_value(rbtree* tree, void* data);

void rbtree_free(rbtree* tree);

/* for debug */
void rbtree_debug(rbtree* tree);

#endif