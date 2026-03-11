#include "app_incl.h"
#include "utils/rbtree/rbtree.h"

/*
 * new node
 *
 * indicate for value type
 * */
static rbtreenode* rbtree_newnode(void)
{
    rbtreenode* tnode = NULL;

    tnode = rmalloc0(sizeof(rbtreenode));
    if (NULL == tnode)
    {
        elog(RLOG_WARNING, "rbtree newnode error");
        return NULL;
    }
    rmemset0(tnode, 0, '\0', sizeof(rbtreenode));
    tnode->color = RBTREE_COLOR_RED;
    tnode->data = NULL;
    return tnode;
}

/*
 * init
 *   Initialize a sentinel node
 * */
rbtree* rbtree_init(treenodedatacmp datacmp, treenodedatafree datafree, treenodedebug debug)
{
    rbtree*     tree = NULL;
    rbtreenode* sentinel = NULL;

    tree = rmalloc0(sizeof(rbtree));
    if (NULL == tree)
    {
        elog(RLOG_WARNING, "rbtree init sentinel error");
        return NULL;
    }
    rmemset0(tree, 0, '\0', sizeof(rbtree));

    /* Root node is the sentinel node */
    sentinel = rbtree_newnode();
    if (NULL == sentinel)
    {
        elog(RLOG_WARNING, "rbtree init sentinel error");
        return NULL;
    }

    sentinel->color = RBTREE_COLOR_BLACK;
    sentinel->left = sentinel->right = sentinel->parent = NULL;
    tree->root = tree->sentinel = sentinel;
    tree->cmpare = datacmp;
    tree->free = datafree;
    tree->debug = debug;
    return tree;
}

/*
 * left rotate
 *
 * let the node's right child to be the node's parent. the right child's left child to be node's
 * right child.
 *
 */
static void rbtree_leftrotate(rbtreenode** root, rbtreenode* node, rbtreenode* sentinel)
{
    rbtreenode* temp = NULL;

    temp = node->right;
    node->right = temp->left;

    if (temp->left != sentinel)
    {
        temp->left->parent = node;
    }

    temp->parent = node->parent;
    if (node == *root)
    {
        *root = temp;
    }
    else if (node == node->parent->left)
    {
        node->parent->left = temp;
    }
    else
    {
        node->parent->right = temp;
    }

    temp->left = node;
    node->parent = temp;
}

/*
 * right rotate
 * */
static void rbtree_rightrotate(rbtreenode** root, rbtreenode* node, rbtreenode* sentinel)
{
    rbtreenode* temp = NULL;

    temp = node->left;
    node->left = temp->right;

    if (temp->right != sentinel)
    {
        temp->right->parent = node;
    }

    temp->parent = node->parent;
    if (node == *root)
    {
        *root = temp;
    }
    else if (node == node->parent->left)
    {
        node->parent->left = temp;
    }
    else
    {
        node->parent->right = temp;
    }

    node->parent = temp;
    temp->right = node;
}

/* Insert node */
static void rbtree_addnode(rbtree* tree, rbtreenode* node)
{
    rbtreenode** p = NULL;
    rbtreenode*  tnode = NULL;

    tnode = tree->root;

    for (;;)
    {
        p = (tree->cmpare(node->data, tnode->data) < 0 ? &tnode->left : &tnode->right);
        if (*p == tree->sentinel)
        {
            break;
        }

        tnode = *p;
    }

    *p = node;
    node->parent = tnode;
    node->left = tree->sentinel;
    node->right = tree->sentinel;
    node->color = RBTREE_COLOR_RED;
}

/*
 *insert node
 *  */
bool rbtree_insert(rbtree* rbtree, void* data)
{
    rbtreenode*  temp = NULL;
    rbtreenode** root = NULL;
    rbtreenode*  node = NULL;
    if (NULL == rbtree || data == NULL)
    {
        return true;
    }

    node = rbtree_newnode();
    if (NULL == node)
    {
        elog(RLOG_WARNING, "rbtree insert error");
        return false;
    }
    node->data = data;

    /* root */
    root = &(rbtree->root);
    if (*root == rbtree->sentinel)
    {
        node->parent = NULL;
        node->left = rbtree->sentinel;
        node->right = rbtree->sentinel;
        node->color = RBTREE_COLOR_BLACK;
        *root = node;
        return true;
    }

    /* insert */
    rbtree_addnode(rbtree, node);

    /* rebalance */
    while ((node != *root) && (node->parent->color == RBTREE_COLOR_RED))
    {
        if (node->parent == node->parent->parent->left)
        {
            temp = node->parent->parent->right;
            if (temp->color == RBTREE_COLOR_RED)
            {
                temp->color = RBTREE_COLOR_BLACK;
                node->parent->color = RBTREE_COLOR_BLACK;
                node->parent->parent->color = RBTREE_COLOR_RED;
                node = node->parent->parent;
            }
            else
            {
                if (node == node->parent->right)
                {
                    node = node->parent;
                    /* left rotate */
                    rbtree_leftrotate(root, node, rbtree->sentinel);
                }

                node->parent->color = RBTREE_COLOR_BLACK;
                node->parent->parent->color = RBTREE_COLOR_RED;
                rbtree_rightrotate(root, node->parent->parent, rbtree->sentinel);
            }
        }
        else
        {
            temp = node->parent->parent->left;
            if (temp->color == RBTREE_COLOR_RED)
            {
                temp->color = RBTREE_COLOR_BLACK;
                node->parent->color = RBTREE_COLOR_BLACK;
                node->parent->parent->color = RBTREE_COLOR_RED;
                node = node->parent->parent;
            }
            else
            {
                if (node == node->parent->left)
                {
                    node = node->parent;
                    /* right rotate */
                    rbtree_rightrotate(root, node, rbtree->sentinel);
                }
                node->parent->color = RBTREE_COLOR_BLACK;
                node->parent->parent->color = RBTREE_COLOR_RED;
                rbtree_leftrotate(root, node->parent->parent, rbtree->sentinel);
            }
        }
    }
    (*root)->color = RBTREE_COLOR_BLACK;
    return true;
}

static rbtreenode* rbtree_minnode(rbtreenode* node, rbtreenode* sentinel)
{
    while (sentinel != node->left)
    {
        node = node->left;
    }
    return node;
}

/* delete node */
void rbtree_delete(rbtree* rbtree, rbtreenode* node)
{
    int          color = RBTREE_COLOR_RED;
    rbtreenode*  bro = NULL;
    rbtreenode*  subnode = NULL;
    rbtreenode*  temp = NULL;
    rbtreenode*  sentinel = NULL;
    rbtreenode** root = NULL;
    ;
    if (NULL == rbtree || NULL == rbtree->root || NULL == node)
    {
        return;
    }

    root = &(rbtree->root);
    sentinel = rbtree->sentinel;

    /*
     * step 1: replace
     *  */
    if (sentinel == node->left)
    {
        temp = node->right;
        subnode = node;
    }
    else if (sentinel == node->right)
    {
        temp = node->left;
        subnode = node;
    }
    else
    {
        /* the min node */
        subnode = rbtree_minnode(node->right, sentinel);
        if (subnode->left != sentinel)
        {
            temp = subnode->left;
        }
        else
        {
            temp = subnode->right;
        }
    }

    if (subnode == rbtree->root)
    {
        *root = temp;
        temp->color = RBTREE_COLOR_BLACK;
        return;
    }

    color = subnode->color;
    /* subnode parent to *node */
    if (subnode == subnode->parent->left)
    {
        subnode->parent->left = temp;
    }
    else if (subnode == subnode->parent->right)
    {
        subnode->parent->right = temp;
    }

    if (node == subnode)
    {
        temp->parent = subnode->parent;
    }
    else
    {
        if (subnode->parent == node)
        {
            temp->parent = subnode;
        }
        else
        {
            temp->parent = subnode->parent;
        }

        subnode->left = node->left;
        subnode->right = node->right;
        subnode->parent = node->parent;
        subnode->color = node->color;
        if (node == *root)
        {
            *root = subnode;
        }
        else
        {
            /* *node's parent to subnode */
            if (node->parent->left == node)
            {
                node->parent->left = subnode;
            }
            else
            {
                node->parent->right = subnode;
            }
        }

        if (sentinel != subnode->left)
        {
            subnode->left->parent = subnode;
        }

        if (sentinel != subnode->right)
        {
            subnode->right->parent = subnode;
        }
    }

    if (NULL != node->data)
    {
        if (NULL != rbtree->free)
        {
            rbtree->free(node->data);
        }
    }
    rfree(node);

    if (RBTREE_COLOR_RED == color)
    {
        return;
    }

    /*
     * reblance
     *  */
    while (temp != rbtree->root && (RBTREE_COLOR_BLACK == temp->color))
    {
        if (temp == temp->parent->left)
        {
            bro = temp->parent->right;
            /* case 1: bro's red, need right rotate */
            if (RBTREE_COLOR_RED == bro->color)
            {
                bro->color = RBTREE_COLOR_BLACK;
                temp->parent->color = RBTREE_COLOR_RED;
                rbtree_leftrotate(&(rbtree->root), temp->parent, sentinel);
                bro = temp->parent->right;
            }

            if (RBTREE_COLOR_BLACK == bro->left->color && RBTREE_COLOR_BLACK == bro->right->color)
            {
                /* All children of sibling node are black */
                bro->color = RBTREE_COLOR_RED;
                temp = temp->parent;
            }
            else
            {
                if (RBTREE_COLOR_BLACK == bro->right->color)
                {
                    bro->left->color = RBTREE_COLOR_BLACK;
                    bro->color = RBTREE_COLOR_RED;
                    rbtree_rightrotate(&(rbtree->root), bro, sentinel);
                    bro = temp->parent->right;
                }

                bro->color = temp->parent->color;
                temp->parent->color = RBTREE_COLOR_BLACK;
                bro->right->color = RBTREE_COLOR_BLACK;
                rbtree_leftrotate(&(rbtree->root), temp->parent, sentinel);
                temp = rbtree->root;
            }
        }
        else
        {
            bro = temp->parent->left;
            if (RBTREE_COLOR_RED == bro->color)
            {
                bro->color = RBTREE_COLOR_BLACK;
                temp->parent->color = RBTREE_COLOR_RED;
                rbtree_rightrotate(&(rbtree->root), temp->parent, sentinel);
                bro = temp->parent->left;
            }

            if (RBTREE_COLOR_BLACK == bro->right->color && RBTREE_COLOR_BLACK == bro->left->color)
            {
                bro->color = RBTREE_COLOR_RED;
                temp = temp->parent;
            }
            else
            {
                if (RBTREE_COLOR_BLACK == bro->left->color)
                {
                    bro->right->color = RBTREE_COLOR_BLACK;
                    bro->color = RBTREE_COLOR_RED;
                    rbtree_leftrotate(&(rbtree->root), bro, sentinel);
                    bro = temp->parent->left;
                }

                bro->color = temp->parent->color;
                temp->parent->color = RBTREE_COLOR_BLACK;
                bro->left->color = RBTREE_COLOR_BLACK;
                rbtree_rightrotate(&(rbtree->root), temp->parent, sentinel);
                temp = rbtree->root;
            }
        }
    }

    temp->color = RBTREE_COLOR_BLACK;
}

rbtreenode* rbtree_get_value(rbtree* tree, void* data)
{
    int          iret = 0;
    rbtreenode** p = NULL;
    rbtreenode*  tnode = NULL;

    /* Get root node */
    tnode = tree->root;

    for (;;)
    {
        iret = tree->cmpare(data, tnode->data);
        if (0 == iret)
        {
            return tnode;
        }
        else if (0 > iret)
        {
            p = &tnode->left;
        }
        else
        {
            p = &tnode->right;
        }

        if (*p == tree->sentinel)
        {
            break;
        }

        tnode = *p;
    }
    return NULL;
}

static void rbtree_traversefree(rbtreenode* root, rbtreenode* sentinel, treenodedatafree datafree)
{
    if (root == NULL)
    {
        return;
    }

    /* Recursively free left subtree */
    rbtree_traversefree(root->left, sentinel, datafree);

    /* Recursively free right subtree */
    rbtree_traversefree(root->right, sentinel, datafree);

    /* Free current node */
    if (sentinel != root)
    {
        /* TODO data release */
        datafree(root->data);
        rfree(root);
    }

    return;
}

void rbtree_free(rbtree* tree)
{
    if (tree == NULL)
    {
        return;
    }

    /* Recursively free left subtree */
    rbtree_traversefree(tree->root->left, tree->sentinel, tree->free);

    /* Recursively free right subtree */
    rbtree_traversefree(tree->root->right, tree->sentinel, tree->free);

    /* Free current node */
    if (tree->root != tree->sentinel)
    {
        tree->free(tree->root->data);
        rfree(tree->root);
    }

    rfree(tree->sentinel);
    rfree(tree);
    return;
}

static void rbtree_treenodedebug(rbtree* tree, rbtreenode* tnode)
{
    if (NULL == tnode || tnode == tree->sentinel)
    {
        return;
    }

    if (NULL != tnode->left)
    {
        rbtree_treenodedebug(tree, tnode->left);
    }

    /* Output this node */
    tree->debug(tnode->data);

    if (NULL != tnode->right)
    {
        rbtree_treenodedebug(tree, tnode->right);
    }
}

/* for debug */
void rbtree_debug(rbtree* tree)
{
    rbtreenode* tnode = NULL;
    if (NULL == tree)
    {
        return;
    }

    /* Get root node */
    tnode = tree->root;

    rbtree_treenodedebug(tree, tnode);
}
