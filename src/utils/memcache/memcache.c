#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/memcache/memcache.h"

/* Mapping between virtual number and page number */
static memcache_vnumber2pnode* memcache_vnumber2pnode_init(void)
{
    memcache_vnumber2pnode* vn2pnode = NULL;
    vn2pnode = rmalloc0(sizeof(memcache_vnumber2pnode));
    if (NULL == vn2pnode)
    {
        elog(RLOG_WARNING, "vnumber2pnode_init out of memory");
        return NULL;
    }
    rmemset0(vn2pnode, 0, '\0', sizeof(memcache_vnumber2pnode));
    return vn2pnode;
}

/* Virtual node memory release */
static void memcache_vnumber2pnode_freefordlist(void* args)
{
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == args)
    {
        return;
    }

    vn2pnode = (memcache_vnumber2pnode*)args;
    if (-1 != vn2pnode->fd)
    {
        osal_file_close(vn2pnode->fd);
    }
    rfree(vn2pnode);
}

/* virtualnode release */
static void memcache_virtualnode_free(memcache_virtualnode* vnode)
{
    if (NULL == vnode)
    {
        return;
    }

    dlist_free(vnode->nodes, memcache_vnumber2pnode_freefordlist);
}

/* pagenode release */
static void memcache_pagenode_free(memcache_pagenode* mpnode)
{
    if (NULL == mpnode)
    {
        return;
    }

    if (NULL == mpnode->data)
    {
        return;
    }

    rfree(mpnode->data);
}

/* pages release */
static void memcache_pages_free(memcache_pages* mpages)
{
    uint64 index = 0;
    if (NULL == mpages)
    {
        return;
    }

    dlist_free(mpages->pages, NULL);
    dlist_free(mpages->freepages, NULL);

    if (0 < mpages->pagecnt && NULL != mpages->pageaddr)
    {
        for (index = 0; index < mpages->pagecnt; index++)
        {
            memcache_pagenode_free(mpages->pageaddr + index);
        }
        rfree(mpages->pageaddr);
    }
    rfree(mpages);
}

/*
 * Initialize a cache
 *  name            Unique identifier, do not duplicate within process
 *  csize           Total size, minimum 16M, maximum 16G, in M units
 *  blksize         Page size for each cache, must be multiple of 8192, minimum 8192, maximum 1M, in
 * bytes
 */
memcache* memcache_init(char* name, int csize, int blksize)
{
    uint64_t              index = 0;
    memcache*             mcache = NULL;
    memcache_pagenode*    pnode = NULL;
    memcache_virtualnode* mcachevnode = NULL;
    char                  mdir[MEMCACHE_DIRLEN] = {0};

    if (MEMCACHE_BLOCKSIZE_MAX < blksize)
    {
        blksize = MEMCACHE_BLOCKSIZE_MAX;
    }

    if (MEMCACHE_BLOCKSIZE_DEFAULT > blksize)
    {
        blksize = MEMCACHE_BLOCKSIZE_DEFAULT;
    }

    if (MEMCACHE_DEFAULT > csize)
    {
        csize = MEMCACHE_DEFAULT;
    }

    if (MEMCACHE_MAXSIZE < csize)
    {
        csize = MEMCACHE_MAXSIZE;
    }

    /*--------------Basic initialization begin----------------------------------*/
    mcache = rmalloc0(sizeof(memcache));
    if (NULL == mcache)
    {
        elog(RLOG_WARNING, "mem_init error");
        return NULL;
    }
    rmemset0(mcache, 0, '\0', sizeof(memcache));
    mcache->name = rstrdup(name);
    mcache->blksize = blksize;
    mcache->maxsize = MEMCACHE_MB2BYTE(csize);

    /* Calculate required blocks */
    mcache->blkcnt = mcache->maxsize / (uint64)mcache->blksize;

    /*--------------Basic initialization   end----------------------------------*/

    /*--------------Virtual number related initialization begin----------------------*/
    mcache->vnumber = 1;
    mcache->vnodemapcnt = MEMCACHE_VNODEMAPCNT;
    mcache->vnodemap = rmalloc0(mcache->vnodemapcnt * sizeof(memcache_virtualnode));
    if (NULL == mcache->vnodemap)
    {
        elog(RLOG_WARNING, "memcache init vnodemap error");
        return NULL;
    }

    /* Initialization settings */
    for (index = 0; index < mcache->vnodemapcnt; index++)
    {
        mcachevnode = mcache->vnodemap + index;
        mcachevnode->nodes = NULL;
    }

    /*--------------Virtual number related initialization   end----------------------*/

    /*--------------Page related initialization begin----------------------------*/
    mcache->pages = rmalloc0(sizeof(memcache_pages));
    if (NULL == mcache->pages)
    {
        elog(RLOG_WARNING, "memcache init memcache_pages error");
        return NULL;
    }
    rmemset0(mcache->pages, 0, '\0', sizeof(memcache_pages));
    mcache->pages->blksize = mcache->blksize;
    mcache->pages->pagecnt = mcache->blkcnt;
    mcache->pages->swapcnt = MEMCACHE_SWAPBATCHCNT;
    mcache->pages->freepages = NULL;
    mcache->pages->pages = NULL;
    mcache->pages->pageaddr =
        (memcache_pagenode*)rmalloc0(mcache->pages->pagecnt * sizeof(memcache_pagenode));
    if (NULL == mcache->pages->pageaddr)
    {
        elog(RLOG_WARNING, "memcache init pageaddr error");
        return NULL;
    }

    /* Initial value settings */
    for (index = 1; index <= mcache->pages->pagecnt; index++)
    {
        pnode = mcache->pages->pageaddr + index - 1;
        pnode->count = 0;
        pnode->data = NULL;
        pnode->no = index;
        pnode->vno = 0;
        pnode->dlnode = NULL;

        mcache->pages->freepages = dlist_put(mcache->pages->freepages, pnode);
        mcache->pages->pages = dlist_put(mcache->pages->pages, pnode);

        /* Point to self */
        pnode->dlnode = mcache->pages->pages->tail;
    }
    /*--------------Page related initialization   end----------------------------*/

    elog(RLOG_INFO,
         "init memcache:%s, size:%lu, blkcnt:%lu",
         name,
         mcache->maxsize,
         mcache->pages->pagecnt);

    /* Create directory */
    snprintf(mdir, MEMCACHE_DIRLEN, "%s/%s", MEMCACHE_DIR, name);
    osal_make_dir(mdir);
    return mcache;
}

/* Virtual node comparison */
static int memcache_vn2pnode_cmp(void* s1, void* s2)
{
    memcache_vnumber2pnode* v1 = NULL;
    memcache_vnumber2pnode* v2 = NULL;

    v1 = (memcache_vnumber2pnode*)s1;
    v2 = (memcache_vnumber2pnode*)s2;

    if (v1->vno == v2->vno)
    {
        return 0;
    }

    return 1;
}

/* Get virtual node by virtual number */
static bool memcache_getvn2pnodebyvnumber(memcache_virtualnode*    vnode,
                                          uint64_t                 vnumber,
                                          memcache_vnumber2pnode** pvn2pnode)
{
    dlistnode*              dlnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == vnode || true == dlist_isnull(vnode->nodes))
    {
        elog(RLOG_WARNING, "get virtual number 2 page node by virtual number argument error");
        return false;
    }

    for (dlnode = vnode->nodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        vn2pnode = (memcache_vnumber2pnode*)dlnode->value;
        if (vnumber != vn2pnode->vno)
        {
            continue;
        }

        *pvn2pnode = vn2pnode;
        return true;
    }

    elog(RLOG_WARNING, "can not get virtual number 2 page node by virtual number:%lu", vnumber);
    return false;
}

/* Swap virtual page to disk and mark page as not in memory */
static bool memcache_vnumber2pnodeswap(memcache*               mcache,
                                       memcache_vnumber2pnode* vn2pnode,
                                       uint8*                  blk)
{
    char vfile[MEMCACHE_DIRLEN] = {0};

    if (-1 == vn2pnode->fd)
    {
        snprintf(vfile, MEMCACHE_DIRLEN, "%s/%s/%016lX", MEMCACHE_DIR, mcache->name, vn2pnode->vno);

        /* Open file */
        vn2pnode->fd = osal_basic_open_file(vfile, O_RDWR | O_CREAT | BINARY);
        if (-1 == vn2pnode->fd)
        {
            elog(RLOG_WARNING, "open file %s error, %s", vfile, strerror(errno));
            return false;
        }
    }

    if (mcache->blksize != osal_file_pwrite(vn2pnode->fd, (char*)blk, mcache->blksize, 0))
    {
        elog(RLOG_WARNING,
             "pwrite file %s/%s/%016lX error, %s",
             MEMCACHE_DIR,
             mcache->name,
             vn2pnode->vno,
             strerror(errno));
        return false;
    }

    vn2pnode->hasfile = true;
    vn2pnode->pno = 0;
    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_OUT;

    osal_file_close(vn2pnode->fd);
    vn2pnode->fd = -1;
    return true;
}

/* Load virtual page into memory */
static bool memcache_vnumber2pnodeload(memcache*               mcache,
                                       memcache_vnumber2pnode* vn2pnode,
                                       uint8*                  blk)
{
    char vfile[MEMCACHE_DIRLEN] = {0};
    if (-1 == vn2pnode->fd)
    {
        /* Open file */
        snprintf(vfile, MEMCACHE_DIRLEN, "%s/%s/%016lX", MEMCACHE_DIR, mcache->name, vn2pnode->vno);

        /* Open file */
        vn2pnode->fd = osal_basic_open_file(vfile, O_RDWR | BINARY);
        if (-1 == vn2pnode->fd)
        {
            elog(RLOG_WARNING, "open file %s error, %s", vfile, strerror(errno));
            return false;
        }
    }

    if (mcache->blksize != osal_file_pread(vn2pnode->fd, (char*)blk, mcache->blksize, 0))
    {
        elog(RLOG_WARNING,
             "pread file %s/%s/%016lX error, %s",
             MEMCACHE_DIR,
             mcache->name,
             vn2pnode->vno,
             strerror(errno));
        return false;
    }

    osal_file_close(vn2pnode->fd);
    vn2pnode->fd = -1;
    return true;
}

/*
 * Swap a batch of pages to disk
 */
static bool memcache_pageswap(memcache* mcache)
{
    int                     index = 0;
    uint64_t                vnodeindex = 0;
    dlistnode*              dlnode = NULL;
    memcache_pagenode*      pnode = NULL;
    memcache_virtualnode*   vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == mcache)
    {
        return false;
    }

    /* Number of swaps */
    for (dlnode = mcache->pages->pages->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        if (index >= mcache->pages->swapcnt)
        {
            break;
        }

        /* Swap out one page */
        pnode = (memcache_pagenode*)dlnode->value;

        /* Currently in use externally */
        if (true == pnode->used)
        {
            continue;
        }

        /*
         * Get virtual node by virtual number, write page to specified file
         */
        /* Get virtual node */
        vnodeindex =
            MEMCACHE_VNUMBER2INDEX((pnode->vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
        vnode = mcache->vnodemap + vnodeindex;
        if (false == memcache_getvn2pnodebyvnumber(vnode, pnode->vno, &vn2pnode))
        {
            elog(RLOG_WARNING, "pageswap get virtual node by number error, %lu", pnode->vno);
            return false;
        }

        /* Flush virtual page file to disk */
        if (false == memcache_vnumber2pnodeswap(mcache, vn2pnode, pnode->data))
        {
            elog(RLOG_WARNING, "pageswap vnumber2pnodeswap error, %lu", pnode->vno);
            return false;
        }

        /* Clear file block */
        rmemset0(pnode->data, 0, '\0', mcache->blksize);
        pnode->vno = 0;

        mcache->pages->freepages = dlist_put(mcache->pages->freepages, pnode);
        index++;
    }
    return true;
}

/*
 * Get an available page number
 */
static memcache_pagenode* memcache_getfreepagenode(memcache* mcache)
{
    memcache_pages*    mpages = NULL;
    memcache_pagenode* pagenode = NULL;

    if (NULL == mcache || NULL == mcache->pages)
    {
        return NULL;
    }

    mpages = mcache->pages;
    /* First get free page from free pages */
    if (false == dlist_isnull(mpages->freepages))
    {
        goto memcache_getfreepageno_done;
    }

    /* Not found, swap page */
    if (false == memcache_pageswap(mcache))
    {
        elog(RLOG_WARNING, "get free page node, pageswap error, ");
        return NULL;
    }

memcache_getfreepageno_done:

    /* Get a page number */
    pagenode = dlist_getvalue(mpages->freepages);
    if (NULL == pagenode->data)
    {
        /* Allocate space */
        pagenode->data = rmalloc0(mcache->blksize);
        if (NULL == pagenode->data)
        {
            elog(RLOG_WARNING, "get free page number, out of memory");
            return false;
        }
        rmemset0(pagenode->data, 0, '\0', mcache->blksize);
    }

    /* Move page to head */
    mpages->pages = dlist_putnode2head(mpages->pages, pagenode->dlnode);
    pagenode->dlnode = mpages->pages->head;

    return pagenode;
}

/* Get an available virtual page, for debug */
bool memcache_getvnode(memcache* mcache, uint64* vno)
{
    /*
     * 1. Get a virtual number
     * 2. Allocate virtual number node
     * 3. Get a free page
     *      3.1 When no free page
     *          3.1.1 Swap out least frequently used page, put swapped page into free pages
     *          3.1.2 Set virtual page flag to OUT (not in memory) on swapped out page
     *      3.2 Get available page from free pages
     *
     * 4. Associate page with virtual node, set virtual page flag to MEM (in memory)
     */
    uint64_t                index = 0;
    memcache_pagenode*      pagenode = NULL;
    memcache_virtualnode*   vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == mcache)
    {
        return false;
    }

    /* Get index of new virtual node */
    index =
        MEMCACHE_VNUMBER2INDEX((mcache->vnumber - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    vn2pnode = memcache_vnumber2pnode_init();
    if (NULL == vn2pnode)
    {
        elog(RLOG_WARNING, "getvnode out of memory");
        return false;
    }
    vn2pnode->hasfile = false;
    vn2pnode->vno = mcache->vnumber;
    mcache->vnumber++;

    /* Get an available free page number */
    pagenode = memcache_getfreepagenode(mcache);
    if (NULL == pagenode)
    {
        elog(RLOG_WARNING, "get free page node error");
        return false;
    }

    /* Set virtual number for pageno */
    pagenode->vno = vn2pnode->vno;
    vn2pnode->pno = pagenode->no;
    vn2pnode->fd = -1;
    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_MEM;

    /* Add vnode to list */
    vnode->nodes = dlist_put(vnode->nodes, vn2pnode);
    *vno = vn2pnode->vno;
    return true;
}

/* Get available page by virtual number */
bool memcache_getpagebyvno(memcache* mcache, uint64 vno, uint8_t** pdata)
{
    /*
     * Get page by virtual number
     * 1. Page in cache, return directly
     * 2. Page not in cache, need to swap and load into cache
     */
    uint64_t                index = 0;
    memcache_pagenode*      pnode = NULL;
    memcache_virtualnode*   vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == mcache)
    {
        elog(RLOG_WARNING, "getpagebyvno argument error");
        return false;
    }

    /* Get index of new virtual node */
    index = MEMCACHE_VNUMBER2INDEX((vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    /* Get virtual page node */
    if (false == memcache_getvn2pnodebyvnumber(vnode, vno, &vn2pnode))
    {
        elog(RLOG_WARNING, "get virtual page error");
        return false;
    }

    /*
     * Check if page is in cache
     */
    if (MEMCACHE_VNUMBER2PNODE_FLAG_MEM == vn2pnode->flag)
    {
        /* In cache, get page node */
        pnode = mcache->pages->pageaddr + (vn2pnode->pno - 1);
        goto memcache_getpagebyvno_done;
    }

    /*
     * Not in cache
     * 1. freepage has space, get a free page
     * 2. freepage has no space, swap
     */
    if (true == dlist_isnull(mcache->pages->freepages))
    {
        /* Do space swap */
        if (false == memcache_pageswap(mcache))
        {
            elog(RLOG_WARNING, "getpagebyvno pageswap error");
            return false;
        }
    }

    /* Get page node */
    pnode = dlist_getvalue(mcache->pages->freepages);
    pnode->vno = vn2pnode->vno;
    vn2pnode->pno = pnode->no;

    /* Check if need to load from disk */
    if (0 > vn2pnode->fd)
    {
        /* Load page from disk */
        if (false == memcache_vnumber2pnodeload(mcache, vn2pnode, pnode->data))
        {
            elog(RLOG_WARNING, "getpagebyvno load page error");
            return false;
        }

        /* Close descriptor */
        osal_file_close(vn2pnode->fd);
        vn2pnode->fd = -1;
    }

    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_MEM;
memcache_getpagebyvno_done:
    pnode->used = true;
    pnode->count++;
    *pdata = pnode->data;
    return true;
}

/* Get an available memory page */
bool memcache_getpage(memcache* mcache, uint64* vno, uint8_t** pdata)
{
    /*
     * 1. Get a virtual number
     * 2. Allocate virtual number node
     * 3. Get a free page
     *      3.1 When no free page
     *          3.1.1 Swap out least frequently used page, put swapped page into free pages
     *          3.1.2 Set virtual page flag to OUT (not in memory) on swapped out page
     *      3.2 Get available page from free pages
     *
     * 4. Associate page with virtual node, set virtual page flag to MEM (in memory)
     */
    uint64_t                index = 0;
    memcache_pagenode*      pnode = NULL;
    memcache_virtualnode*   vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == mcache)
    {
        return false;
    }

    /* Get index of new virtual node */
    index =
        MEMCACHE_VNUMBER2INDEX((mcache->vnumber - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    vn2pnode = memcache_vnumber2pnode_init();
    if (NULL == vn2pnode)
    {
        elog(RLOG_WARNING, "getvnode out of memory");
        return false;
    }
    vn2pnode->hasfile = false;
    vn2pnode->vno = mcache->vnumber;
    mcache->vnumber++;

    /* Get an available free page number */
    pnode = memcache_getfreepagenode(mcache);
    if (NULL == pnode)
    {
        elog(RLOG_WARNING, "get free page node error");
        return false;
    }

    /* Set virtual number for pageno */
    pnode->vno = vn2pnode->vno;
    vn2pnode->pno = pnode->no;
    vn2pnode->fd = -1;
    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_MEM;

    /* Add vnode to list */
    vnode->nodes = dlist_put(vnode->nodes, vn2pnode);
    pnode->used = true;
    pnode->count++;

    /* Set output parameter */
    *vno = vn2pnode->vno;
    *pdata = pnode->data;
    return true;
}

/* Free page by virtual number */
void memcache_putpage(memcache* mcache, uint64 vno)
{
    uint64_t                index = 0;
    memcache_pagenode*      pnode = NULL;
    memcache_virtualnode*   vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;
    char                    vfile[MEMCACHE_DIRLEN] = {0};

    if (NULL == mcache || 0 == vno)
    {
        return;
    }

    /* Get index of new virtual node */
    index = MEMCACHE_VNUMBER2INDEX((vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    /* Get virtual page node */
    if (false == memcache_getvn2pnodebyvnumber(vnode, vno, &vn2pnode))
    {
        return;
    }

    /*
     * Not in cache
     */
    if (MEMCACHE_VNUMBER2PNODE_FLAG_OUT == vn2pnode->flag)
    {
        goto memcache_putvnode_done;
    }

    /* In cache, set page node as available and put into freepages */
    pnode = mcache->pages->pageaddr + (vn2pnode->pno - 1);
    pnode->vno = 0;
    pnode->used = false;
    pnode->count--;
    mcache->pages->freepages = dlist_put(mcache->pages->freepages, pnode);

memcache_putvnode_done:

    /* Clean up file */
    if (true == vn2pnode->hasfile)
    {
        snprintf(vfile, MEMCACHE_DIRLEN, "%s/%s/%016lX", MEMCACHE_DIR, mcache->name, vn2pnode->vno);
        osal_durable_unlink(vfile, RLOG_DEBUG);
    }

    /* Clean up vnode node */
    vnode->nodes = dlist_deletebyvaluefirstmatch(
        vnode->nodes, vn2pnode, memcache_vn2pnode_cmp, memcache_vnumber2pnode_freefordlist);
    return;
}

/* After staging data, page can be swapped */
void memcache_pagestore(memcache* mcache, uint64 vno)
{
    uint64_t                index = 0;
    memcache_pagenode*      pnode = NULL;
    memcache_virtualnode*   vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if (NULL == mcache || 0 == vno)
    {
        return;
    }

    /* Get index of new virtual node */
    index = MEMCACHE_VNUMBER2INDEX((vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    /* Get virtual page node */
    if (false == memcache_getvn2pnodebyvnumber(vnode, vno, &vn2pnode))
    {
        return;
    }

    /*
     * Not in cache
     */
    if (MEMCACHE_VNUMBER2PNODE_FLAG_OUT == vn2pnode->flag)
    {
        return;
    }

    /* In cache, set page node as available and put into freepages */
    pnode = mcache->pages->pageaddr + (vn2pnode->pno - 1);

    /* Page can be swapped */
    pnode->used = false;

    return;
}

/* Free */
void memcache_free(memcache* mcache)
{
    uint64 index = 0;
    char   mdir[MEMCACHE_DIRLEN] = {0};
    if (NULL == mcache)
    {
        return;
    }

    if (NULL != mcache->name)
    {
        rfree(mcache->name);
    }

    if (0 < mcache->vnodemapcnt && NULL != mcache->vnodemap)
    {
        for (index = 0; index < mcache->vnodemapcnt; index++)
        {
            memcache_virtualnode_free(mcache->vnodemap + index);
        }
        rfree(mcache->vnodemap);
    }

    memcache_pages_free(mcache->pages);

    /* Create directory */
    snprintf(mdir, MEMCACHE_DIRLEN, "%s/%s", MEMCACHE_DIR, mcache->name);
    osal_remove_dir(mdir);

    rfree(mcache);
}
