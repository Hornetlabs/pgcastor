#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/memcache/memcache.h"

/* 虚拟编号与页编号的对应 */
static memcache_vnumber2pnode* memcache_vnumber2pnode_init(void)
{
    memcache_vnumber2pnode* vn2pnode = NULL;
    vn2pnode = rmalloc0(sizeof(memcache_vnumber2pnode));
    if(NULL == vn2pnode)
    {
        elog(RLOG_WARNING, "vnumber2pnode_init out of memory");
        return NULL;
    }
    rmemset0(vn2pnode, 0, '\0', sizeof(memcache_vnumber2pnode));
    return vn2pnode;
}

/* 虚拟节点内存释放 */
static void memcache_vnumber2pnode_freefordlist(void* args)
{
    memcache_vnumber2pnode* vn2pnode = NULL;

    if(NULL == args)
    {
        return;
    }

    vn2pnode = (memcache_vnumber2pnode*)args;
    if(-1 != vn2pnode->fd)
    {
        FileClose(vn2pnode->fd);
    }
    rfree(vn2pnode);
}

/* virtualnode 释放 */
static void memcache_virtualnode_free(memcache_virtualnode* vnode)
{
    if(NULL == vnode)
    {
        return;
    }

    dlist_free(vnode->nodes, memcache_vnumber2pnode_freefordlist);
}

/* pagenode 释放 */
static void memcache_pagenode_free(memcache_pagenode* mpnode)
{
    if(NULL == mpnode)
    {
        return;
    }

    if(NULL == mpnode->data)
    {
        return;
    }

    rfree(mpnode->data);
}

/* pages 释放 */
static void memcache_pages_free(memcache_pages* mpages)
{
    uint64 index = 0;
    if(NULL == mpages)
    {
        return;
    }

    dlist_free(mpages->pages, NULL);
    dlist_free(mpages->freepages, NULL);

    if(0 < mpages->pagecnt && NULL != mpages->pageaddr)
    {
        for(index = 0; index < mpages->pagecnt; index++)
        {
            memcache_pagenode_free(mpages->pageaddr + index);
        }
        rfree(mpages->pageaddr);
    }
    rfree(mpages);
}

/*
 * 初始化一个缓存
 *  name            唯一标识, 在进程内不要重复
 *  csize           总大小, 最小为16M, 最大为: 16G, 单位为 M
 *  blksize         每个缓存的页大小, 需要为 8192 的倍数,最小为 8192, 最大为 1M, 单位为字节
*/
memcache* memcache_init(char* name, int csize, int blksize)
{
    uint64_t index                      = 0;
    memcache* mcache                    = NULL;
    memcache_pagenode* pnode            = NULL;
    memcache_virtualnode* mcachevnode   = NULL;
    char mdir[MEMCACHE_DIRLEN]          = { 0 };

    if(MEMCACHE_BLOCKSIZE_MAX < blksize)
    {
        blksize = MEMCACHE_BLOCKSIZE_MAX;
    }

    if(MEMCACHE_BLOCKSIZE_DEFAULT > blksize)
    {
        blksize = MEMCACHE_BLOCKSIZE_DEFAULT;
    }

    if(MEMCACHE_DEFAULT > csize)
    {
        csize = MEMCACHE_DEFAULT;
    }

    if(MEMCACHE_MAXSIZE < csize)
    {
        csize = MEMCACHE_MAXSIZE;
    }

    /*--------------基础初始化 begin----------------------------------*/
    mcache = rmalloc0(sizeof(memcache));
    if(NULL == mcache)
    {
        elog(RLOG_WARNING, "mem_init error");
        return NULL;
    }
    rmemset0(mcache, 0, '\0', sizeof(memcache));
    mcache->name = rstrdup(name);
    mcache->blksize = blksize;
    mcache->maxsize = MEMCACHE_MB2BYTE(csize);

    /* 换算需要的块数 */
    mcache->blkcnt = mcache->maxsize/(uint64)mcache->blksize;

    /*--------------基础初始化   end----------------------------------*/

    /*--------------虚拟编号相关内容初始化 begin----------------------*/
    mcache->vnumber = 1;
    mcache->vnodemapcnt = MEMCACHE_VNODEMAPCNT;
    mcache->vnodemap = rmalloc0(mcache->vnodemapcnt*sizeof(memcache_virtualnode));
    if(NULL == mcache->vnodemap)
    {
        elog(RLOG_WARNING, "memcache init vnodemap error");
        return NULL;
    }
    
    /* 初始化设置 */
    for(index = 0; index < mcache->vnodemapcnt; index++)
    {
        mcachevnode = mcache->vnodemap + index;
        mcachevnode->nodes = NULL;
    }

    /*--------------虚拟编号相关内容初始化   end----------------------*/

    /*--------------页相关内容初始化 begin----------------------------*/
    mcache->pages = rmalloc0(sizeof(memcache_pages));
    if(NULL == mcache->pages)
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
    mcache->pages->pageaddr = (memcache_pagenode*)rmalloc0(mcache->pages->pagecnt*sizeof(memcache_pagenode));
    if(NULL == mcache->pages->pageaddr)
    {
        elog(RLOG_WARNING, "memcache init pageaddr error");
        return NULL;
    }

    /* 初始值设置 */
    for(index = 1; index <= mcache->pages->pagecnt; index++)
    {
        pnode = mcache->pages->pageaddr + index - 1;
        pnode->count = 0;
        pnode->data = NULL;
        pnode->no = index;
        pnode->vno = 0;
        pnode->dlnode = NULL;

        mcache->pages->freepages = dlist_put(mcache->pages->freepages, pnode);
        mcache->pages->pages = dlist_put(mcache->pages->pages, pnode);

        /* 指向自己 */
        pnode->dlnode = mcache->pages->pages->tail;
    }
    /*--------------页相关内容初始化   end----------------------------*/

    elog(RLOG_INFO, "init memcache:%s, size:%lu, blkcnt:%lu", name, mcache->maxsize, mcache->pages->pagecnt);

    /* 创建文件夹 */
    snprintf(mdir, MEMCACHE_DIRLEN, "%s/%s", MEMCACHE_DIR, name);
    MakeDir(mdir);
    return mcache;
}

/* 虚拟节点比较 */
static int memcache_vn2pnode_cmp(void* s1, void* s2)
{
    memcache_vnumber2pnode* v1 = NULL;
    memcache_vnumber2pnode* v2 = NULL;

    v1 = (memcache_vnumber2pnode*)s1;
    v2 = (memcache_vnumber2pnode*)s2;

    if(v1->vno == v2->vno)
    {
        return 0;
    }

    return 1;
}


/* 根据 虚拟编号 获取 虚拟节点 */
static bool memcache_getvn2pnodebyvnumber(memcache_virtualnode* vnode, uint64_t vnumber, memcache_vnumber2pnode** pvn2pnode)
{
    dlistnode* dlnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if(NULL == vnode || true == dlist_isnull(vnode->nodes))
    {
        elog(RLOG_WARNING, "get virtual number 2 page node by virtual number argument error");
        return false;
    }

    for(dlnode = vnode->nodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        vn2pnode = (memcache_vnumber2pnode*)dlnode->value;
        if(vnumber != vn2pnode->vno)
        {
            continue;
        }

        *pvn2pnode = vn2pnode;
        return true;
    }

    elog(RLOG_WARNING, "can not get virtual number 2 page node by virtual number:%lu", vnumber);
    return false;
}


/* 将虚拟页面置换到磁盘中,并设置标识页面不在内存中 */
static bool memcache_vnumber2pnodeswap(memcache* mcache, memcache_vnumber2pnode* vn2pnode, uint8* blk)
{
    char vfile[MEMCACHE_DIRLEN] = { 0 };

    if(-1 == vn2pnode->fd)
    {
        snprintf(vfile, MEMCACHE_DIRLEN, "%s/%s/%016lX", MEMCACHE_DIR, mcache->name, vn2pnode->vno);

        /* 打开文件 */
        vn2pnode->fd = BasicOpenFile(vfile, O_RDWR | O_CREAT | RIPPLE_BINARY);
        if(-1 == vn2pnode->fd)
        {
            elog(RLOG_WARNING, "open file %s error, %s", vfile, strerror(errno));
            return false;
        }
    }

    if(mcache->blksize != FilePWrite(vn2pnode->fd, (char*)blk, mcache->blksize, 0))
    {
        elog(RLOG_WARNING, "pwrite file %s/%s/%016lX error, %s",
                            MEMCACHE_DIR,
                            mcache->name,
                            vn2pnode->vno,
                            strerror(errno));
        return false;
    }

    vn2pnode->hasfile = true;
    vn2pnode->pno = 0;
    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_OUT;

    FileClose(vn2pnode->fd);
    vn2pnode->fd = -1;
    return true;
}

/* 将虚拟页面加载到内存中 */
static bool memcache_vnumber2pnodeload(memcache* mcache, memcache_vnumber2pnode* vn2pnode, uint8* blk)
{
    char vfile[MEMCACHE_DIRLEN] = { 0 };
    if(-1 == vn2pnode->fd)
    {
        /* 打开文件 */
        snprintf(vfile, MEMCACHE_DIRLEN, "%s/%s/%016lX", MEMCACHE_DIR, mcache->name, vn2pnode->vno);

        /* 打开文件 */
        vn2pnode->fd = BasicOpenFile(vfile, O_RDWR | RIPPLE_BINARY);
        if(-1 == vn2pnode->fd)
        {
            elog(RLOG_WARNING, "open file %s error, %s", vfile, strerror(errno));
            return false;
        }
    }

    if(mcache->blksize != FilePRead(vn2pnode->fd, (char*)blk, mcache->blksize, 0))
    {
        elog(RLOG_WARNING, "pread file %s/%s/%016lX error, %s",
                            MEMCACHE_DIR,
                            mcache->name,
                            vn2pnode->vno,
                            strerror(errno));
        return false;
    }

    FileClose(vn2pnode->fd);
    vn2pnode->fd = -1;
    return true;
}

/* 
 * 将一批页面置换到磁盘中
  */
static bool memcache_pageswap(memcache* mcache)
{
    int index = 0;
    uint64_t vnodeindex = 0;
    dlistnode* dlnode = NULL;
    memcache_pagenode* pnode = NULL;
    memcache_virtualnode* vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if(NULL == mcache)
    {
        return false;
    }

    /* 交换的个数 */
    for(dlnode = mcache->pages->pages->tail; NULL != dlnode; dlnode = dlnode->prev)
    {
        if(index >= mcache->pages->swapcnt)
        {
            break;
        }

        /* 将一个页面置换出去 */
        pnode = (memcache_pagenode*)dlnode->value;

        /* 正在外部被使用 */
        if(true == pnode->used)
        {
            continue;
        }

        /* 
         * 根据虚拟编号获取虚拟节点, 将页面写入到指定的文件中
         */
        /* 获取虚拟节点 */
        vnodeindex = MEMCACHE_VNUMBER2INDEX((pnode->vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
        vnode = mcache->vnodemap + vnodeindex;
        if(false == memcache_getvn2pnodebyvnumber(vnode, pnode->vno, &vn2pnode))
        {
            elog(RLOG_WARNING, "pageswap get virtual node by number error, %lu", pnode->vno);
            return false;
        }

        /* 将虚拟页面文件落盘 */
        if(false == memcache_vnumber2pnodeswap(mcache, vn2pnode, pnode->data))
        {
            elog(RLOG_WARNING, "pageswap vnumber2pnodeswap error, %lu", pnode->vno);
            return false;
        }

        /* 将文件块清空 */
        rmemset0(pnode->data, 0, '\0', mcache->blksize);
        pnode->vno = 0;

        mcache->pages->freepages = dlist_put(mcache->pages->freepages, pnode);
        index++;
    }
    return true;
}

/*
 * 获取一个可用的页面编号
*/
static memcache_pagenode* memcache_getfreepagenode(memcache* mcache)
{
    memcache_pages* mpages = NULL;
    memcache_pagenode* pagenode = NULL;

    if(NULL == mcache || NULL == mcache->pages)
    {
        return NULL;
    }

    mpages = mcache->pages;
    /* 首先在空闲页面中获取空闲页 */
    if(false == dlist_isnull(mpages->freepages))
    {
        goto memcache_getfreepageno_done;
    }

    /* 没有找到, 那么置换页面 */
    if(false == memcache_pageswap(mcache))
    {
        elog(RLOG_WARNING, "get free page node, pageswap error, ");
        return NULL;
    }

memcache_getfreepageno_done:

    /* 获取一个页号 */
    pagenode = dlist_getvalue(mpages->freepages);
    if(NULL == pagenode->data)
    {
        /* 申请空间 */
        pagenode->data = rmalloc0(mcache->blksize);
        if(NULL == pagenode->data)
        {
            elog(RLOG_WARNING, "get free page number, out of memory");
            return false;
        }
        rmemset0(pagenode->data, 0, '\0', mcache->blksize);
    }

    /* 将页转移到头部 */
    mpages->pages = dlist_putnode2head(mpages->pages, pagenode->dlnode);
    pagenode->dlnode = mpages->pages->head;

    return pagenode;
}

/* 获取一个可用的虚拟页， for debug */
bool memcache_getvnode(memcache* mcache, uint64* vno)
{
    /* 
     * 1、获取一个虚拟编号
     * 2、申请虚拟编号节点
     * 3、获取一个空闲页
     *      3.1 没有空闲页时
     *          3.1.1 将使用频繁最最低的页面置换出去,将置换出去的页面放入到空闲页面中
     *          3.1.2 将置换出去的页面上对应的虚拟页面上的标识设置为 OUT(不在内存中)
     *      3.2 在空闲页面中获取可用页面
     * 
     * 4、在虚拟编号节点上与页面关联, 设置虚拟页面上的标识为 MEM(在内存中)
     */
    uint64_t index = 0;
    memcache_pagenode* pagenode = NULL;
    memcache_virtualnode* vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if(NULL == mcache)
    {
        return false;
    }

    /* 获取新虚拟节点的索引 */
    index = MEMCACHE_VNUMBER2INDEX((mcache->vnumber - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    vn2pnode = memcache_vnumber2pnode_init();
    if(NULL == vn2pnode)
    {
        elog(RLOG_WARNING, "getvnode out of memory");
        return false;
    }
    vn2pnode->hasfile = false;
    vn2pnode->vno = mcache->vnumber;
    mcache->vnumber++;

    /* 获取一个可用的空闲页编号 */
    pagenode = memcache_getfreepagenode(mcache);
    if(NULL == pagenode)
    {
        elog(RLOG_WARNING, "get free page node error");
        return false;
    }

    /* 设置 pageno 对应的虚拟编号 */
    pagenode->vno = vn2pnode->vno;
    vn2pnode->pno = pagenode->no;
    vn2pnode->fd = -1;
    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_MEM;

    /* 将 vnode 加入到链表中 */
    vnode->nodes = dlist_put(vnode->nodes, vn2pnode);
    *vno = vn2pnode->vno;
    return true;
}

/* 根据虚拟号获取可用 page */
bool memcache_getpagebyvno(memcache* mcache, uint64 vno, uint8_t** pdata)
{
    /*
     * 根据虚拟编号获取页
     * 1、页在缓存中，直接返回
     * 2、页不在缓存中， 那么需要置换并加载到缓存中
     */
    uint64_t index = 0;
    memcache_pagenode* pnode = NULL;
    memcache_virtualnode* vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if(NULL == mcache)
    {
        elog(RLOG_WARNING, "getpagebyvno argument error");
        return false;
    }

    /* 获取新虚拟节点的索引 */
    index = MEMCACHE_VNUMBER2INDEX((vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    /* 获取虚拟页节点 */
    if(false == memcache_getvn2pnodebyvnumber(vnode, vno, &vn2pnode))
    {
        elog(RLOG_WARNING, "get virtual page error");
        return false;
    }

    /* 
     * 查看页是否在缓存中
     */
    if(MEMCACHE_VNUMBER2PNODE_FLAG_MEM == vn2pnode->flag)
    {
        /* 在缓存中, 获取页节点 */
        pnode = mcache->pages->pageaddr + (vn2pnode->pno - 1);
        goto memcache_getpagebyvno_done;
    }

    /* 
     * 不在缓存中
     * 1、 freepage 有空间, 获取一个空闲页
     * 2、 freepage 中没有空间，置换
     */
    if(true == dlist_isnull(mcache->pages->freepages))
    {
        /* 做空间置换 */
        if(false == memcache_pageswap(mcache))
        {
            elog(RLOG_WARNING, "getpagebyvno pageswap error");
            return false;
        }
    }

    /* 获取页节点 */
    pnode = dlist_getvalue(mcache->pages->freepages);
    pnode->vno = vn2pnode->vno;
    vn2pnode->pno = pnode->no;

    /* 查看是否需要在磁盘中加载空间 */
    if(0 > vn2pnode->fd)
    {
        /* 在磁盘中加载页 */
        if(false == memcache_vnumber2pnodeload(mcache, vn2pnode, pnode->data))
        {
            elog(RLOG_WARNING, "getpagebyvno load page error");
            return false;
        }

        /* 关闭描述符 */
        FileClose(vn2pnode->fd);
        vn2pnode->fd = -1;
    }

    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_MEM;
memcache_getpagebyvno_done:
    pnode->used = true;
    pnode->count++;
    *pdata = pnode->data;
    return true;
}

/* 获取一个可用的内存页 */
bool memcache_getpage(memcache* mcache, uint64* vno, uint8_t** pdata)
{
    /* 
     * 1、获取一个虚拟编号
     * 2、申请虚拟编号节点
     * 3、获取一个空闲页
     *      3.1 没有空闲页时
     *          3.1.1 将使用频繁最最低的页面置换出去,将置换出去的页面放入到空闲页面中
     *          3.1.2 将置换出去的页面上对应的虚拟页面上的标识设置为 OUT(不在内存中)
     *      3.2 在空闲页面中获取可用页面
     * 
     * 4、在虚拟编号节点上与页面关联, 设置虚拟页面上的标识为 MEM(在内存中)
     */
    uint64_t index = 0;
    memcache_pagenode* pnode = NULL;
    memcache_virtualnode* vnode = NULL;
    memcache_vnumber2pnode* vn2pnode = NULL;

    if(NULL == mcache)
    {
        return false;
    }

    /* 获取新虚拟节点的索引 */
    index = MEMCACHE_VNUMBER2INDEX((mcache->vnumber - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    vn2pnode = memcache_vnumber2pnode_init();
    if(NULL == vn2pnode)
    {
        elog(RLOG_WARNING, "getvnode out of memory");
        return false;
    }
    vn2pnode->hasfile = false;
    vn2pnode->vno = mcache->vnumber;
    mcache->vnumber++;

    /* 获取一个可用的空闲页编号 */
    pnode = memcache_getfreepagenode(mcache);
    if(NULL == pnode)
    {
        elog(RLOG_WARNING, "get free page node error");
        return false;
    }

    /* 设置 pageno 对应的虚拟编号 */
    pnode->vno = vn2pnode->vno;
    vn2pnode->pno = pnode->no;
    vn2pnode->fd = -1;
    vn2pnode->flag = MEMCACHE_VNUMBER2PNODE_FLAG_MEM;

    /* 将 vnode 加入到链表中 */
    vnode->nodes = dlist_put(vnode->nodes, vn2pnode);
    pnode->used = true;
    pnode->count++;
    
    /* 出参设置 */
    *vno = vn2pnode->vno;
    *pdata = pnode->data;
    return true;
}

/* 根据虚拟号释放 page */
void memcache_putpage(memcache* mcache, uint64 vno)
{
    uint64_t index                      = 0;
    memcache_pagenode* pnode            = NULL;
    memcache_virtualnode* vnode         = NULL;
    memcache_vnumber2pnode* vn2pnode    = NULL;
    char vfile[MEMCACHE_DIRLEN]         = { 0 };

    if(NULL == mcache || 0 == vno)
    {
        return;
    }

    /* 获取新虚拟节点的索引 */
    index = MEMCACHE_VNUMBER2INDEX((vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    /* 获取虚拟页节点 */
    if(false == memcache_getvn2pnodebyvnumber(vnode, vno, &vn2pnode))
    {
        return;
    }

    /* 
     * 不在缓存中
     */
    if(MEMCACHE_VNUMBER2PNODE_FLAG_OUT == vn2pnode->flag)
    {
        goto memcache_putvnode_done;
    }

    /* 在缓存中，将页节点设置为可用, 并放入到 freepages 中 */
    pnode = mcache->pages->pageaddr + (vn2pnode->pno - 1);
    pnode->vno = 0;
    pnode->used = false;
    pnode->count--;
    mcache->pages->freepages = dlist_put(mcache->pages->freepages, pnode);

memcache_putvnode_done:

    /* 清理文件 */
    if(true == vn2pnode->hasfile)
    {
        snprintf(vfile, MEMCACHE_DIRLEN, "%s/%s/%016lX", MEMCACHE_DIR, mcache->name, vn2pnode->vno);
        durable_unlink(vfile, RLOG_DEBUG);
    }

    /* 清理 vnode 节点 */
    vnode->nodes = dlist_deletebyvaluefirstmatch(vnode->nodes, vn2pnode, memcache_vn2pnode_cmp, memcache_vnumber2pnode_freefordlist);
    return;
}

/* 暂存数据后, 页可用于置换 */
void memcache_pagestore(memcache* mcache, uint64 vno)
{
    uint64_t index                      = 0;
    memcache_pagenode* pnode            = NULL;
    memcache_virtualnode* vnode         = NULL;
    memcache_vnumber2pnode* vn2pnode    = NULL;

    if(NULL == mcache || 0 == vno)
    {
        return;
    }

    /* 获取新虚拟节点的索引 */
    index = MEMCACHE_VNUMBER2INDEX((vno - 1), MEMCACHE_VNODEMAPMASK(mcache->vnodemapcnt));
    vnode = mcache->vnodemap + index;

    /* 获取虚拟页节点 */
    if(false == memcache_getvn2pnodebyvnumber(vnode, vno, &vn2pnode))
    {
        return;
    }

    /* 
     * 不在缓存中
     */
    if(MEMCACHE_VNUMBER2PNODE_FLAG_OUT == vn2pnode->flag)
    {
        return;
    }

    /* 在缓存中，将页节点设置为可用, 并放入到 freepages 中 */
    pnode = mcache->pages->pageaddr + (vn2pnode->pno - 1);

    /* 页可用于置换 */
    pnode->used = false;

    return;
}

/* 释放 */
void memcache_free(memcache* mcache)
{
    uint64 index = 0;
    char mdir[MEMCACHE_DIRLEN]          = { 0 };
    if(NULL == mcache)
    {
        return;
    }

    if(NULL != mcache->name)
    {
        rfree(mcache->name);
    }

    if(0 < mcache->vnodemapcnt && NULL != mcache->vnodemap)
    {
        for(index = 0; index < mcache->vnodemapcnt; index++)
        {
            memcache_virtualnode_free(mcache->vnodemap + index);
        }
        rfree(mcache->vnodemap);
    }

    memcache_pages_free(mcache->pages);

    /* 创建文件夹 */
    snprintf(mdir, MEMCACHE_DIRLEN, "%s/%s", MEMCACHE_DIR, mcache->name);
    RemoveDir(mdir);

    rfree(mcache);
}
