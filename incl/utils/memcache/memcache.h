#ifndef _MEMCACHE_H_
#define _MEMCACHE_H_

#define                 MEMCACHE_BLOCKSIZE_DEFAULT                      8192

/* 1M */
#define                 MEMCACHE_BLOCKSIZE_MAX                          1048576

/* 16M */
#define                 MEMCACHE_DEFAULT                                16
#define                 MEMCACHE_MAXSIZE                                1048576

#define                 MEMCACHE_KB2BYTE(kbytes)                        ((uint64_t)kbytes*(uint64)1024)
#define                 MEMCACHE_MB2BYTE(mbytes)                        (((uint64)MEMCACHE_KB2BYTE(mbytes))*(uint64)1024)

#define                 MEMCACHE_VNODEMAPCNT                            1024
#define                 MEMCACHE_VNODEMAPMASK(cnt)                      ((uint64)cnt - (uint64)1)
#define                 MEMCACHE_VNUMBER2INDEX(vnumber, mask)           ((uint64)vnumber&(uint64)mask)


/* 缓存目录 */
#define                 MEMCACHE_DIRLEN                                 256
#define                 MEMCACHE_DIR                                    "memcache"

/* 发生页面置换时, 置换的个数 */
#define                 MEMCACHE_SWAPBATCHCNT                           5

/* 
 * 暂不支持多线程同时调用
 */

/* 页面上的基础信息 */
typedef struct MEMCACHE_PAGENODE
{
    /* 用于标识是否正在被使用, 交换时若为 true，那么跳过这个页面 */
    bool                        used;

    /* 计数器 */
    uint64_t                    count;

    /* 编号,从 1 开始 */
    uint64_t                    no;

    dlistnode*                  dlnode;

    /* 所持有该 page 的虚拟编号, 0 为无效的 */
    uint64_t                    vno;

    /* 数据 */
    uint8_t*                    data;
} memcache_pagenode;

typedef struct MEMCACHE_PAGES
{
    /* 每个页的大小 */
    int                                 blksize;

    /* 每次页置换,置换的页数 */
    int                                 swapcnt;

    /* 块的数量 */
    uint64_t                            pagecnt;

    /* 每个节点为: memcache_pagenode, 用于页置换时使用 */
    dlist*                              pages;

    /* 空闲的页,快速获取空闲页面 */
    dlist*                              freepages;

    /* page addr, 内容为 memcache_pagenode */
    memcache_pagenode*                  pageaddr;
} memcache_pages;


/* 虚拟编号与文件的映射 */
typedef enum MEMCACHE_VNUMBER2PNODE_FLAG
{
    MEMCACHE_VNUMBER2PNODE_FLAG_NOP             = 0x00,

    /* 在内存中 */
    MEMCACHE_VNUMBER2PNODE_FLAG_MEM             ,

    /* 被置换出去了 */
    MEMCACHE_VNUMBER2PNODE_FLAG_OUT             

} memcache_vnumber2pnode_flag;

typedef struct MEMCACHE_VNUMBER2PNODE
{
    /* 在磁盘中含有文件 */
    bool                                    hasfile;

    /* 具体含义查看上面的定义       */
    uint8_t                                 flag;

    /* 
     * 虚拟编号文件,
     * 被置换出去时, 此值会 > 0, 内存写写入到 fd 指向的文件中
     * 删除虚拟节点时,需要关闭此描述符
     */
    int                                     fd;

    /* 虚拟编号                     */
    uint64_t                                vno;

    /* 页面编号,当页面不在内存中时,此值为 0 */
    uint64_t                                pno;
} memcache_vnumber2pnode;

typedef struct MEMCACHE_VIRTUALNODE
{
    /* 类型为 memcache_vnumber2pnode */
    dlist*                                  nodes;
} memcache_virtualnode;

typedef struct MEMCACHE
{
    /* 名称, 用于标识唯一 */
    char*                                   name;

    /* 每个页的大小 */
    int                                     blksize;

    /* 块的数量 */
    uint64_t                                blkcnt;

    /* 虚拟编号的hash数 */
    uint64_t                                vnodemapcnt;

    /* 虚拟编号, 从 1 开始 */
    uint64_t                                vnumber;

    /* 大小, 需要为 blocksize 的倍数 */
    uint64_t                                maxsize;

    /* 虚拟编号节点与缓存页映射 */
    memcache_virtualnode*                   vnodemap;

    /* 页编号缓存 */
    memcache_pages*                         pages;
} memcache;

/*
 * 初始化一个缓存
 *  name            唯一标识, 在进程内不要重复
 *  csize           总大小, 最小为16M, 最大为: 16G, 单位为 M
 *  blksize         每个缓存的页大小, 需要为 8192 的倍数,最小为 8192, 最大为 1M, 单位为字节
*/
memcache* memcache_init(char* name, int csize, int blksize);

/* 获取一个可用的页编号, for debug */
bool memcache_getvnode(memcache* mcache, uint64* vno);

/* 根据虚拟号获取可用 page */
bool memcache_getpagebyvno(memcache* mcache, uint64 vno, uint8_t** pdata);

/* 获取一个可用的内存页 */
bool memcache_getpage(memcache* mcache, uint64* vno, uint8_t** pdata);

/* 根据虚拟号释放 page */
void memcache_putpage(memcache* mcache, uint64 vno);

/* 暂存数据后, 页可用于置换 */
void memcache_pagestore(memcache* mcache, uint64 vno);

/* 释放 */
void memcache_free(memcache* mcache);

#endif
