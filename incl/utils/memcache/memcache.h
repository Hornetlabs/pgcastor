#ifndef _MEMCACHE_H_
#define _MEMCACHE_H_

#define MEMCACHE_BLOCKSIZE_DEFAULT 8192

/* 1M */
#define MEMCACHE_BLOCKSIZE_MAX 1048576

/* 16M */
#define MEMCACHE_DEFAULT 16
#define MEMCACHE_MAXSIZE 1048576

#define MEMCACHE_KB2BYTE(kbytes) ((uint64_t)kbytes * (uint64)1024)
#define MEMCACHE_MB2BYTE(mbytes) (((uint64)MEMCACHE_KB2BYTE(mbytes)) * (uint64)1024)

#define MEMCACHE_VNODEMAPCNT 1024
#define MEMCACHE_VNODEMAPMASK(cnt) ((uint64)cnt - (uint64)1)
#define MEMCACHE_VNUMBER2INDEX(vnumber, mask) ((uint64)vnumber & (uint64)mask)

/* cachedirectory */
#define MEMCACHE_DIRLEN 256
#define MEMCACHE_DIR "memcache"

/* send pagesurfacesetswitchwhen setswitch unit */
#define MEMCACHE_SWAPBATCHCNT 5

/* temporarilynot muchthread whencall */

/* pagesurfaceup info */
typedef struct MEMCACHE_PAGENODE
{
    /* used foridentifierwhether atis use handswitchwhenifas true skip unitpagesurface */
    bool used;

    /* device */
    uint64_t count;

    /* number begin */
    uint64_t no;

    dlistnode* dlnode;

    /* hasthe page virtualnumber asinvalid */
    uint64_t vno;

    /* data */
    uint8_t* data;
} memcache_pagenode;

typedef struct MEMCACHE_PAGES
{
    /* unitpage size */
    int blksize;

    /* pagesetswitch setswitch page */
    int swapcnt;

    /* piece count */
    uint64_t pagecnt;

    /* unitsectionpointas memcache_pagenode used forpagesetswitchwhen use */
    dlist* pages;

    /* emptyidle page getemptyidlepagesurface */
    dlist* freepages;

    /* page addr, contentas memcache_pagenode */
    memcache_pagenode* pageaddr;
} memcache_pages;

/* virtualnumberandfile mapping */
typedef enum MEMCACHE_VNUMBER2PNODE_FLAG
{
    MEMCACHE_VNUMBER2PNODE_FLAG_NOP = 0x00,

    /* atmemoryin */
    MEMCACHE_VNUMBER2PNODE_FLAG_MEM,

    /* issetswitchoutgo */
    MEMCACHE_VNUMBER2PNODE_FLAG_OUT

} memcache_vnumber2pnode_flag;

typedef struct MEMCACHE_VNUMBER2PNODE
{
    /* diskincontainhasfile */
    bool hasfile;

    /* toolbodycontainmeaningcheckseeupsurface meaning */
    uint8_t flag;

    /* virtualnumberfile issetswitchoutgowhen thisvaluewill memorywritewriteto fd specify filein
     * deletevirtualsectionpointwhen needclosethisdescriptor */
    int fd;

    /* virtualnumber */
    uint64_t vno;

    /* pagesurfacenumber,whenpagesurfacenotatmemoryinwhen,thisvalueas 0 */
    uint64_t pno;
} memcache_vnumber2pnode;

typedef struct MEMCACHE_VIRTUALNODE
{
    /* typeas memcache_vnumber2pnode */
    dlist* nodes;
} memcache_virtualnode;

typedef struct MEMCACHE
{
    /* name, used foridentifieruniqueone */
    char* name;

    /* unitpage size */
    int blksize;

    /* piece count */
    uint64_t blkcnt;

    /* virtualnumber hash */
    uint64_t vnodemapcnt;

    /* virtualnumber begin */
    uint64_t vnumber;

    /* size needas blocksize */
    uint64_t maxsize;

    /* virtualnumbersectionpointandcachepagemapping */
    memcache_virtualnode* vnodemap;

    /* pagenumbercache */
    memcache_pages* pages;
} memcache;

/* initializeoneunitcache name uniqueoneidentifier atprocessinsidenotneedheavycomplex csize
 * totalsize most as16M mostlargeas positionas blksize unitcache pagesize needas most as mostlargeas
 * positionascharsection */
memcache* memcache_init(char* name, int csize, int blksize);

/* getoneunitavailable pagenumber, for debug */
bool memcache_getvnode(memcache* mcache, uint64* vno);

/* according tovirtualnumbergetavailable page */
bool memcache_getpagebyvno(memcache* mcache, uint64 vno, uint8_t** pdata);

/* getoneunitavailable memorypage */
bool memcache_getpage(memcache* mcache, uint64* vno, uint8_t** pdata);

/* according tovirtualnumberfree page */
void memcache_putpage(memcache* mcache, uint64 vno);

/* temporarilystoredataback pageavailable setswitch */
void memcache_pagestore(memcache* mcache, uint64 vno);

/* free */
void memcache_free(memcache* mcache);

#endif
