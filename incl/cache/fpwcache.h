#ifndef FPWCACHE_H
#define FPWCACHE_H

typedef struct REORDERBUFFERFPWKEY
{
    uint16_t itemoffset;
    uint32_t blcknum;
    Oid      relfilenode;
} ReorderBufferFPWKey;

typedef struct REORDERBUFFERFPWENTRY
{
    uint16_t   itemoffset;
    uint32_t   blcknum;
    Oid        relfilenode;
    XLogRecPtr lsn;
    uint32_t   len;
    void*      data;
} ReorderBufferFPWEntry;

/* list */
typedef struct ReorderBufferFPWNode
{
    ReorderBufferFPWKey key;
    XLogRecPtr          lsn;
} ReorderBufferFPWNode;

extern HTAB* fpwcache_init(transcache* transcache);
extern void fpwcache_add(transcache*            transcache,
                         ReorderBufferFPWKey*   key,
                         ReorderBufferFPWEntry* entry);

extern void fpwcache_calcredolsnbyrestartlsn(transcache* transcache,
                                             XLogRecPtr  restartlsn,
                                             XLogRecPtr* redolsn);

#endif
