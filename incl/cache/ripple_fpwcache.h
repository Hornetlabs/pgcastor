#ifndef RIPPLE_FPWCACHE_H
#define RIPPLE_FPWCACHE_H

typedef struct REORDERBUFFERFPWKEY
{
    uint16_t    itemoffset;
    uint32_t    blcknum;
    Oid         relfilenode;
} ReorderBufferFPWKey;

typedef struct REORDERBUFFERFPWENTRY
{
    uint16_t    itemoffset;
    uint32_t    blcknum;
    Oid         relfilenode;
    XLogRecPtr  lsn;
    uint32_t    len;
    void       *data;
} ReorderBufferFPWEntry;

/* list */
typedef struct ReorderBufferFPWNode
{
    ReorderBufferFPWKey     key;
    XLogRecPtr              lsn;
}ReorderBufferFPWNode;


extern HTAB *ripple_fpwcache_init(ripple_transcache *transcache);
extern void ripple_fpwcache_add(ripple_transcache *transcache,
                                ReorderBufferFPWKey *key,
                                ReorderBufferFPWEntry *entry);

extern void ripple_fpwcache_calcredolsnbyrestartlsn(ripple_transcache *transcache,
                                            XLogRecPtr restartlsn,
                                            XLogRecPtr* redolsn);

#endif
