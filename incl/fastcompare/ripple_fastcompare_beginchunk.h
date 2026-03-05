#ifndef RIPPLE_FASTCOMPARE_BEGINCHUNK_H
#define RIPPLE_FASTCOMPARE_BEGINCHUNK_H

typedef struct RIPPLE_FASTCOMPARE_BEGINCHUNK
{
    ripple_fastcompare_chunk    base;
    uint8                       flag;
    List                       *minprivalue;
    List                       *maxprivalue;
} ripple_fastcompare_beginchunk;

extern ripple_fastcompare_beginchunk *ripple_fastcompare_beginchunk_init(void);
extern uint8 *ripple_fastcompare_beginchunk_serial(ripple_fastcompare_beginchunk *beginchunk, uint32 *size);
extern void ripple_fastcompare_beginchunk_clean(ripple_fastcompare_beginchunk *chunk);
extern bool ripple_fastcompare_beginchunk_send(void* netclient, ripple_fastcompare_beginchunk *beginchunk);
extern ripple_fastcompare_beginchunk* ripple_fastcompare_beginchunk_fetchdata(void* privdata, uint8* buffer);
#endif
