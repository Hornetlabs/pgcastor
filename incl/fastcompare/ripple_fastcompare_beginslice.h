#ifndef RIPPLE_FASTCOMPARE_BEGINSLICE_H
#define RIPPLE_FASTCOMPARE_BEGINSLICE_H

typedef struct RIPPLE_FASTCOMPARE_BEGINSLICE
{
    ripple_fastcompare_chunk        base;
    uint8                           flag;
    uint32                          num;
    char                           *schema;
    char                           *table;
    char                           *condition;
    List                           *columns;
} ripple_fastcompare_beginslice;

extern ripple_fastcompare_beginslice *ripple_fastcompare_beginslice_init(void);
extern void ripple_fastcompare_beginslice_clean(ripple_fastcompare_beginslice *beginslice);
extern uint8 *ripple_fastcompare_beginslice_serial(ripple_fastcompare_beginslice *beginslice, uint32 *size);
extern bool ripple_fastcompare_beginslice_send(void* netclient, ripple_fastcompare_beginslice *beginslice);
extern ripple_fastcompare_beginslice* ripple_fastcompare_beginslice_fetchdata(void* privdata, uint8* buffer);

#endif
