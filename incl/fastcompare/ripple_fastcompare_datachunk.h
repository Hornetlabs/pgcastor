#ifndef RIPPLE_FASTCOMPARE_DATACHUNK_H
#define RIPPLE_FASTCOMPARE_DATACHUNK_H

typedef struct RIPPLE_FASTCOMPARE_DATACHUNK
{
    ripple_fastcompare_chunk    base;
    List                       *rows;   /* 存储列值, ripple_fastcompare_row */
} ripple_fastcompare_datachunk;

extern ripple_fastcompare_datachunk *ripple_fastcompare_datachunk_init(void);
extern void ripple_fastcompare_datachunk_clean(ripple_fastcompare_datachunk *datachunk);
extern uint8 *ripple_fastcompare_datachunk_serial(ripple_fastcompare_datachunk *chunk,
                                                 uint32 *size);
extern ripple_fastcompare_datachunk *ripple_fastcompare_datachunk_deserial(uint8 *chunk);
extern bool ripple_fastcompare_datachunk_send(void* netclient_in, ripple_fastcompare_datachunk *datachunk);
extern ripple_fastcompare_datachunk* ripple_fastcompare_datachunk_s2dcorrectdata_fetchdata(void* privdata, uint8* buffer);
extern uint8* ripple_fastcompare_datachunk_d2scorrectdata_build(void* privdata, uint32* totallen);
#endif
