#ifndef RIPPLE_FASTCOMPARE_DATACOMPARE_H
#define RIPPLE_FASTCOMPARE_DATACOMPARE_H

typedef struct RIPPLE_FASTCOMPARE_DATACOMPARE
{
    ripple_fastcompare_datacmpresult   *result;
    ripple_fastcompare_simpledatachunk *refchunk;
    ripple_fastcompare_simpledatachunk *corrchunk;
} ripple_fastcompare_datacompare;

extern ripple_fastcompare_datacompare *ripple_fastcompare_init_datacompare(void);
extern bool ripple_fastcompare_compare_simple_chunk(ripple_fastcompare_datacompare *cmp);
extern void ripple_fastcompare_datacompare_set_chunk(ripple_fastcompare_datacompare *cmp, 
                                                     ripple_fastcompare_simpledatachunk *refchunk,
                                                     ripple_fastcompare_simpledatachunk *corrchunk);
extern void ripple_fastcompare_datacompare_free(ripple_fastcompare_datacompare *cmp);

#endif
