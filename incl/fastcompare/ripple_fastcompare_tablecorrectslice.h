#ifndef RIPPLE_FASTCOMPARE_TABLECORRECTSLICE_H
#define RIPPLE_FASTCOMPARE_TABLECORRECTSLICE_H

typedef struct RIPPLE_FASTCOMPARE_TABLECORRECTSLICE
{
    uint16                                  sliceflag;       /* 标记分片的位置 */
    uint16                                  chunkflag;
    uint16                                  prikeycnt;
    uint32                                  no;             /* 分片编号 */
    char                                    *condition;
    HTAB*                                   compresult;
    ripple_fastcompare_beginslice*          beginslice;
    ripple_fastcompare_beginchunk*          beginchunk;
    ripple_fastcompare_simpledatachunk*     dstchunk;         /* 目标端simpledatachunk */
} ripple_fastcompare_tablecorrectslice;

extern ripple_fastcompare_tablecorrectslice *ripple_fastcompare_tablecorrectslice_init(void);

extern void ripple_fastcompare_tablecorrectslice_set_dstchunk(ripple_fastcompare_tablecorrectslice* correctslice, ripple_fastcompare_simpledatachunk* dstchunk);

extern void ripple_fastcompare_tablecorrectslice_free(ripple_fastcompare_tablecorrectslice* correctslice);

#endif