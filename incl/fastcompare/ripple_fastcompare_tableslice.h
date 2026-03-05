#ifndef RIPPLE_FASTCOMPARE_TABLESLICE_H
#define RIPPLE_FASTCOMPARE_TABLESLICE_H

/* slice位置标记 */
#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_NORMAL  0x00
#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_FIRST   0x01
#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_LAST    0x02

#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_SET_FIRST(flag)     (flag |= RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_FIRST)
#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_SET_LAST(flag)      (flag |= RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_LAST)

#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_FIRST(flag)     (flag & RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_FIRST)
#define RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_IS_LAST(flag)      (flag & RIPPLE_FASTCOMPARE_TABLESLICE_SLICE_LAST)


typedef struct RIPPLE_FASTCOMPARE_TABLESLICE
{
    Oid     oid;        /* 表oid */
    uint8   flag;       /* 标记分片的位置 */
    uint32  no;         /* 分片编号 */
    char   *condition;  /* 分片条件 */
} ripple_fastcompare_tableslice;

extern ripple_fastcompare_tableslice *ripple_fastcompare_tableslice_init(void);

extern void ripple_fastcompare_tableslice_Slice2Chunk(ripple_fastcompare_tableslicetask *task,
                                                      ripple_fastcompare_tableslice *slice);
#endif
