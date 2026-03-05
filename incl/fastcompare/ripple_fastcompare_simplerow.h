#ifndef RIPPLE_FASTCOMPARE_SIMPLEROW_H
#define RIPPLE_FASTCOMPARE_SIMPLEROW_H

typedef struct RIPPLE_FASTCOMPARE_SIMPLEROW
{
    uint32  crc;        /* 行crc计算结果 */
    List   *privalues;  /* ripple_fastcompare_simplecolumnvalue */
} ripple_fastcompare_simplerow;

extern ripple_fastcompare_simplerow *ripple_fastcompare_simplerow_init(void);
void ripple_fastcompare_simplerow_list_clean(List *row_list);
extern void ripple_fastcompare_simplerow_clean(ripple_fastcompare_simplerow *row);

extern void ripple_fastcompare_simplerow_crcComp(ripple_fastcompare_simplerow *row,
                                          void *data,
                                          uint32 len);
extern void ripple_fastcompare_simplerow_crcFin(ripple_fastcompare_simplerow *row);

#endif
