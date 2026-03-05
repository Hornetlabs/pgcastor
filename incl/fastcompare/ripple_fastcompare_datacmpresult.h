#ifndef RIPPLE_FASTCOMPARE_DATACMPRESULT_H
#define RIPPLE_FASTCOMPARE_DATACMPRESULT_H

typedef struct RIPPLE_FASTCOMPARE_DATACMPRESULT
{
    ripple_fastcompare_chunk   base;
    List                      *checkresult; /* 回查源端处理的数据(I/U)*/
    List                      *corrresult;  /* 纠错后在目标端删除的数据 */
} ripple_fastcompare_datacmpresult;


typedef struct RIPPLE_FASTCOMPARE_COMPARERESULTHASHENTRY
{
    uint32  crc;        /* privalues计算而来 */
    List   *privalues;  /* ripple_fastcompare_datacmpresultitem */
} ripple_fastcompare_compareresulthashentry;

extern ripple_fastcompare_datacmpresult *ripple_fastcompare_datacmpresult_init(void);

extern void ripple_fastcompare_datacmpresult_free(ripple_fastcompare_datacmpresult *cmpresult);

extern bool ripple_fastcompare_datacmpresult_hashisnull(HTAB *result);

extern void ripple_fastcompare_datacmpresult_hash_free(HTAB *result);

#endif
