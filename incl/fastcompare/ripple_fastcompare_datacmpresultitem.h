#ifndef RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_H
#define RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_H

typedef enum RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP
{
    RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INVALID = 0x00,
    RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_INSERT,
    RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_UPDATE,
    RIPPLE_FASTCOMPARE_DATACMPRESULTITEM_OP_DELETE
} ripple_fastcompare_datacmpresultitem_op;

typedef struct RIPPLE_FASTCOMPARE_DATACMPRESULTITEM
{
    uint8   op;
    List   *privalues;  /* 引用 DataChunk 中的 privalues,Datachunk中置空 */
} ripple_fastcompare_datacmpresultitem;

extern ripple_fastcompare_datacmpresultitem *ripple_fastcompare_datacmpresultitem_init(int op);

extern void ripple_fastcompare_datacmpresultitem_free(ripple_fastcompare_datacmpresultitem *resultitem);

#endif
