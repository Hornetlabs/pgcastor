#ifndef RIPPLE_FASTCOMPARE_ROW_H
#define RIPPLE_FASTCOMPARE_ROW_H

typedef enum RIPPLE_FASTCOMPARE_ROW_OP
{
    RIPPLE_FASTCOMPARE_ROW_OP_INVALID = 0x00,
    RIPPLE_FASTCOMPARE_ROW_OP_INSERT,
    RIPPLE_FASTCOMPARE_ROW_OP_UPDATE,
    RIPPLE_FASTCOMPARE_ROW_OP_DELETE
} RIPPLE_FASTCOMPARE_ROW_OP;

typedef struct RIPPLE_FASTCOMPARE_ROW
{
    uint32  cnt;
    uint8   op;
    ripple_fastcompare_columnvalue *column;
} ripple_fastcompare_row;

extern ripple_fastcompare_row *ripple_fastcompare_row_init(void);
extern void ripple_fastcompare_row_column_init(ripple_fastcompare_row *row, int cnt);
extern void ripple_fastcompare_row_lsit_clean(List *row_list);
extern void ripple_fastcompare_row_clean(ripple_fastcompare_row *row);

#endif
