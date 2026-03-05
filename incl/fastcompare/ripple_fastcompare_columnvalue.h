#ifndef RIPPLE_FASTCOMPARE_COLUMNVALUE_H
#define RIPPLE_FASTCOMPARE_COLUMNVALUE_H

typedef struct RIPPLE_FASTCOMPARE_COLUMNVALUE
{
    uint32  len;
    uint32  type;
    uint16  flag;
    uint32  colid;
    char   *value;
} ripple_fastcompare_columnvalue;

extern uint32 ripple_fastcompare_columnvalue_list_crc(List *column_list);
extern void ripple_fastcompare_columnvalue_list_clean(List *list);
extern void ripple_fastcompare_columnvalue_clean(ripple_fastcompare_columnvalue *col);
extern ripple_fastcompare_columnvalue *ripple_fastcompare_columnvalue_init(void);
#endif
