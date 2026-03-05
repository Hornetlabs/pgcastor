#ifndef RIPPLE_FASTCOMPARE_COLUMNDEFINE_H
#define RIPPLE_FASTCOMPARE_COLUMNDEFINE_H


typedef struct RIPPLE_FASTCOMPARE_COLUMNDEFINE
{
    uint32  colid;
    char   *colname;
} ripple_fastcompare_columndefine;

extern ripple_fastcompare_columndefine *ripple_fastcompare_columndefine_init(void);
extern void ripple_fastcompare_columndefine_list_clean(List *col_list);
extern void ripple_fastcompare_columndefine_clean(ripple_fastcompare_columndefine *col);

#endif
