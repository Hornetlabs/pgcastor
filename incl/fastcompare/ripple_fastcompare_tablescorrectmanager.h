#ifndef RIPPLE_FASTCOMPARE_TABLESCORRECTMANAGER_H
#define RIPPLE_FASTCOMPARE_TABLESCORRECTMANAGER_H

typedef struct RIPPLE_FASTCOMPARE_TABLESCORRECTMANAGER
{
    ripple_netserver            base;
    ripple_fastcompare_tablecomparecatalog  *catalog;
} ripple_fastcompare_tablescorrectmanager;

ripple_fastcompare_tablescorrectmanager* ripple_fastcompare_tablescorrectmanager_init(void);

void ripple_fastcompare_tablescorrectmanager_free(ripple_fastcompare_tablescorrectmanager* tablecorrect);

#endif
