#ifndef _RIPPLE_REBUILD_PREPARESTMT_H
#define _RIPPLE_REBUILD_PREPARESTMT_H

typedef struct RIPPLE_REBUILD_PREPARESTMT
{
    uint64                          number;                                 /* prepareno        */
    char                            *preparesql;                            /* 预解析语句  key   */
    char                            stmtname[RIPPLE_NAMEDATALEN];           /* stmtname   value */
}ripple_rebuild_preparestmt;

ripple_rebuild_preparestmt* ripple_rebuild_preparestmt_init(void);

int ripple_rebuild_preparestmt_cmp(void* v1, void* v2);

void ripple_rebuild_preparestmt_debug(void* v1);

void ripple_rebuild_preparestmt_free(void* argv);

#endif
