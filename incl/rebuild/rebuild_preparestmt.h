#ifndef _REBUILD_PREPARESTMT_H
#define _REBUILD_PREPARESTMT_H

typedef struct REBUILD_PREPARESTMT
{
    uint64 number;                /* prepareno        */
    char*  preparesql;            /* Pre-parsed statement key   */
    char   stmtname[NAMEDATALEN]; /* stmtname   value */
} rebuild_preparestmt;

rebuild_preparestmt* rebuild_preparestmt_init(void);

int rebuild_preparestmt_cmp(void* v1, void* v2);

void rebuild_preparestmt_debug(void* v1);

void rebuild_preparestmt_free(void* argv);

#endif
