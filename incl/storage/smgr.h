#ifndef _SMGR_H
#define _SMGR_H

typedef struct SMGR_IF
{
    /* Open */
    bool (*smgr_open)(void* state);

    /* Close */
    bool (*smgr_close)(void* state);

    /* Flush */
    bool (*smgr_flush)(void* state);

    /* Write */
    bool (*smgr_write)(void* state);

    /* Read */
    bool (*smgr_read)(void* state);

    /* Delete */
    bool (*smgr_unlink)(void* state);
} smgr_if;

typedef struct SMGR_STATE
{
    int      bufid;
    int      fileid;
    smgr_if* smgr;
} smgr_state;

#endif
