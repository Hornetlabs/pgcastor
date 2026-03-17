#ifndef _SMGR_H
#define _SMGR_H

typedef struct SMGR_IF
{
    /* 打开 */
    bool (*smgr_open)(void* state);

    /* 关闭 */
    bool (*smgr_close)(void* state);

    /* 刷新 */
    bool (*smgr_flush)(void* state);

    /* 写 */
    bool (*smgr_write)(void* state);

    /* 读 */
    bool (*smgr_read)(void* state);

    /* 删除 */
    bool (*smgr_unlink)(void* state);
} smgr_if;

typedef struct SMGR_STATE
{
    int                     bufid;
    int                     fileid;
    smgr_if*         smgr;
} smgr_state;


#endif
