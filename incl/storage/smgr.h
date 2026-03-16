#ifndef _RIPPLE_SMGR_H
#define _RIPPLE_SMGR_H

typedef struct RIPPLE_SMGR_IF
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
} ripple_smgr_if;

typedef struct RIPPLE_SMGR_STATE
{
    int                     bufid;
    int                     fileid;
    ripple_smgr_if*         smgr;
} ripple_smgr_state;


#endif
