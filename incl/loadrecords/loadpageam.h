#ifndef _RIPPLE_LOADPAGEAM_H_
#define _RIPPLE_LOADPAGEAM_H_

typedef struct RIPPLE_LOADPAGEROUTINE
{
    /* 初始化 */
    ripple_loadpage* (*loadpageinit)(void);

    /* 设置数据源 */
    bool (*loadpagesetfilesource)(ripple_loadpage* loadpage, char* fsource);

    /* 设置类型, WAL/TRAIL */
    void (*loadpagesettype)(ripple_loadpage* loadpage, int type);

    /* 设置加载的起点 */
    void (*loadpagesetstartpos)(ripple_loadpage* loadpage, ripple_recpos pos);

    /* 关闭文件描述符 */
    void (*loadpageclose)(ripple_loadpage* loadpage);

    /* 加载页面 */
    bool (*loadpage)(ripple_loadpage* loadpage, mpage* mp);

    /* 内存释放 */
    void (*loadpagefree)(ripple_loadpage* loadpage);
} ripple_loadpageroutine;

/* 获取 getpage 信息 */
ripple_loadpageroutine* ripple_loadpage_getpageroutine(int type);

#endif
