#ifndef _LOADPAGEAM_H_
#define _LOADPAGEAM_H_

typedef struct LOADPAGEROUTINE
{
    /* 初始化 */
    loadpage* (*loadpageinit)(void);

    /* 设置数据源 */
    bool (*loadpagesetfilesource)(loadpage* loadpage, char* fsource);

    /* 设置类型, WAL/TRAIL */
    void (*loadpagesettype)(loadpage* loadpage, int type);

    /* 设置加载的起点 */
    void (*loadpagesetstartpos)(loadpage* loadpage, recpos pos);

    /* 关闭文件描述符 */
    void (*loadpageclose)(loadpage* loadpage);

    /* 加载页面 */
    bool (*loadpage)(loadpage* loadpage, mpage* mp);

    /* 内存释放 */
    void (*loadpagefree)(loadpage* loadpage);
} loadpageroutine;

/* 获取 getpage 信息 */
loadpageroutine* loadpage_getpageroutine(int type);

#endif
