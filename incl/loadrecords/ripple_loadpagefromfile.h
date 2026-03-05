#ifndef _RIPPLE_LOADPAGEFROMFILE_H_
#define _RIPPLE_LOADPAGEFROMFILE_H_

typedef enum RIPPLE_LOADPAGEFROMFILE_TYPE
{
    RIPPLE_LOADPAGEFROMFILE_TYPE_NOP = 0X00,
    RIPPLE_LOADPAGEFROMFILE_TYPE_TRAIL,
    RIPPLE_LOADPAGEFROMFILE_TYPE_WAL
} ripple_loadpagefromfile_type;

typedef struct RIPPLE_LOADPAGEFROMFILE
{
    ripple_loadpage             loadpage;
    int                         filetype;
    int                         fd;
    uint64                      foffset;
    uint64                      fileno;
    char                        fdir[RIPPLE_MAXPATH];
    char                        fpath[RIPPLE_ABSPATH];
} ripple_loadpagefromfile;

/* 初始化 */
ripple_loadpage* ripple_loadpagefromfile_init(void);


/* 设置文件的存储路径 */
bool ripple_loadpagefromfile_setfdir(ripple_loadpage* loadpage, char* fsource);

/* 设置类型 */
void ripple_loadpagefromfile_settype(ripple_loadpage* loadpage, int type);

/* 设置解析的起点 */
void ripple_loadpagefromfile_setstartpos(ripple_loadpage* loadpage, ripple_recpos pos);

/* 加载页 */
bool ripple_loadpagefromfile_loadpage(ripple_loadpage* loadpage, mpage* mp);

/* 关闭文件描述符 */
void ripple_loadpagefromfile_close(ripple_loadpage* loadpage);

/* 释放 */
void ripple_loadpagefromfile_free(ripple_loadpage* loadpage);

#endif
