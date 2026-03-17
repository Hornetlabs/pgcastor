#ifndef _LOADPAGEFROMFILE_H_
#define _LOADPAGEFROMFILE_H_

typedef enum LOADPAGEFROMFILE_TYPE
{
    LOADPAGEFROMFILE_TYPE_NOP = 0X00,
    LOADPAGEFROMFILE_TYPE_TRAIL,
    LOADPAGEFROMFILE_TYPE_WAL
} loadpagefromfile_type;

typedef struct LOADPAGEFROMFILE
{
    loadpage             loadpage;
    int                         filetype;
    int                         fd;
    uint64                      foffset;
    uint64                      fileno;
    char                        fdir[MAXPATH];
    char                        fpath[ABSPATH];
} loadpagefromfile;

/* 初始化 */
loadpage* loadpagefromfile_init(void);


/* 设置文件的存储路径 */
bool loadpagefromfile_setfdir(loadpage* loadpage, char* fsource);

/* 设置类型 */
void loadpagefromfile_settype(loadpage* loadpage, int type);

/* 设置解析的起点 */
void loadpagefromfile_setstartpos(loadpage* loadpage, recpos pos);

/* 加载页 */
bool loadpagefromfile_loadpage(loadpage* loadpage, mpage* mp);

/* 关闭文件描述符 */
void loadpagefromfile_close(loadpage* loadpage);

/* 释放 */
void loadpagefromfile_free(loadpage* loadpage);

#endif
