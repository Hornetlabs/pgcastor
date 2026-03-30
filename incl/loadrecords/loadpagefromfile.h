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
    loadpage loadpage;
    int      filetype;
    int      fd;
    uint64   foffset;
    uint64   fileno;
    char     fdir[MAXPATH];
    char     fpath[ABSPATH];
} loadpagefromfile;

/* Initialize */
loadpage* loadpagefromfile_init(void);

/* Set file storage path */
bool loadpagefromfile_setfdir(loadpage* loadpage, char* fsource);

/* Set type */
void loadpagefromfile_settype(loadpage* loadpage, int type);

/* Set parsing starting point */
void loadpagefromfile_setstartpos(loadpage* loadpage, recpos pos);

/* Load page */
bool loadpagefromfile_loadpage(loadpage* loadpage, mpage* mp);

/* Close file descriptor */
void loadpagefromfile_close(loadpage* loadpage);

/* Release */
void loadpagefromfile_free(loadpage* loadpage);

#endif
