#ifndef _LOADPAGEAM_H_
#define _LOADPAGEAM_H_

typedef struct LOADPAGEROUTINE
{
    /* Initialize */
    loadpage* (*loadpageinit)(void);

    /* Set data source */
    bool (*loadpagesetfilesource)(loadpage* loadpage, char* fsource);

    /* Set type, WAL/TRAIL */
    void (*loadpagesettype)(loadpage* loadpage, int type);

    /* Set starting point for loading */
    void (*loadpagesetstartpos)(loadpage* loadpage, recpos pos);

    /* Close file descriptor */
    void (*loadpageclose)(loadpage* loadpage);

    /* Load page */
    bool (*loadpage)(loadpage* loadpage, mpage* mp);

    /* Memory release */
    void (*loadpagefree)(loadpage* loadpage);
} loadpageroutine;

/* Get getpage information */
loadpageroutine* loadpage_getpageroutine(int type);

#endif
