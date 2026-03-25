#include "app_incl.h"
#include "utils/mpage/mpage.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"

static loadpageroutine m_loadpagefile = {.loadpageinit = loadpagefromfile_init,
                                         .loadpagesetfilesource = loadpagefromfile_setfdir,
                                         .loadpagesettype = loadpagefromfile_settype,
                                         .loadpagesetstartpos = loadpagefromfile_setstartpos,
                                         .loadpageclose = loadpagefromfile_close,
                                         .loadpage = loadpagefromfile_loadpage,
                                         .loadpagefree = loadpagefromfile_free};

/* get page routine information */
loadpageroutine* loadpage_getpageroutine(int type)
{
    if (LOADPAGE_TYPE_FILE == type)
    {
        return &m_loadpagefile;
    }

    elog(RLOG_WARNING, "get page routine, unknown type:%d", type);
    return NULL;
}
