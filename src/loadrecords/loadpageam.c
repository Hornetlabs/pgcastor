#include "ripple_app_incl.h"
#include "utils/mpage/mpage.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"

static ripple_loadpageroutine m_loadpagefile =
{
    .loadpageinit           = ripple_loadpagefromfile_init,
    .loadpagesetfilesource  = ripple_loadpagefromfile_setfdir,
    .loadpagesettype        = ripple_loadpagefromfile_settype,
    .loadpagesetstartpos    = ripple_loadpagefromfile_setstartpos,
    .loadpageclose          = ripple_loadpagefromfile_close,
    .loadpage               = ripple_loadpagefromfile_loadpage,
    .loadpagefree           = ripple_loadpagefromfile_free
};

/* 获取 getpage 信息 */
ripple_loadpageroutine* ripple_loadpage_getpageroutine(int type)
{
    if(RIPPLE_LOADPAGE_TYPE_FILE == type)
    {
        return &m_loadpagefile;
    }

    elog(RLOG_WARNING, "get page routine, unknown type:%d", type);
    return NULL;
}
