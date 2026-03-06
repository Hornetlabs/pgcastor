#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/init/ripple_datainit.h"

typedef struct RIPPLE_SUBDIRS
{
    ripple_proc_type            type;
    int                         subcnt;
    char**                      subdirs;
} ripple_subdirs;

/* capture 子目录 */
static char *m_subdirscapture[] = {
    "catalog",
    "chk",
    "trail",
    "stat",
    "log",
    "filter",
    "refresh",
    "onlinerefresh"
};

/* integrate 子目录 */
static char *m_subdirsintegrate[] = {
    "chk",
    "trail",
    "stat",
    "log",
    "filter",
    "refresh",
    "onlinerefresh"
};

/* xmanager jobname 子目录 */
static char *m_subdirsxmanager[] = {
    "log",
    "metric"
};

static ripple_subdirs   m_subdirs[] =
{
    {RIPPLE_PROC_TYPE_NOP,              0,                                  NULL},
    {RIPPLE_PROC_TYPE_CAPTURE,          lengthof(m_subdirscapture),         m_subdirscapture},
    {RIPPLE_PROC_TYPE_INTEGRATE,        lengthof(m_subdirsintegrate),       m_subdirsintegrate},
    {RIPPLE_PROC_TYPE_HGRECEIVEWAL,     0,                                  NULL},
    {RIPPLE_PROC_TYPE_PGRECEIVEWAL,     0,                                  NULL},
    {RIPPLE_PROC_TYPE_XMANAGER,         lengthof(m_subdirsxmanager),        m_subdirsxmanager}
};

/* 创建 data 目录 */
bool ripple_datainit_init(char* in_wdata)
{
    int index = 0;
    DIR* datadir = NULL;
    struct dirent *file = NULL;
    char path[RIPPLE_MAXPATH] = { 0 };

    if(NULL == m_subdirs[g_proctype].subdirs)
    {
        return true;
    }

    datadir = OpenDir(in_wdata);
    if(NULL == datadir)
    {
        if(errno != ENOENT)
        {
            elog(RLOG_WARNING, "open dir error:%s", in_wdata);
            return false;
        }

        /* 创建目录 */
        if(0 != MakeDir(in_wdata))
        {
            elog(RLOG_WARNING, "could not create directory:%s", in_wdata);
            return false;
        }
    }
    else
    {
        while (errno = 0, (file = ReadDir(datadir, in_wdata)) != NULL)
        {
            if (0 == strcmp(".", file->d_name)
                || 0 == strcmp("..", file->d_name))
            {
                continue;
            }
            else
            {
                elog(RLOG_WARNING, "directory %s exists but is not empty", guc_getConfigOption(RIPPLE_CFG_KEY_DATA));
                return false;
            }
        }
    }
    FreeDir(datadir);

    /* 创建子目录 */
    for(index = 0; index < m_subdirs[g_proctype].subcnt; index++)
    {
        rmemset1(path, 0, '\0', RIPPLE_MAXPATH);
        snprintf(path, RIPPLE_MAXPATH, "%s/%s", 
                                        guc_getConfigOption(RIPPLE_CFG_KEY_DATA),
                                        m_subdirs[g_proctype].subdirs[index]);

        if(0 != MakeDir(path))
        {
            elog(RLOG_WARNING, "could not create directory:%s", path);
            return false;
        }
    }
    return true;
}

/* 临时文件清理 */
void ripple_datainit_clear(const char *dir_path) 
{
    struct dirent *entry;
    DIR *dp = OpenDir(dir_path);

    if (dp == NULL) {
        elog(RLOG_WARNING, "Could not found folder %s", dir_path);
        return ;
    }

    while (((entry = ReadDir(dp,dir_path)) != NULL)) {
        if (strstr(entry->d_name, ".tmp") != NULL)
        {
            char full_path[1024];
            snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
            durable_unlink(full_path, RLOG_DEBUG);
            return ;
        }
    }
    FreeDir(dp);
    return ; 
}

