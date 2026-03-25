#include "app_incl.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/init/datainit.h"

typedef struct SUBDIRS
{
    proc_type type;
    int       subcnt;
    char**    subdirs;
} subdirs;

/* capture subdirectory */
static char* m_subdirscapture[] = {"catalog", "chk",    "trail",   "stat",
                                   "log",     "filter", "refresh", "onlinerefresh"};

/* integrate subdirectory */
static char* m_subdirsintegrate[] = {"chk",    "trail",   "stat",         "log",
                                     "filter", "refresh", "onlinerefresh"};

/* xmanager jobname subdirectory */
static char* m_subdirsxmanager[] = {"log", "metric"};

static subdirs m_subdirs[] = {
    {PROC_TYPE_NOP, 0, NULL},
    {PROC_TYPE_CAPTURE, lengthof(m_subdirscapture), m_subdirscapture},
    {PROC_TYPE_INTEGRATE, lengthof(m_subdirsintegrate), m_subdirsintegrate},
    {PROC_TYPE_PGRECEIVEWAL, 0, NULL},
    {PROC_TYPE_XMANAGER, lengthof(m_subdirsxmanager), m_subdirsxmanager}};

/* Create data directory */
bool datainit_init(char* in_wdata)
{
    int            index = 0;
    DIR*           datadir = NULL;
    struct dirent* file = NULL;
    char           path[MAXPATH] = {0};

    if (NULL == m_subdirs[g_proctype].subdirs)
    {
        return true;
    }

    datadir = osal_open_dir(in_wdata);
    if (NULL == datadir)
    {
        if (errno != ENOENT)
        {
            elog(RLOG_WARNING, "open dir error:%s", in_wdata);
            return false;
        }

        /* Create directory */
        if (0 != osal_make_dir(in_wdata))
        {
            elog(RLOG_WARNING, "could not create directory:%s", in_wdata);
            return false;
        }
    }
    else
    {
        while (errno = 0, (file = osal_read_dir(datadir, in_wdata)) != NULL)
        {
            if (0 == strcmp(".", file->d_name) || 0 == strcmp("..", file->d_name))
            {
                continue;
            }
            else
            {
                elog(RLOG_WARNING, "directory %s exists but is not empty",
                     guc_getConfigOption(CFG_KEY_DATA));
                return false;
            }
        }
    }
    osal_free_dir(datadir);

    /* Create subdirectories */
    for (index = 0; index < m_subdirs[g_proctype].subcnt; index++)
    {
        rmemset1(path, 0, '\0', MAXPATH);
        snprintf(path, MAXPATH, "%s/%s", guc_getConfigOption(CFG_KEY_DATA),
                 m_subdirs[g_proctype].subdirs[index]);

        if (0 != osal_make_dir(path))
        {
            elog(RLOG_WARNING, "could not create directory:%s", path);
            return false;
        }
    }
    return true;
}

/* Temporary file cleanup */
void datainit_clear(const char* dir_path)
{
    struct dirent* entry;
    DIR*           dp = osal_open_dir(dir_path);

    if (dp == NULL)
    {
        elog(RLOG_WARNING, "Could not found folder %s", dir_path);
        return;
    }

    while (((entry = osal_read_dir(dp, dir_path)) != NULL))
    {
        if (strstr(entry->d_name, ".tmp") != NULL)
        {
            char full_path[1024];
            snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
            osal_durable_unlink(full_path, RLOG_DEBUG);
            return;
        }
    }
    osal_free_dir(dp);
    return;
}
