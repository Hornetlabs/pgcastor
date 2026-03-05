#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/path/ripple_path.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/mpage/mpage.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"

int main(int argc, char** argv)
{
    int index                                   = 0;
    char* profilepath                           = NULL;
    const char* loglevel                        = NULL;
    dlistnode* dlnode                           = NULL;
    ripple_record* record                       = NULL;
    ripple_loadtrailrecords* loadtrailrecords   = NULL;


    ripple_mem_init();
    profilepath = ripple_make_absolute_path(argv[1]);
    rmemcpy1(g_profilepath, 0, profilepath, strlen(profilepath));
    rfree(profilepath);

    /* 获取绝对路径，并加载 license */
    rmemcpy1(g_cfgpath, 0, argv[1], strlen(argv[1]));
    ripple_path_canonicalize_path(g_cfgpath);
    for(index = strlen(g_cfgpath); index > 0; index--)
    {
        if(g_cfgpath[index - 1] != '/')
        {
            g_cfgpath[index - 1] = '\0';
            continue;
        }
        break;
    }
    snprintf(g_cfgpath + strlen(g_cfgpath), RIPPLE_MAXPATH, "%s", RIPPLE_LICENSE_NAME);

    g_proctype = RIPPLE_PROC_TYPE_INTEGRATE;
    /* 参数解析 */
    guc_loadcfg(argv[1], false);

    /* 查看解析内容是否正确 */
    guc_debug();

    /* 设置 日志级别 */
    loglevel = guc_getConfigOption(RIPPLE_CFG_KEY_LOG_LEVEL);
    if(NULL == loglevel)
    {
        elog(RLOG_ERROR, "unrecognized configuration parameter:%s", loglevel);
    }

    elog_seteloglevel(loglevel);

    loadtrailrecords = ripple_loadtrailrecords_init();
    
    if(false == ripple_loadtrailrecords_setloadpageroutine(loadtrailrecords, RIPPLE_LOADPAGE_TYPE_FILE))
    {
        printf("loadtrailrecords set page routine error\n");
        return 1;
    }

    if(false == ripple_loadtrailrecords_setloadsource(loadtrailrecords, guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR)))
    {
        printf("loadtrailrecords set load source error\n");
        return 1;
    }

    /* 设置解析的起点 */
    ripple_loadtrailrecords_setloadposition(loadtrailrecords, 0, 0);
    while(1)
    {
        if(false == ripple_loadtrailrecords_load(loadtrailrecords))
        {
            printf("loadtrailrecords load error\n");
            return 1;
        }

        if(true == dlist_isnull(loadtrailrecords->records))
        {
            break;
        }

        /* 输出类型 */
        for(dlnode = loadtrailrecords->records->head; NULL != dlnode; dlnode = loadtrailrecords->records->head)
        {
            record = (ripple_record*)dlnode->value;
            printf("record type:%d, start:%lu.%lu, end:%lu.%lu\n",
                    record->type,
                    record->start.trail.fileid,
                    record->start.trail.offset,
                    record->end.trail.fileid,
                    record->end.trail.offset);

            if(9437144 == record->end.trail.offset && 1 == record->end.trail.fileid)
            {
                ripple_record_free(record);
            }
            else
            {
                ripple_record_free(record);
            }
            loadtrailrecords->records->head = dlnode->next;
            rfree(dlnode);
        }
        rfree(loadtrailrecords->records);
        loadtrailrecords->records = NULL;
    }

    ripple_loadtrailrecords_free(loadtrailrecords);
    return 0;
}
