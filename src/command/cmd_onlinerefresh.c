#include "ripple_app_incl.h"
#include "utils/init/ripple_init.h"
#include "utils/string/stringinfo.h"
#include "utils/list/list_func.h"
#include "command/ripple_cmd.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "misc/ripple_misc_lockfiles.h"

static bool ripple_cmd_onlinerefresh_append_result(char *input, StringInfo str)
{
    if (strlen(input) > 129)
    {
        return false;
    }

    if (!strstr(input, "."))
    {
        return false;
    }

    appendStringInfo(str, "%s\n", input);
    return true;
}

static char *ripple_cmd_onlinerefresh_format_table_info(List *source)
{
    StringInfoData str = {'\0'};
    ListCell *cell = NULL;
    char *result = NULL;

    initStringInfo(&str);

    foreach(cell, source)
    {
        char *table = (char *) lfirst(cell);

        if (!ripple_cmd_onlinerefresh_append_result(table, &str))
        {
            return NULL;
        }
    }

    result = rstrdup(str.data);
    rfree(str.data);

    return result;
}

/* 输出 */
static void ripple_onlinerefresh_status_print(void)
{
    FILE *fp = NULL;
    char fline[1024] = {'\0'};
    StringInfoData str = {"\0"};
    bool first = true;

    initStringInfo(&str);

    fp = FileFOpen(RIPPLE_ONLINEREFRESH_STATUS, "r");
    if (NULL == fp)
    {
        printf("open %s failed\n", RIPPLE_ONLINEREFRESH_STATUS);
        rfree(str.data);
        /* make compiler happy */
        return;
    }

    while(fgets(fline, 130, fp))
    {
        int line_len = 0;

        line_len = strlen(fline);
        /* 排除换行符 */
        if ('\n' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* 空行 */
                continue;
            }
        }
        /* windos文本排除回车符 */
        if ('\r' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* 空行 */
                continue;
            }
        }
        if (0 == line_len)
        {
            /* 空行 */
            continue;
        }

        if (first)
        {
            if (!strncmp(fline, "SUCCESS", 8))
            {
                printf("success send online refresh signal to capture!\n");
                rfree(str.data);
                /* 删除文件 */
                unlink(RIPPLE_ONLINEREFRESH_STATUS);
                unlink(RIPPLE_ONLINEREFRESH_DAT);
                return;
            }

            first = false;
            continue;
        }

        appendStringInfo(&str, "%s\n", fline);
    }

    printf("failed to send onlinere fresh signal, because some tables don't exist\ntable list:\n");
    printf("\t%s\n", str.data);

    rfree(str.data);
    /* 删除文件 */
    unlink(RIPPLE_ONLINEREFRESH_STATUS);
    unlink(RIPPLE_ONLINEREFRESH_DAT);
}

/* 获取onlinerefresh状态信息 */
static void ripple_cmd_onlinerefresh_get_onlinerefresh_status(void)
{
    int cnt = 0;
    long    ripplepid;
    struct stat statbuf;
    char    szMsg[256] = { 0 };

    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        printf("Is ripple running?\n");
        return;
    }

    if(0 != kill((pid_t) ripplepid, SIGUSR1))
    {
        snprintf(szMsg, 128, "could not send status signal (PID:%ld) : %s\n", ripplepid, strerror(errno));
        printf("%s\n", szMsg);
    }

    /* 检测onlinerefresh */
    for(cnt = 0; cnt < (RIPPLE_WAIT*RIPPLE_WAITS_PER_SEC); cnt++)
    {
        /* 检测文件是否存在 */
        if (0 != stat(RIPPLE_ONLINEREFRESH_STATUS, &statbuf))
        {
            if (errno != ENOENT)
            {
                /* 读取数据并输出 */
                printf("get %s stat error, %s\n", RIPPLE_ONLINEREFRESH_STATUS, strerror(errno));
                exit(-1);
            }
        }
        else
        {
            ripple_onlinerefresh_status_print();
            exit(0);
        }

        sleep(1);

        printf(".");
    }

    printf("can not get online refresh status\n");
}

bool ripple_cmd_onlinerefresh(void *extra_config)
{
    char   *wdata = NULL;
    char   *rewrite = NULL;
    int     fd = -1;
    List   *table_list = NULL;

    if (!extra_config)
    {
        printf("online refresh tables is NULL, please check your input\n");
        return false;
    }
    table_list = (List *) extra_config;

    /* 获取工作目录 */
    wdata = guc_getdata();

    /* 检测 data 目录是否存在 */
    if(false == DirExist(wdata))
    {
        printf("work data not exist:%s\n", wdata);
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 格式化输入 */
    rewrite = ripple_cmd_onlinerefresh_format_table_info(table_list);
    if (!rewrite)
    {
        printf("invalid online refresh tables format, please check your input\n");
        return false;
    }

    fd = BasicOpenFile(RIPPLE_ONLINEREFRESH_DAT, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if(-1 == fd)
    {
        printf("open file:%s error, %s\n", RIPPLE_ONLINEREFRESH_DAT, strerror(errno));
        return false;
    }

    FileWrite(fd, rewrite, strlen(rewrite));

    FileClose(fd);

    ripple_cmd_onlinerefresh_get_onlinerefresh_status();

    return true;
}
