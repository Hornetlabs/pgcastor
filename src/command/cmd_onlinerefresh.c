#include "app_incl.h"
#include "utils/init/init.h"
#include "utils/string/stringinfo.h"
#include "utils/list/list_func.h"
#include "command/cmd.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "misc/misc_lockfiles.h"

static bool cmd_onlinerefresh_append_result(char* input, StringInfo str)
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

static char* cmd_onlinerefresh_format_table_info(List* source)
{
    StringInfoData str = {'\0'};
    ListCell*      cell = NULL;
    char*          result = NULL;

    initStringInfo(&str);

    foreach (cell, source)
    {
        char* table = (char*)lfirst(cell);

        if (!cmd_onlinerefresh_append_result(table, &str))
        {
            return NULL;
        }
    }

    result = rstrdup(str.data);
    rfree(str.data);

    return result;
}

/* Output */
static void onlinerefresh_status_print(void)
{
    FILE*          fp = NULL;
    char           fline[1024] = {'\0'};
    StringInfoData str = {"\0"};
    bool           first = true;

    initStringInfo(&str);

    fp = osal_file_fopen(ONLINEREFRESH_STATUS, "r");
    if (NULL == fp)
    {
        printf("open %s failed\n", ONLINEREFRESH_STATUS);
        rfree(str.data);
        /* make compiler happy */
        return;
    }

    while (fgets(fline, 130, fp))
    {
        int line_len = 0;

        line_len = strlen(fline);
        /* Remove newline character */
        if ('\n' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* Empty line */
                continue;
            }
        }
        /* Windows text: remove carriage return */
        if ('\r' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* Empty line */
                continue;
            }
        }
        if (0 == line_len)
        {
            /* Empty line */
            continue;
        }

        if (first)
        {
            if (!strncmp(fline, "SUCCESS", 8))
            {
                printf("success send online refresh signal to capture!\n");
                rfree(str.data);
                /* Delete files */
                unlink(ONLINEREFRESH_STATUS);
                unlink(ONLINEREFRESH_DAT);
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
    /* Delete files */
    unlink(ONLINEREFRESH_STATUS);
    unlink(ONLINEREFRESH_DAT);
}

/* Get onlinerefresh status information */
static void cmd_onlinerefresh_get_onlinerefresh_status(void)
{
    int         cnt = 0;
    long        castorpid;
    struct stat statbuf;
    char        szMsg[256] = {0};

    castorpid = misc_lockfiles_getpid();
    if (0 == castorpid)
    {
        printf("Is castor running?\n");
        return;
    }

    if (0 != kill((pid_t)castorpid, SIGUSR1))
    {
        snprintf(szMsg, 128, "could not send status signal (PID:%ld) : %s\n", castorpid, strerror(errno));
        printf("%s\n", szMsg);
    }

    /* Check onlinerefresh */
    for (cnt = 0; cnt < (WAIT * WAITS_PER_SEC); cnt++)
    {
        /* Check if file exists */
        if (0 != stat(ONLINEREFRESH_STATUS, &statbuf))
        {
            if (errno != ENOENT)
            {
                /* Read and output data */
                printf("get %s stat error, %s\n", ONLINEREFRESH_STATUS, strerror(errno));
                exit(-1);
            }
        }
        else
        {
            onlinerefresh_status_print();
            exit(0);
        }

        sleep(1);

        printf(".");
    }

    printf("can not get online refresh status\n");
}

bool cmd_onlinerefresh(void* extra_config)
{
    char* wdata = NULL;
    char* rewrite = NULL;
    int   fd = -1;
    List* table_list = NULL;

    if (!extra_config)
    {
        printf("online refresh tables is NULL, please check your input\n");
        return false;
    }
    table_list = (List*)extra_config;

    /* Get working directory */
    wdata = guc_getdata();

    /* Check if data directory exists */
    if (false == osal_dir_exist(wdata))
    {
        printf("work data not exist:%s\n", wdata);
        return false;
    }

    /* Change working directory */
    chdir(wdata);

    /* Format input */
    rewrite = cmd_onlinerefresh_format_table_info(table_list);
    if (!rewrite)
    {
        printf("invalid online refresh tables format, please check your input\n");
        return false;
    }

    fd = osal_basic_open_file(ONLINEREFRESH_DAT, O_RDWR | O_CREAT | BINARY);
    if (-1 == fd)
    {
        printf("open file:%s error, %s\n", ONLINEREFRESH_DAT, strerror(errno));
        return false;
    }

    osal_file_write(fd, rewrite, strlen(rewrite));

    osal_file_close(fd);

    cmd_onlinerefresh_get_onlinerefresh_status();

    return true;
}
