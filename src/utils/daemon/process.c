/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
*/
#include "app_incl.h"
#include "utils/guc/guc.h"
#include "port/ipc/ipc.h"
#include "port/file/fd.h"
#include "utils/daemon/process.h"

/* 关闭标准输入/输出 */
void closestd(void)
{
    /* stdin */
    osal_file_close(STDIN_FILENO);

    osal_file_open("/dev/null", O_RDWR, 0);

    /* stdout */
    osal_file_close(STDOUT_FILENO);
    osal_file_open("/dev/null", O_RDWR, 0);


    /* stderr */
    osal_file_close(STDERR_FILENO);
    osal_file_open("/dev/null", O_RDWR, 0);
    g_closestd = true;
}


/* 设置为后台执行 */
void makedaemon(void)
{
    int ret = 0;
    pid_t pid = 0;
    int pipes[2] = {-1, -1};
    char pipemsg[128] = {0};

    fflush(stdout);
	fflush(stderr);

    ret = osal_ipc_pipe(pipes);
    if(0 != ret)
    {
        elog(RLOG_ERROR, "pipe error:%s", strerror(errno));
    }

    pid = osal_ipc_fork();
    if(-1 == pid)
    {
        elog(RLOG_ERROR, "make ripple daemon error ipc fork");
    }

    /*  */
    if(0 != pid)
    {
        /* 父进程 */
        /* 等待子进程退出 */
        wait(&ret);
        osal_file_close(pipes[1]);
        if(0 != ret)
        {
            elog(RLOG_ERROR, "ripple init error");
        }

        while((ret = osal_file_read(pipes[0], pipemsg, 128)))
        {
            if(0 > ret)
            {
                elog(RLOG_ERROR, "read pipe message error, %s", strerror(errno));
            }

            if(ret >= 5 && 0 == strncmp(pipemsg, "error", 5))
            {
                elog(RLOG_ERROR, "error happend in init, %s", pipemsg);
            }
            elog(RLOG_INFO, "pipe message:%s", pipemsg);
            rmemset1(pipemsg, 0, '\0', 128);
        }
        osal_file_close(pipes[0]);
        exit(0);
    }

    /* 子进程 */
    osal_file_close(pipes[0]);
    setsid();
    setpgid(0, getpid());

    /* 再次 fork */
    pid = osal_ipc_fork();
    if(-1 == pid)
    {
        if(22 != osal_file_write(pipes[1], "errorsecond fork error", 22))
        {
            elog(RLOG_ERROR, "write pipe error:%s\n", strerror(errno));
        }

        osal_file_close(pipes[1]);
        elog(RLOG_ERROR, "sec fork error:%s", strerror(errno));
        exit(-1);
    }

    if(0 < pid)
    {
        /* 父进程,退出 */
        osal_file_close(pipes[1]);
        exit(0);
    }

    osal_file_close(pipes[1]);
}

/* 执行后台命令 */
bool execcommand(char* cmd, void* args, void (*childdestroy)(void* args))
{
    pid_t pid = 0;
    if (NULL == cmd)
    {
        return false;
    }

    pid = osal_ipc_fork();
    if (-1 == pid)
    {
        elog(RLOG_WARNING, "exec command error, fork child error");
        return false;
    }

    if (0 != pid)
    {
        /* 父进程 */
        return true;
    }

    /*
     * 子进程
     *  1、清理资源
     *  2、执行 system 命令
     */
    childdestroy(args);

    system(cmd);
    exit(0);
}
