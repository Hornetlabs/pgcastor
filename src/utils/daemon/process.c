/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
*/
#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "port/ipc/ipc.h"
#include "port/file/fd.h"
#include "utils/daemon/ripple_process.h"

/* 关闭标准输入/输出 */
void ripple_closestd(void)
{
    /* stdin */
    FileClose(STDIN_FILENO);

    FileOpen("/dev/null", O_RDWR, 0);

    /* stdout */
    FileClose(STDOUT_FILENO);
    FileOpen("/dev/null", O_RDWR, 0);


    /* stderr */
    FileClose(STDERR_FILENO);
    FileOpen("/dev/null", O_RDWR, 0);
    g_closestd = true;
}


/* 设置为后台执行 */
void ripple_makedaemon(void)
{
    int ret = 0;
    pid_t pid = 0;
    int pipes[2] = {-1, -1};
    char pipemsg[128] = {0};

    fflush(stdout);
	fflush(stderr);

    ret = ripple_ipc_pipe(pipes);
    if(0 != ret)
    {
        elog(RLOG_ERROR, "pipe error:%s", strerror(errno));
    }

    pid = ripple_ipc_fork();
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
        FileClose(pipes[1]);
        if(0 != ret)
        {
            elog(RLOG_ERROR, "ripple init error");
        }

        while((ret = FileRead(pipes[0], pipemsg, 128)))
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
        FileClose(pipes[0]);
        exit(0);
    }

    /* 子进程 */
    FileClose(pipes[0]);
    setsid();
    setpgid(0, getpid());

    /* 再次 fork */
    pid = ripple_ipc_fork();
    if(-1 == pid)
    {
        if(22 != FileWrite(pipes[1], "errorsecond fork error", 22))
        {
            elog(RLOG_ERROR, "write pipe error:%s\n", strerror(errno));
        }

        FileClose(pipes[1]);
        elog(RLOG_ERROR, "sec fork error:%s", strerror(errno));
        exit(-1);
    }

    if(0 < pid)
    {
        /* 父进程,退出 */
        FileClose(pipes[1]);
        exit(0);
    }

    FileClose(pipes[1]);
}

/* 执行后台命令 */
bool ripple_execcommand(char* cmd, void* args, void (*childdestroy)(void* args))
{
    pid_t pid = 0;
    if (NULL == cmd)
    {
        return false;
    }

    pid = ripple_ipc_fork();
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
