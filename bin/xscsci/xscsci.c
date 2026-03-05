#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>

#include "ripple_c.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xscsci_input.h"
#include "xscsci_prescan.h"
#include "xscsci_scan_private.h"
#include "xscsci_scansup.h"
#include "xscsci.h"
#include "xscsci_precommand.h"

#define XSCSCI_SETMASK(mask)	sigprocmask(SIG_SETMASK, mask, NULL)

typedef void (*xscscisigfunc) (int signo);

/* Global variables */
sigset_t	UnBlockSig,
			BlockSig;

/* 初始化xsciscistat */
static xsciscistat* xscsci_init(void)
{
    xsciscistat* xscisc = NULL;

    xscisc = (xsciscistat*)malloc(sizeof(xsciscistat));
    if (NULL == xscisc)
    {
        return NULL;
    }
    memset(xscisc, 0, sizeof(xsciscistat));
    xscisc->xsynchhome = NULL;
    xscisc->conn = NULL;

    return xscisc;
}

static xscscisigfunc xscsci_signal_pm(int signo, xscscisigfunc func)
{
	struct sigaction act,
				oact;

	act.sa_handler = func;
	if (func == SIG_IGN || func == SIG_DFL)
	{
		/* in these cases, act the same as pqsignal() */
		sigemptyset(&act.sa_mask);
		act.sa_flags = SA_RESTART;
	}
	else
	{
		act.sa_mask = BlockSig;
		act.sa_flags = 0;
	}

	if (sigaction(signo, &act, &oact) < 0)
		return SIG_ERR;
	return oact.sa_handler;
}


static void xscsci_signal_initmask(void)
{
    sigemptyset(&UnBlockSig);

    /* First set all signals, then clear some. */
    sigfillset(&BlockSig);
    sigdelset(&BlockSig, SIGTRAP);
    sigdelset(&BlockSig, SIGABRT);
    sigdelset(&BlockSig, SIGILL);
    sigdelset(&BlockSig, SIGFPE);
    sigdelset(&BlockSig, SIGSEGV);
    sigdelset(&BlockSig, SIGBUS);
    sigdelset(&BlockSig, SIGSYS);
    sigdelset(&BlockSig, SIGCONT);
}


static void xscsci_signal_init(void)
{
    xscsci_signal_initmask();

    XSCSCI_SETMASK(&BlockSig);

    xscsci_signal_pm(SIGHUP, SIG_IGN);
    xscsci_signal_pm(SIGQUIT, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGALRM, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGPIPE, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGUSR1, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGUSR2, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGCHLD, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGTTIN, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGTTOU, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGXFSZ, SIG_IGN); /* ignored */
    xscsci_signal_pm(SIGTERM, SIG_IGN); /* ignored */
}

static void xscsci_singal_setmask(void)
{
    XSCSCI_SETMASK(&UnBlockSig);
}

/* 
 * 获取环境变量XSYNCH
 * 检查是否配置以及是否为目录
 */
static bool xscsci_getxsynchhome(xsciscistat* xscisc)
{
    struct stat st;

    xscisc->xsynchhome = getenv("XSYNCH");

    /* 获取不到环境变量 */
    if (NULL == xscisc->xsynchhome)
    {
        printf("XSYNCH configuration error: not set XSYNCH \n");
        return false;
    }

    /* 是否存在 */
    if (stat(xscisc->xsynchhome, &st) != 0) 
    {
        printf("XSYNCH configuration error: %s (%s)\n", xscisc->xsynchhome, strerror(errno));
        return false;
    }

    /* 是否为目录 */
    if (!S_ISDIR(st.st_mode)) 
    {
        printf("XSYNCH configuration error: %s is not a directory\n", xscisc->xsynchhome);
        return false;
    }
    return true;
}

static bool xscsci_parseargs(int argc, char **argv, char **connstr)
{
    int index_argc  = 1;
    char port[128]  = {'\0'};
    char host[512]  = {'\0'};

    memset(port, 0 , 128);
    memset(host, 0 , 512);

    while (index_argc < argc)
    {
        if (strcmp(argv[index_argc], "-h") == 0)
        {
            if (index_argc + 1 >= argc)
            {
                printf("-h requires an argument\n");
                return false;
            }

            snprintf(host, sizeof(host), "%s", argv[index_argc + 1]);
            index_argc += 2;
        }
        else if (strcmp(argv[index_argc], "-p") == 0)
        {
            if (index_argc + 1 >= argc)
            {
                printf("-p requires an argument\n");
                return false;
            }

            snprintf(port, sizeof(port), "%s", argv[index_argc + 1]);
            index_argc += 2;
        }
        else
        {
            printf("unknown option: %s\n", argv[index_argc]);
            return false;
        }
    }

    if ('\0' == host[0] && '\0' == port[0])
    {
        return true;
    }

    if ('\0' == port[0])
    {
        sprintf(port, "%s", "6543");
    }

    *connstr = (char*)malloc(512);
    memset(*connstr, 0 , 512);

    if ('\0' == host[0] || 0 == strcmp(host, "127.0.0.1"))
    {
        sprintf(*connstr, "host=127.0.0.1 port=%s protocol=UNIXDOMAIN", port);
    }
    else
    {
        sprintf(*connstr, "host=%s port=%s protocol=TCP", host, port);
    }

    return true;
}

/* 帮助文档 */
static void xscsci_help(void)
{
    printf("you can use xscsci, the command-line to xsynch.\n");

    /* create 命令支持 */
    printf("---------use create command create a job----------------\n");
    printf("create manager              create progress\n");
    printf("create pgreceivelog         create hgreceivelog\n");
    printf("create capture              create pump\n");
    printf("create collector            create integrate\n");
    printf("\n");

    /* edit 支持 */
    printf("---------use edit command edit job config file----------\n");
    printf("edit manager\n");
    printf("edit pgreceivelog           edit hgreceivelog\n");
    printf("edit capture                edit pump\n");
    printf("edit collector              edit integrate\n");
    printf("\n");

    /* init 支持 */
    printf("---------use init command init job work dir-------------\n");
    printf("init manager\n");
    printf("init pgreceivelog           init hgreceivelog\n");
    printf("init capture                init pump\n");
    printf("init collector              init integrate\n");
    printf("\n");

    /* start 支持 */
    printf("---------use start command start job -------------------\n");
    printf("start manager\n");
    printf("start pgreceivelog          start hgreceivelog\n");
    printf("start capture               start pump\n");
    printf("start collector             start integrate\n");
    printf("\n");

    /* stop 支持 */
    printf("---------use stop command stop job ---------------------\n");
    printf("stop manager\n");
    printf("stop pgreceivelog           stop hgreceivelog\n");
    printf("stop capture                stop pump\n");
    printf("stop collector              stop integrate\n");
    printf("\n");

    /* alter 支持 */
    printf("---------use alter command alter progress member--------\n");
    printf("alter progress\n");

    /* reload 支持 */
    printf("---------use reload command reload config file----------\n");
    printf("reload manager\n");
    printf("reload pgreceivelog         reload hgreceivelog\n");
    printf("reload capture              reload pump\n");
    printf("reload collector            reload integrate\n");
    printf("\n");

    /* remove 支持 */
    printf("---------use remove command remove config file----------\n");
    printf("remove manager\n");
    printf("remove pgreceivelog         remove hgreceivelog\n");
    printf("remove capture              remove pump\n");
    printf("remove collector            remove integrate\n");
    printf("\n");

    /* drop 支持 */
    printf("---------use drop command drop job----------------------\n");
    printf("drop manager                drop progress\n");
    printf("drop pgreceivelog           drop hgreceivelog\n");
    printf("drop capture                drop pump\n");
    printf("drop collector              drop integrate\n");
    printf("\n");

    /* info 支持 */
    printf("---------use info command view job base info------------\n");
    printf("info manager                info progress\n");
    printf("info pgreceivelog           info hgreceivelog\n");
    printf("info capture                info pump\n");
    printf("info collector              info integrate\n");
    printf("\n");

    /* watch 支持 */
    printf("---------use watch command view job info every seconds--\n");
    printf("watch manager               watch progress\n");
    printf("watch pgreceivelog          watch hgreceivelog\n");
    printf("watch capture               watch pump\n");
    printf("watch collector             watch integrate\n");
    printf("\n");

    /* watch 支持 */
    printf("use exit/quit command exit xscsci\n");
}

/* 程序退出 */
static void xscsci_exit(int rcode)
{
    /*
     * 1、释放与manager的连接
     * 2、回收资源
     */
    exit(rcode);
}

int main(int argc, char **argv)
{
    bool hasmore                            = false;
    int parserret                           = 0;
    xscsci_prescanresult result             = 0;
    char* line                              = NULL;
    char *connstr                           = NULL;
    char* prevhistorybuf                    = NULL;
    volatile xsynch_exbuffer querybuf       = NULL;
    xscsci_prescan* prescan                 = NULL;
    xsynch_cmd* cmd                         = NULL;
    xsynch_exbuffer historybuf              = NULL;
    xsciscistat* xscisc                     = NULL;

    if (false == xscsci_parseargs(argc, argv, &connstr))
    {
        xscsci_exit(1);
    }

    /* 初始化 */
    querybuf = xsynch_exbufferdata_init();
    if (NULL == querybuf)
    {
        printf("out of memory, %s\n", strerror(errno));
        xscsci_exit(1);
    }

    historybuf = xsynch_exbufferdata_init();
    if (NULL == historybuf)
    {
        printf("out of memory, %s\n", strerror(errno));
        xscsci_exit(1);
    }

    prescan = xscsci_prescan_create();
    if (NULL == prescan)
    {
        printf("out of memory, %s\n", strerror(errno));
        exit(1);
    }

    xscisc = xscsci_init();
    if (NULL == xscisc)
    {
        printf("out of memory, %s\n", strerror(errno));
        exit(1);
    }

    if (false == xscsci_getxsynchhome(xscisc))
    {
        exit(1);
    }

    xscsci_signal_init();

    /*
     * 连接 manager
     */
    xscisc->conn = XSynchSetParam(connstr);
    XSynchConn(xscisc->conn);
    free(connstr);

    /* readline 初始化 */
    xscsci_input_init();

    xscsci_singal_setmask();

    while(1)
    {
        fflush(stdout);

        /* 获取 readline 数据 */
        line = xscsci_input_getsinteractive("xscsci=>");
        if (NULL == line || 0 == strlen(line))
        {
            continue;
        }

        /* 0 字节 */
        if (0 == strlen(line))
        {
            free(line);
            continue;
        }

        /* line 处理
         * 1、exit/quit/help 识别
         * 2、启动行识别码, 查看最后一个字符是否为 ";", 不为证明不是一个完整的行, 完整行加入到历史中
         * 3、启动词法语法分析，生成固定结构体
         * 4、根据固定结构体查看是本地执行还是发送至目标端执行
         *  4.1 manager 本地执行
         *  4.2 其它类型的在 manager 端执行
         * 5、在 manager 端执行时, 通过调用中间库执行
         * 
         */
        /* 查看是否为 help/exit/quit */
        if (0 == strncasecmp("help", line, 4))
        {
            /*
             * help 输出
             */
            xscsci_help();
        }
        else if (0 == strncasecmp("exit", line, 4)
                 || 0 == strncasecmp("quit", line, 4))
        {
            /* 关闭与manager的连接 */
            xscsci_exit(0);
        }

        /* 调用词法解析, 查看是否为完整的行 */
        xscsci_prescan_setup(prescan, line, strlen(line));
        hasmore = true;
        while(true == hasmore)
        {
            result = xscsci_prescan_scan(prescan, querybuf);
            if (XSCSCI_PRESCANRESULT_UNSUPPORT == result)
            {
                /* 报错 */
                printf("%s has unsupport char\n", line);
                xsynch_exbufferdata_reset(querybuf);
                break;
            }
            else if (XSCSCI_PRESCANRESULT_EOL == result)
            {
                hasmore = false;

                /* 添加换行符 */
                if (0 < querybuf->len)
                {
                    xsynch_exbufferdata_appendchar(querybuf, '\n');

                    /* 添加到历史数据 */
                    xscsci_input_appendhistory(line, historybuf);
                }
                break;
            }
            else if (PSCAN_SEMICOLON == result)
            {
                /* 
                 * 遇到了分号,有一个完整的语句产生
                 * 1、词法语法分析
                 * 2、将分析后的结果发送到manager处理
                 * 3、将结果展示出来
                 * 4、重置 querybuf
                 */
                /* 如果只有空格和 ';' 字符，那么就等待 */
                if (true == xscsci_scansup_onlysemicolon(querybuf->data))
                {
                    break;
                }

                /*
                 * 添加上下翻页
                 *  1、组装历史数据
                 *  2、添加到历史中
                 *  3、清理资源
                 */
                /* 添加到历史数据 */
                xscsci_input_appendhistory(line, historybuf);

                /* 添加到历史中 */
                xscsci_input_sendhistory(historybuf, &prevhistorybuf);

                /* 调用词法语法分析模块生成具体的结构 */
                xscsci_scan_init(querybuf->data);

                /* 解析 */
                parserret = xscsci_scan_yyparse();
                if (0 != parserret)
                {
                    xsynch_exbufferdata_reset(querybuf);
                    break;
                }

                cmd = g_scanparseresult;

                /* 根据结构体组装消息类型 */

                /* 发送消息并等待 manager 返回 */

                /* 显示返回的内容 */
                xscsci_precommand(xscisc, cmd);

                XSynchGetErrmsg(xscisc->conn);

                XSynchClear(xscisc->conn);

                xsynch_exbufferdata_reset(querybuf);
                xsynch_command_free(cmd);
                hasmore = true;
            }
        }
        xscsci_prescan_finish(prescan);
        free(line);

        /* 调用词法语法解析生成具体的结构 */

        /* 根据结构体组装消息类型 */

        /* 发送消息并等待 manager 返回 */

        /* 显示返回的内容 */
        
    }
    return 0;
}
