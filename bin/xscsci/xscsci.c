#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>

#include "app_c.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xscsci_input.h"
#include "xscsci_prescan.h"
#include "xscsci_scan_private.h"
#include "xscsci_scansup.h"
#include "xscsci.h"
#include "xscsci_precommand.h"

#define XSCSCI_SETMASK(mask) sigprocmask(SIG_SETMASK, mask, NULL)

typedef void (*xscscisigfunc)(int signo);

/* Global variables */
sigset_t UnBlockSig, BlockSig;

/* initialize xscsci stat */
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
    struct sigaction act, oact;

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
    {
        return SIG_ERR;
    }
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
 * get XSYNCH environment variable
 * check if configured and if it is a directory
 */
static bool xscsci_getxsynchhome(xsciscistat* xscisc)
{
    struct stat st;

    xscisc->xsynchhome = getenv("XSYNCH");

    /* environment variable not found */
    if (NULL == xscisc->xsynchhome)
    {
        printf("XSYNCH configuration error: not set XSYNCH \n");
        return false;
    }

    /* check if exists */
    if (stat(xscisc->xsynchhome, &st) != 0)
    {
        printf("XSYNCH configuration error: %s (%s)\n", xscisc->xsynchhome, strerror(errno));
        return false;
    }

    /* check if is directory */
    if (!S_ISDIR(st.st_mode))
    {
        printf("XSYNCH configuration error: %s is not a directory\n", xscisc->xsynchhome);
        return false;
    }
    return true;
}

static bool xscsci_parseargs(int argc, char** argv, char** connstr)
{
    int  index_argc = 1;
    char port[128] = {'\0'};
    char host[512] = {'\0'};

    memset(port, 0, 128);
    memset(host, 0, 512);

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
    memset(*connstr, 0, 512);

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

/* help documentation */
static void xscsci_help(void)
{
    printf("you can use xscsci, the command-line to xsynch.\n");

    /* create command support */
    printf("---------use create command create a job----------------\n");
    printf("create manager              create progress\n");
    printf("create pgreceivelog\n");
    printf("create capture              create integrate\n");
    printf("\n");

    /* edit support */
    printf("---------use edit command edit job config file----------\n");
    printf("edit manager\n");
    printf("edit pgreceivelog\n");
    printf("edit capture                edit integrate\n");
    printf("\n");

    /* init support */
    printf("---------use init command init job work dir-------------\n");
    printf("init manager\n");
    printf("init pgreceivelog\n");
    printf("init capture                init integrate\n");
    printf("\n");

    /* start support */
    printf("---------use start command start job -------------------\n");
    printf("start manager\n");
    printf("start pgreceivelog\n");
    printf("start capture               start integrate\n");
    printf("\n");

    /* stop support */
    printf("---------use stop command stop job ---------------------\n");
    printf("stop manager\n");
    printf("stop pgreceivelog\n");
    printf("stop capture                stop integrate\n");
    printf("\n");

    /* alter support */
    printf("---------use alter command alter progress member--------\n");
    printf("alter progress\n");

    /* reload support */
    printf("---------use reload command reload config file----------\n");
    printf("reload manager\n");
    printf("reload pgreceivelog\n");
    printf("reload capture              reload integrate\n");
    printf("\n");

    /* remove support */
    printf("---------use remove command remove config file----------\n");
    printf("remove manager\n");
    printf("remove pgreceivelog\n");
    printf("remove capture              remove integrate\n");
    printf("\n");

    /* drop support */
    printf("---------use drop command drop job----------------------\n");
    printf("drop manager                drop progress\n");
    printf("drop pgreceivelog\n");
    printf("drop capture                drop integrate\n");
    printf("\n");

    /* info support */
    printf("---------use info command view job base info------------\n");
    printf("info manager                info progress\n");
    printf("info pgreceivelog\n");
    printf("info capture                info integrate\n");
    printf("\n");

    /* watch support */
    printf("---------use watch command view job info every seconds--\n");
    printf("watch manager               watch progress\n");
    printf("watch pgreceivelog\n");
    printf("watch capture               watch integrate\n");
    printf("\n");

    /* watch support */
    printf("use exit/quit command exit xscsci\n");
}

/* program exit */
static void xscsci_exit(int rcode)
{
    /*
     * 1. release connection to manager
     * 2. reclaim resources
     */
    exit(rcode);
}

int main(int argc, char** argv)
{
    bool                     hasmore = false;
    int                      parserret = 0;
    xscsci_prescanresult     result = 0;
    char*                    line = NULL;
    char*                    connstr = NULL;
    char*                    prevhistorybuf = NULL;
    volatile xsynch_exbuffer querybuf = NULL;
    xscsci_prescan*          prescan = NULL;
    xsynch_cmd*              cmd = NULL;
    xsynch_exbuffer          historybuf = NULL;
    xsciscistat*             xscisc = NULL;

    if (false == xscsci_parseargs(argc, argv, &connstr))
    {
        xscsci_exit(1);
    }

    /* initialize */
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
     * connect to manager
     */
    xscisc->conn = XSynchSetParam(connstr);
    XSynchConn(xscisc->conn);
    free(connstr);

    /* readline initialization */
    xscsci_input_init();

    xscsci_singal_setmask();

    while (1)
    {
        fflush(stdout);

        /* get readline data */
        line = xscsci_input_getsinteractive("xscsci=>");
        if (NULL == line || 0 == strlen(line))
        {
            continue;
        }

        /* 0 bytes */
        if (0 == strlen(line))
        {
            free(line);
            continue;
        }

        /* line processing
         * 1. exit/quit/help recognition
         * 2. start line identifier, check if last character is ";", if not, line is incomplete,
         * complete lines added to history
         * 3. start lexical/syntax analysis, generate fixed structure
         * 4. based on fixed structure, check if local execution or sent to target for execution
         *  4.1 manager local execution
         *  4.2 other types execute on manager side
         * 5. when executing on manager side, through calling middleware library
         *
         */
        /* check if help/exit/quit */
        if (0 == strncasecmp("help", line, 4))
        {
            /*
             * help output
             */
            xscsci_help();
        }
        else if (0 == strncasecmp("exit", line, 4) || 0 == strncasecmp("quit", line, 4))
        {
            /* close connection with manager */
            xscsci_exit(0);
        }

        /* call lexical parser, check if complete line */
        xscsci_prescan_setup(prescan, line, strlen(line));
        hasmore = true;
        while (true == hasmore)
        {
            result = xscsci_prescan_scan(prescan, querybuf);
            if (XSCSCI_PRESCANRESULT_UNSUPPORT == result)
            {
                /* error */
                printf("%s has unsupport char\n", line);
                xsynch_exbufferdata_reset(querybuf);
                break;
            }
            else if (XSCSCI_PRESCANRESULT_EOL == result)
            {
                hasmore = false;

                /* add newline */
                if (0 < querybuf->len)
                {
                    xsynch_exbufferdata_appendchar(querybuf, '\n');

                    /* add to history data */
                    xscsci_input_appendhistory(line, historybuf);
                }
                break;
            }
            else if (PSCAN_SEMICOLON == result)
            {
                /*
                 * encountered semicolon, a complete statement produced
                 * 1. lexical/syntax analysis
                 * 2. send analysis result to manager for processing
                 * 3. display the result
                 * 4. reset querybuf
                 */
                /* if only spaces and ';' characters, then wait */
                if (true == xscsci_scansup_onlysemicolon(querybuf->data))
                {
                    break;
                }

                /*
                 * add up/down page navigation
                 *  1. assemble history data
                 *  2. add to history
                 *  3. clean up resources
                 */
                /* add to history data */
                xscsci_input_appendhistory(line, historybuf);

                /* add to history */
                xscsci_input_sendhistory(historybuf, &prevhistorybuf);

                /* call lexical/syntax analysis module to generate specific structure */
                xscsci_scan_init(querybuf->data);

                /* parse */
                parserret = xscsci_scan_yyparse();
                if (0 != parserret)
                {
                    xsynch_exbufferdata_reset(querybuf);
                    break;
                }

                cmd = g_scanparseresult;

                /* assemble message type based on structure */

                /* send message and wait for manager response */

                /* display returned content */
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

        /* call lexical/syntax analysis to generate specific structure */

        /* assemble message type based on structure */

        /* send message and wait for manager response */

        /* display returned content */
    }
    return 0;
}
