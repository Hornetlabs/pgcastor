#include "app_incl.h"
#include "signal/app_signal.h"
#include "command/cmd.h"

#define SETMASK(mask) sigprocmask(SIG_SETMASK, mask, NULL)

typedef void (*ripplesigfunc)(int signo);

/* Global variables */
sigset_t UnBlockSig, BlockSig;

static void signal_sigterm(int signo)
{
    /* Received exit signal */
    /*
     * Set global variable g_gotsigterm
     */
    g_gotsigterm = true;
    elog(RLOG_INFO, "recv sigterm, process is about to close.");
    return;
}

static void signal_reload(int signo)
{
    if (CAPTURERELOAD_STATUS_UNSET == g_gotsigreload)
    {
        g_gotsigreload = CAPTURERELOAD_STATUS_RELOADING_PARSERWAL;
        elog(RLOG_INFO, "ripple is about to reload.");
    }
    else
    {
        elog(RLOG_INFO, "ripple is processing reload.");
    }
    return;
}

static void signal_sigusr2(int signo)
{
    return;
}

static void signal_initmask(void)
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

static ripplesigfunc signal_pm(int signo, ripplesigfunc func)
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

void signal_init(void)
{
    signal_initmask();

    SETMASK(&BlockSig);

    signal_pm(SIGHUP, signal_reload);
    signal_pm(SIGINT, SIG_IGN);         /* ignored */
    signal_pm(SIGQUIT, SIG_IGN);        /* ignored */
    signal_pm(SIGALRM, SIG_IGN);        /* ignored */
    signal_pm(SIGPIPE, SIG_IGN);        /* ignored */
    signal_pm(SIGUSR1, SIG_IGN);        /* ignored */
    signal_pm(SIGUSR2, signal_sigusr2); /* ignored */
    signal_pm(SIGCHLD, SIG_IGN);        /* ignored */
    signal_pm(SIGTTIN, SIG_IGN);        /* ignored */
    signal_pm(SIGTTOU, SIG_IGN);        /* ignored */
    signal_pm(SIGXFSZ, SIG_IGN);        /* ignored */
    signal_pm(SIGTERM, signal_sigterm); /* wait for children and shut down */
}

void singal_setmask(void)
{
    SETMASK(&UnBlockSig);
}
