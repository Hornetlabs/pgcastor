#include "ripple_app_incl.h"
#include "signal/ripple_signal.h"
#include "command/ripple_cmd.h"

#define RIPPLE_SETMASK(mask)	sigprocmask(SIG_SETMASK, mask, NULL)

typedef void (*ripplesigfunc) (int signo);

/* Global variables */
sigset_t	UnBlockSig,
			BlockSig;

static void ripple_signal_sigterm(int signo)
{
    /* 接收到退出信号 */
    /*
     * 设置全局变量 g_gotsigterm
    */
    g_gotsigterm = true;
    elog(RLOG_INFO, "recv sigterm, process is about to close.");
    return;
}

static void ripple_signal_reload(int signo)
{
    if (RIPPLE_CAPTURERELOAD_STATUS_UNSET == g_gotsigreload)
    {
        g_gotsigreload = RIPPLE_CAPTURERELOAD_STATUS_RELOADING_PARSERWAL;
        elog(RLOG_INFO, "ripple is about to reload.");
    }
    else
    {
        elog(RLOG_INFO, "ripple is processing reload.");
    }
    return;
}

static void ripple_signal_sigusr2(int signo)
{
    return;
}


static void ripple_signal_initmask(void)
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

static ripplesigfunc ripple_signal_pm(int signo, ripplesigfunc func)
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

void ripple_signal_init(void)
{
    ripple_signal_initmask();

    RIPPLE_SETMASK(&BlockSig);

    ripple_signal_pm(SIGHUP, ripple_signal_reload);
    ripple_signal_pm(SIGINT, SIG_IGN); /* ignored */
    ripple_signal_pm(SIGQUIT, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGALRM, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGPIPE, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGUSR1, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGUSR2, ripple_signal_sigusr2);	/* ignored */
    ripple_signal_pm(SIGCHLD, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGTTIN, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGTTOU, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGXFSZ, SIG_IGN);	/* ignored */
    ripple_signal_pm(SIGTERM, ripple_signal_sigterm);	/* wait for children and shut down */
}

void ripple_singal_setmask(void)
{
    RIPPLE_SETMASK(&UnBlockSig);
}
