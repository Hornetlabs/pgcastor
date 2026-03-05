#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"

#define ELOG_ERRORSTACKSIZE             30

typedef struct ELOG_ERRORSTACK
{
    int                     emsglen;
    char                    emsg[MAX_LOGLINESIZE];
} elog_errorstack;

typedef struct ELOG_ERRORSTATCKS
{
    /* 最新的日志记录位置 */
    int                     level;

    /* 错误栈 */
    elog_errorstack         estacks[ELOG_ERRORSTACKSIZE];
} elog_errorstacks;

/* log level string */
static char *m_loglevel[] = {
		"LOG_DEBUG",
		"LOG_INFO",
		"LOG_WARNING",
		"LOG_ERR",
		NULL
};

static bool                     m_init = false;
static int                      m_logfd = -1;
static char                     m_logfilename[RIPPLE_MAXPATH] = { 0 };
static int                      m_year = 0;
static int                      m_month = 0;
static int                      m_day = 0;
static char*                    m_logdir = NULL;
static char*                    m_jobname = NULL;
static elog_errorstacks*        m_errorstack = NULL;

static pthread_mutex_t  m_loglock;

/* 初始化错误堆栈 */
bool ripple_log_initerrorstack(void)
{
    int index = 0;
    if (NULL != m_errorstack)
    {
        return true;
    }

    m_errorstack = rmalloc0(sizeof(elog_errorstacks));
    if (NULL == m_errorstack)
    {
        return false;
    }
    rmemset0(m_errorstack, 0, '\0', sizeof(m_errorstack));


    /* 首先设置为空 */
    for (index = 0; index < ELOG_ERRORSTACKSIZE; index++)
    {
        m_errorstack->level = 0;
        m_errorstack->estacks[index].emsglen = 0;
        rmemset1(m_errorstack->estacks[index].emsg, 0, '\0', MAX_LOGLINESIZE);
    }
    return true;
}

char* ripple_log_geterrormsg(void)
{
    if (NULL == m_errorstack)
    {
        return NULL;
    }

    return m_errorstack->estacks[0].emsg;
}

void ripple_log_destroyerrorstack(void)
{
    if (NULL == m_errorstack)
    {
        return;
    }
    rfree(m_errorstack);
    m_errorstack = NULL;
}

void ripple_log_init(void)
{
    time_t now;
    struct tm *pcnow = NULL;
    struct timeval tval;

    time(&now);
    gettimeofday(&tval,NULL);
    pcnow=localtime(&now);
    if(pcnow == NULL)
    {
        return;
    }

    /* 获取日志存放路径 */
    m_logdir = guc_getConfigOption(RIPPLE_CFG_KEY_LOG_DIR);
    if(NULL == m_logdir)
    {
        m_logdir = (char*)rmalloc0((strlen("log") + 1));
        rmemset0(m_logdir, 0 , '\0', (strlen("log") + 1));
        rmemcpy0(m_logdir, 0 , "log", strlen("log"));
    }

    MakeDir(m_logdir);

    /* 获取名称 */
    m_jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);
    if(NULL == m_jobname)
    {
        m_jobname = "ripple";
    }

    m_year = (pcnow->tm_year + 1900);
    m_month = (pcnow->tm_mon + 1);
    m_day = pcnow->tm_mday;

    snprintf(m_logfilename, RIPPLE_MAXPATH, "%s/%s_%d-%d-%d-%02d%02d%02d.log",
                                                m_logdir,
                                                m_jobname,
                                                pcnow->tm_year + 1900,
                                                pcnow->tm_mon + 1,
                                                pcnow->tm_mday,
                                                pcnow->tm_hour,
                                                pcnow->tm_min,
                                                pcnow->tm_sec);

    /* 打开文件 */
    m_logfd = BasicOpenFile(m_logfilename, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (m_logfd  < 0)
    {
        printf("open file %s error %s\n", m_logfilename, strerror(errno));
        syslog(LOG_INFO, "open file %s error %s", m_logfilename, strerror(errno));
        exit(-1);
    }

    /* 日志清理 */
    FileTruncate(m_logfd, 0);

    printf("\nThe logs of the ripple will be stored in %s\n", m_logfilename);
    m_init = true;

    ripple_thread_mutex_init(&m_loglock, NULL);
}

/* 日志打印 */
void ripple_log(const char *filename, int line, int level, const char *format, ...)
{
	int pos = 0;
    int newfd = -1;
	time_t now;
    char logFormat[] = {"\n %d-%d-%d:%d:%d:%d:%d|%s:%d|%s|"};
    char logfilname[RIPPLE_MAXPATH] = { 0 };
	char logInfo[MAX_LOGLINESIZE] = {0};
	struct timeval tval;
	struct tm *pcnow;
	va_list ap;

	if(level < g_loglevel)
	{
		return;
	}

	time(&now);
	gettimeofday(&tval,NULL);
	pcnow=localtime(&now);
	if(pcnow == NULL){
		return;
	}

	pos = snprintf(logInfo, MAX_LOGLINESIZE, logFormat,
			pcnow->tm_year+1900, pcnow->tm_mon+1, pcnow->tm_mday,
			pcnow->tm_hour, pcnow->tm_min, pcnow->tm_sec, tval.tv_usec/1000,
			filename,
			line,
			m_loglevel[level]);

	if(pos == (MAX_LOGLINESIZE-1)){
		return;
	}

	va_start(ap, format);
	vsnprintf(logInfo+pos, MAX_LOGLINESIZE - pos, format, ap);
	va_end(ap);

    if(true == m_init)
    {
        ripple_thread_lock(&m_loglock);
    }

    if(true == m_init)
    {
        if(-1 == m_logfd)
        {
            m_year = (pcnow->tm_year + 1900);
            m_month = (pcnow->tm_mon + 1);
            m_day = pcnow->tm_mday;
            snprintf(m_logfilename, RIPPLE_MAXPATH, "%s/%s_%d-%d-%d-%02d%02d%02d.log",
                                                    m_logdir,
                                                    m_jobname,
                                                    pcnow->tm_year + 1900,
                                                    pcnow->tm_mon + 1,
                                                    pcnow->tm_mday,
                                                    pcnow->tm_hour,
                                                    pcnow->tm_min,
                                                    pcnow->tm_sec);
            
            m_logfd = BasicOpenFile(m_logfilename, O_RDWR | O_CREAT | RIPPLE_BINARY);
            if (m_logfd  < 0)
            {
                printf("open file %s error %s\n", m_logfilename, strerror(errno));
                syslog(LOG_INFO, "open file %s error %s", m_logfilename, strerror(errno));
            }
        }
        else
        {
            snprintf(logfilname, RIPPLE_MAXPATH, "%s/%s_%d-%d-%d-%02d%02d%02d.log",
                                                    m_logdir,
                                                    m_jobname,
                                                    pcnow->tm_year + 1900,
                                                    pcnow->tm_mon + 1,
                                                    pcnow->tm_mday,
                                                    pcnow->tm_hour,
                                                    pcnow->tm_min,
                                                    pcnow->tm_sec);
            
            /* 日期发生了切换，那么切换日志文件 */
            if(m_year != (pcnow->tm_year + 1900)
                || m_month != (pcnow->tm_mon + 1)
                || m_day != pcnow->tm_mday)
            {
                /* 切换 */
                newfd = BasicOpenFile(logfilname, O_RDWR | O_CREAT | RIPPLE_BINARY);
                if(newfd > 0)
                {
                    CloseTransientFile(m_logfd);
                }
                m_logfd = newfd;
                memcpy(m_logfilename, logfilname, RIPPLE_MAXPATH);
                m_year = (pcnow->tm_year + 1900);
                m_month = (pcnow->tm_mon + 1);
                m_day = pcnow->tm_mday;
            }
        }
        FileWrite(m_logfd, logInfo, strlen(logInfo));

        if(false == g_closestd)
        {
            printf("%s\n", logInfo);
        }
    }
    else
    {
        printf("%s\n", logInfo);
        syslog(LOG_INFO, "%s", logInfo);
    }

    if (level >= RLOG_WARNING && NULL != m_errorstack)
    {
        /* 复制错误信息到 */
        if (ELOG_ERRORSTACKSIZE == m_errorstack->level)
        {
            m_errorstack->level = 0;
        }

        rmemset1(m_errorstack->estacks[m_errorstack->level].emsg, 0, '\0', MAX_LOGLINESIZE);
        snprintf(m_errorstack->estacks[m_errorstack->level].emsg, MAX_LOGLINESIZE,
                 "ERROR: %s", logInfo + pos);
        m_errorstack->level++;
    }

    if(true == m_init)
    {
        ripple_thread_unlock(&m_loglock);
    }

    if(level >= RLOG_ERROR)
    {
        if(0 != g_mainthrid && g_mainthrid == pthread_self())
        {
            exit(-1);
        }
        else
        {
            ripple_pthread_exit(NULL);
        }
    }
}


void ripple_setloglevel(const char* loglevel)
{
	if(strlen(loglevel) == strlen("DEBUG")
		&& 0 == strcasecmp(loglevel, "DEBUG"))
	{
		g_loglevel = RLOG_DEBUG;
	}
	else if(strlen(loglevel) == strlen("INFO")
		&& 0 == strcasecmp(loglevel, "INFO"))
	{
		g_loglevel = RLOG_INFO;
	}
	else if(strlen(loglevel) == strlen("WARNING")
		&& 0 == strcasecmp(loglevel, "WARNING"))
	{
		g_loglevel = RLOG_WARNING;
	}
	else if(strlen(loglevel) == strlen("ERROR")
		&& 0 == strcasecmp(loglevel, "ERROR"))
	{
		g_loglevel = RLOG_ERROR;
	}
	else
	{
		elog(RLOG_ERROR, "unknown log level:%s, support: DEBUG INFO WARNING ERROR", loglevel);
	}
}

