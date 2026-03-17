#ifndef _ELOG_H
#define _ELOG_H

#define RLOG_DEBUG              0
#define RLOG_INFO               1
#define RLOG_WARNING            2
#define RLOG_ERROR              3

#define MAX_LOGFORMATSIZE       1024
#define MAX_LOGLINESIZE         8192

/* 初始化错误堆栈 */
extern void log_init(void);
extern bool log_initerrorstack(void);
extern void log_destroyerrorstack(void);
extern char* log_geterrormsg(void);
extern void setloglevel(const char* loglevel);
extern void rlog(const char *filename, int line, int level, const char *format, ...);
#define elog(level, fmt, ...)           rlog(__FILE__, __LINE__, level, fmt, ##__VA_ARGS__)

#define elog_seteloglevel(loglevel)     setloglevel(loglevel)

#endif
