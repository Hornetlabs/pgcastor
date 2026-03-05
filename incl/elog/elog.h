#ifndef _RIPPLE_ELOG_H
#define _RIPPLE_ELOG_H

#define RLOG_DEBUG              0
#define RLOG_INFO               1
#define RLOG_WARNING            2
#define RLOG_ERROR              3

#define MAX_LOGFORMATSIZE       1024
#define MAX_LOGLINESIZE         8192

/* 初始化错误堆栈 */
extern void ripple_log_init(void);
extern bool ripple_log_initerrorstack(void);
extern void ripple_log_destroyerrorstack(void);
extern char* ripple_log_geterrormsg(void);
extern void ripple_setloglevel(const char* loglevel);
extern void ripple_log(const char *filename, int line, int level, const char *format, ...);
#define elog(level, fmt, ...)           ripple_log(__FILE__, __LINE__, level, fmt, ##__VA_ARGS__)

#define elog_seteloglevel(loglevel)     ripple_setloglevel(loglevel)

#endif
