#ifndef XK_PG_PARSER_LOG_ERRLOG_H
#define XK_PG_PARSER_LOG_ERRLOG_H


#define XK_PG_PARSER_DEBUG_SILENCE 0
#define XK_PG_PARSER_DEBUG_WARNING 1
#define XK_PG_PARSER_DEBUG_INFO    2
#define XK_PG_PARSER_DEBUG_DEBUG   3

#define xk_pg_parser_log_errlog(level, fmt, ...) \
        if (level > XK_PG_PARSER_DEBUG_SILENCE) \
            printf(fmt, ##__VA_ARGS__)

#endif
