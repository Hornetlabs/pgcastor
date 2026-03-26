#ifndef PG_PARSER_LOG_ERRLOG_H
#define PG_PARSER_LOG_ERRLOG_H

#define PG_PARSER_DEBUG_SILENCE 0
#define PG_PARSER_DEBUG_WARNING 1
#define PG_PARSER_DEBUG_INFO    2
#define PG_PARSER_DEBUG_DEBUG   3

#define pg_parser_log_errlog(level, fmt, ...) \
    if (level > PG_PARSER_DEBUG_SILENCE)      \
    printf(fmt, ##__VA_ARGS__)

#endif
