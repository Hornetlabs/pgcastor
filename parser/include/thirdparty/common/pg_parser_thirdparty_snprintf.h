#ifndef PG_PARSER_THIRDPARTY_SNPRINTF_H
#define PG_PARSER_THIRDPARTY_SNPRINTF_H

typedef struct
{
    char*   bufptr;   /* next buffer output position */
    char*   bufstart; /* first buffer element */
    char*   bufend;   /* last+1 buffer element, or NULL */
    /* bufend == NULL is for sprintf, where we assume buf is big enough */
    FILE*   stream; /* eventual output destination, or NULL */
    int32_t nchars; /* # chars sent to stream, or dropped */
    bool    failed; /* call is a failure; errno is set */
} PrintfTarget;

extern int32_t pg_parser_strfromd(char* str, size_t count, int32_t precision, double value);

#endif
