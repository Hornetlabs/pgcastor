#ifndef _PGCASTOR_EXBUFFERDATA_H_
#define _PGCASTOR_EXBUFFERDATA_H_

#define PGCASTOR_EXPBUFFER_DEFAULTSIZE 1024

typedef struct PGCASTOR_EXBUFFERDATA
{
    char*  data;
    size_t len;
    size_t maxlen;
} pgcastor_exbufferdata;

typedef pgcastor_exbufferdata* pgcastor_exbuffer;

/* initialize content */
extern bool pgcastor_exbufferdata_initdata(pgcastor_exbuffer exbuffer);

/* create an ex buffer */
extern pgcastor_exbuffer pgcastor_exbufferdata_init(void);

/* reset */
extern bool pgcastor_exbufferdata_reset(pgcastor_exbuffer exbuffer);

/*
 * expand capacity
 *  no length and input parameter checking
 */
extern bool pgcastor_exbufferdata_enlarge(pgcastor_exbuffer exbuffer, size_t needed);

/* append content */
extern bool pgcastor_exbufferdata_append(pgcastor_exbuffer exbuffer, const char* fmt, ...);

/* append binary data */
extern bool pgcastor_exbufferdata_appendbinary(pgcastor_exbuffer exbuffer, const char* data, size_t datalen);

/* append string */
extern bool pgcastor_exbufferdata_appendstr(pgcastor_exbuffer exbuffer, const char* data);

/* append character */
extern bool pgcastor_exbufferdata_appendchar(pgcastor_exbuffer exbuffer, char ch);

/* free buffer */
extern void pgcastor_exbufferdata_free(pgcastor_exbuffer exbuffer);

#endif
