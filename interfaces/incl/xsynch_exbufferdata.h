#ifndef _XSYNCH_EXBUFFERDATA_H_
#define _XSYNCH_EXBUFFERDATA_H_

#define XSYNCH_EXPBUFFER_DEFAULTSIZE 1024

typedef struct XSYNCH_EXBUFFERDATA
{
    char*  data;
    size_t len;
    size_t maxlen;
} xsynch_exbufferdata;

typedef xsynch_exbufferdata* xsynch_exbuffer;

/* initialize content */
extern bool xsynch_exbufferdata_initdata(xsynch_exbuffer exbuffer);

/* create an ex buffer */
extern xsynch_exbuffer xsynch_exbufferdata_init(void);

/* reset */
extern bool xsynch_exbufferdata_reset(xsynch_exbuffer exbuffer);

/*
 * expand capacity
 *  no length and input parameter checking
 */
extern bool xsynch_exbufferdata_enlarge(xsynch_exbuffer exbuffer, size_t needed);

/* append content */
extern bool xsynch_exbufferdata_append(xsynch_exbuffer exbuffer, const char* fmt, ...);

/* append binary data */
extern bool xsynch_exbufferdata_appendbinary(xsynch_exbuffer exbuffer, const char* data, size_t datalen);

/* append string */
extern bool xsynch_exbufferdata_appendstr(xsynch_exbuffer exbuffer, const char* data);

/* append character */
extern bool xsynch_exbufferdata_appendchar(xsynch_exbuffer exbuffer, char ch);

/* free buffer */
extern void xsynch_exbufferdata_free(xsynch_exbuffer exbuffer);

#endif
