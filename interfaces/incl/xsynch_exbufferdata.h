#ifndef _XSYNCH_EXBUFFERDATA_H_
#define _XSYNCH_EXBUFFERDATA_H_

#define XSYNCH_EXPBUFFER_DEFAULTSIZE            1024

typedef struct XSYNCH_EXBUFFERDATA
{
    char*               data;
    size_t              len;
    size_t              maxlen;
} xsynch_exbufferdata;

typedef xsynch_exbufferdata *xsynch_exbuffer;

/* 初始化内容 */
extern bool xsynch_exbufferdata_initdata(xsynch_exbuffer exbuffer);

/* 生成一个ex buffer */
extern xsynch_exbuffer xsynch_exbufferdata_init(void);

/* 重置 */
extern bool xsynch_exbufferdata_reset(xsynch_exbuffer exbuffer);

/* 
 * 扩容
 *  不做长度和入参检测
*/
extern bool xsynch_exbufferdata_enlarge(xsynch_exbuffer exbuffer, size_t needed);

/* 添加内容 */
extern bool xsynch_exbufferdata_append(xsynch_exbuffer exbuffer, const char *fmt,...);

/* 添加字符串 */
extern bool xsynch_exbufferdata_appendbinary(xsynch_exbuffer exbuffer, const char *data, size_t datalen);

/* 合并字符串 */
extern bool xsynch_exbufferdata_appendstr(xsynch_exbuffer exbuffer, const char *data);

/* 添加字符 */
extern bool xsynch_exbufferdata_appendchar(xsynch_exbuffer exbuffer, char ch);

/* 释放空间 */
extern void xsynch_exbufferdata_free(xsynch_exbuffer exbuffer);

#endif
