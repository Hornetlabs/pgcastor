#ifndef _VARSTR_H_
#define _VARSTR_H_

typedef struct VARSTR
{
    /* 使用的长度 */
    uint64_t                start;

    /* data 长度 */
    uint64_t                size;

    /* 数据区 */
    uint8_t*                data;
} varstr;

/* 初始化 */
extern varstr* varstr_init(uint64 len);

/* 重置 */
extern bool varstr_reset(varstr* vstr);

/* 
 * 扩容
 *  不做长度和入参检测
*/
extern bool varstr_enlarge(varstr* vstr, uint64 needed);

/* 添加内容 */
extern bool varstr_append(varstr* vstr, const char *fmt,...);

/* 添加字符串 */
extern bool varstr_appendbinary(varstr* vstr, const char *data, uint64 datalen);

/* 合并字符串 */
extern bool varstr_appendstr(varstr* vstr, const char *data);

/* 添加字符 */
extern bool varstr_appendchar(varstr* vstr, char ch);

/* 释放 */
extern void varstr_free(varstr* vstr);

#endif
