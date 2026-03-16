#ifndef RIPPLE_ONLINEREFRESH_CAPTURE_PARSER_H
#define RIPPLE_ONLINEREFRESH_CAPTURE_PARSER_H

typedef struct RIPPLE_ONLINEREFRESH_CAPTUREPARSER
{
    ripple_decodingcontext*                 decodingctx;
} ripple_onlinerefresh_captureparser;

extern ripple_onlinerefresh_captureparser *ripple_onlinerefresh_captureparser_init(void);
/*
 * 设置解析器需要的基础信息
 *  1、字符集/时区/源字符集/目标字符集
 *  2、加载系统字典
 *  3、构建checkpoint信息
*/
extern void ripple_onlinerefresh_captureparser_loadmetadata(ripple_onlinerefresh_captureparser* olcparser);

extern bool ripple_onlinerefresh_captureparser_datasetinit(ripple_decodingcontext *ctx, ripple_onlinerefresh_capture* onlinerefresh);
extern void *ripple_onlinerefresh_captureparser_main(void* args);
extern void ripple_onlinerefresh_captureparser_free(void* args);

#endif
