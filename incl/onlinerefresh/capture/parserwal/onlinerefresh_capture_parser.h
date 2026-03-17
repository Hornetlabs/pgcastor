#ifndef ONLINEREFRESH_CAPTURE_PARSER_H
#define ONLINEREFRESH_CAPTURE_PARSER_H

typedef struct ONLINEREFRESH_CAPTUREPARSER
{
    decodingcontext*                 decodingctx;
} onlinerefresh_captureparser;

extern onlinerefresh_captureparser *onlinerefresh_captureparser_init(void);
/*
 * 设置解析器需要的基础信息
 *  1、字符集/时区/源字符集/目标字符集
 *  2、加载系统字典
 *  3、构建checkpoint信息
*/
extern void onlinerefresh_captureparser_loadmetadata(onlinerefresh_captureparser* olcparser);

extern bool onlinerefresh_captureparser_datasetinit(decodingcontext *ctx, onlinerefresh_capture* onlinerefresh);
extern void *onlinerefresh_captureparser_main(void* args);
extern void onlinerefresh_captureparser_free(void* args);

#endif
