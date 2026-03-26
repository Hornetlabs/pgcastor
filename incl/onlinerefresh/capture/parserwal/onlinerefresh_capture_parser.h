#ifndef ONLINEREFRESH_CAPTURE_PARSER_H
#define ONLINEREFRESH_CAPTURE_PARSER_H

typedef struct ONLINEREFRESH_CAPTUREPARSER
{
    decodingcontext* decodingctx;
} onlinerefresh_captureparser;

extern onlinerefresh_captureparser* onlinerefresh_captureparser_init(void);
/*
 * Set basic information needed by parser
 *  1、Charset/timezone/source charset/target charset
 *  2、Load system dictionary
 *  3、Build checkpoint information
 */
extern void onlinerefresh_captureparser_loadmetadata(onlinerefresh_captureparser* olcparser);

extern bool onlinerefresh_captureparser_datasetinit(decodingcontext*       ctx,
                                                    onlinerefresh_capture* onlinerefresh);
extern void* onlinerefresh_captureparser_main(void* args);
extern void onlinerefresh_captureparser_free(void* args);

#endif
