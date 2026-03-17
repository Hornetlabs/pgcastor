#ifndef _TRANSLOG_WALTIMELINE_H_
#define _TRANSLOG_WALTIMELINE_H_

/* 写时间线文件 */
bool translog_waltimeline_flush(char* walpath, char* filename, char* content);

/*
 * 查看时间线文件是否存在
 *  存在返回 true
 *  不存在返回 false
 */
bool translog_waltimeline_exist(char* walpath, TimeLineID tli);

#endif
