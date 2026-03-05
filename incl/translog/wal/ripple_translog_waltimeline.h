#ifndef _RIPPLE_TRANSLOG_WALTIMELINE_H_
#define _RIPPLE_TRANSLOG_WALTIMELINE_H_

/* 写时间线文件 */
bool ripple_translog_waltimeline_flush(char* walpath, char* filename, char* content);

/*
 * 查看时间线文件是否存在
 *  存在返回 true
 *  不存在返回 false
 */
bool ripple_translog_waltimeline_exist(char* walpath, TimeLineID tli);

#endif
