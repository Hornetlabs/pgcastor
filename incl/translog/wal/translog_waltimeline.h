#ifndef _TRANSLOG_WALTIMELINE_H_
#define _TRANSLOG_WALTIMELINE_H_

/* Write timeline file */
bool translog_waltimeline_flush(char* walpath, char* filename, char* content);

/*
 * Check if timeline file exists
 *  Return true if exists
 *  Return false if not exists
 */
bool translog_waltimeline_exist(char* walpath, TimeLineID tli);

#endif
