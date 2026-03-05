#ifndef _RIPPLE_DATAINIT_H
#define _RIPPLE_DATAINIT_H

bool ripple_datainit_init(char* in_wdata);

void ripple_datainit_init_jobnamesubdir(char* dir_path);

/* 临时文件清理 */
void ripple_datainit_clear(const char *dir_path);

#endif
