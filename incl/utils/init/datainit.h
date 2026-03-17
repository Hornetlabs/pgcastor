#ifndef _DATAINIT_H
#define _DATAINIT_H

bool datainit_init(char* in_wdata);

void datainit_init_jobnamesubdir(char* dir_path);

/* 临时文件清理 */
void datainit_clear(const char *dir_path);

#endif
