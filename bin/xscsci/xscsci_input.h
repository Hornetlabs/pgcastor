#ifndef _XSCSCI_INPUT_H_
#define _XSCSCI_INPUT_H_


/* 输入初始化 */
extern bool xscsci_input_init(void);


/*
 * 通过 readline 获取数据
*/
extern char* xscsci_input_getsinteractive(char* prefix);

extern void xscsci_input_appendhistory(const char *s, xsynch_exbuffer historybuf);

extern void xscsci_input_sendhistory(xsynch_exbuffer historybuf, char** prevhistorybuf);

#endif
