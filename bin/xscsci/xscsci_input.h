#ifndef _XSCSCI_INPUT_H_
#define _XSCSCI_INPUT_H_

/* initialize input */
extern bool xscsci_input_init(void);

/*
 * get data through readline
 */
extern char* xscsci_input_getsinteractive(char* prefix);

extern void xscsci_input_appendhistory(const char* s, pgcastor_exbuffer historybuf);

extern void xscsci_input_sendhistory(pgcastor_exbuffer historybuf, char** prevhistorybuf);

#endif
