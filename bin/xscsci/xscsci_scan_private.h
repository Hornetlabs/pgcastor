#ifndef _XSCSCI_SCAN_PRIVATE_H_
#define _XSCSCI_SCAN_PRIVATE_H_

extern xsynch_cmd* g_scanparseresult;

extern void xscsci_scan_init(const char* str);
extern void xscsci_scan_finish(void);
extern int xscsci_scan_yyparse(void);
extern int xscsci_scan_yylex(void);
extern void xscsci_scan_yyerror(const char* str);

#endif
