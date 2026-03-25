#ifndef _XSCSCI_SCANSUP_H_
#define _XSCSCI_SCANSUP_H_

char* xscsci_scansup_downcasetruncateident(const char* ident, int len, bool warn);

/* check if only semicolon remains after excluding unimportant characters */
bool xscsci_scansup_onlysemicolon(const char* str);

#endif
