#ifndef _XSCSCI_SCANSUP_H_
#define _XSCSCI_SCANSUP_H_

char* xscsci_scansup_downcasetruncateident(const char *ident, int len, bool warn);

/* 排除无需关注的字符后, 查看是否只有分号 */
bool xscsci_scansup_onlysemicolon(const char* str);

#endif
