#ifndef _RIPPLE_PARSERTRAIL_DBMETADATA_H
#define _RIPPLE_PARSERTRAIL_DBMETADATA_H

/*
 * 数据库信息应用
 *  此处为独立的处理逻辑，函数流程到此说明前期的处理已经完成，在这只需要应用即可
*/
bool ripple_parsertrail_dbmetadataapply(ripple_parsertrail* parsertrail, void* data);
void ripple_parsertrail_dbmetadataclean(ripple_parsertrail* parsertrail, void* data);

#endif
