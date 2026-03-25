#ifndef _PARSERTRAIL_DBMETADATA_H
#define _PARSERTRAIL_DBMETADATA_H

/*
 * Database information application
 *  This is independent processing logic, function flow here indicates previous processing is
 * complete, only need to apply here
 */
bool parsertrail_dbmetadataapply(parsertrail* parsertrail, void* data);
void parsertrail_dbmetadataclean(parsertrail* parsertrail, void* data);

#endif
