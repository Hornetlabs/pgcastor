#ifndef _MISC_CONTROL_H
#define _MISC_CONTROL_H

/* read control file */
void misc_controldata_load(void);

/* write control file */
void misc_controldata_flush(void);

/* read and initialize control file */
void misc_controldata_init(void);

/* cleanup */
void misc_controldata_destroy(void);

/* set status to init */
void misc_controldata_stat_setinit(void);

/* set status to rewind */
void misc_controldata_stat_setrewind(void);

/* set status to rewinding */
void misc_controldata_stat_setrewinding(void);

/* set status to running */
void misc_controldata_stat_setrunning(void);

/* set status to shutdown */
void misc_controldata_stat_setshutdown(void);

/* set status to recovery */
void misc_controldata_stat_setrecovery(void);

/* get status value */
int misc_controldata_stat_get(void);

/* set database id */
void misc_controldata_database_set(Oid database);

/* get database id */
Oid misc_controldata_database_get(void* invalid);

/* set database name */
void misc_controldata_dbname_set(char* dbname);

/* set monetary locale */
void misc_controldata_monetary_set(char* monetary);

/* get monetary locale */
char* misc_controldata_monetary_get(void);

/* set numeric locale */
void misc_controldata_numeric_set(char* numeric);

/* get numeric locale */
char* misc_controldata_numeric_get(void);

/* set timezone */
void misc_controldata_timezone_set(char* timezone);

/* get timezone */
char* misc_controldata_timezone_get(void);

/* set source encoding */
void misc_controldata_orgencoding_set(char* encoding);

/* get source encoding */
char* misc_controldata_orgencoding_get(void);

/* set destination encoding */
void misc_controldata_dstencoding_set(char* encoding);

/* get destination encoding */
char* misc_controldata_dstencoding_get(void);

/* get database name */
char* misc_controldata_dbname_get(void);

#endif
