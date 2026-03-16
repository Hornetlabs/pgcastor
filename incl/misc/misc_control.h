#ifndef _RIPPLE_MISC_CONTROL_H
#define _RIPPLE_MISC_CONTROL_H


/* 读控制文件 */
void ripple_misc_controldata_load(void);

/* 写 control 文件 */
void ripple_misc_controldata_flush(void);

/* 读控制文件 */
void ripple_misc_controldata_init(void);

/* 清理 */
void ripple_misc_controldata_destroy(void);


/* 设置状态为init */
void ripple_misc_controldata_stat_setinit(void);

/* 设置状态为rewind */
void ripple_misc_controldata_stat_setrewind(void);

/* 设置状态为rewinding */
void ripple_misc_controldata_stat_setrewinding(void);

/* 设置状态为trunning */
void ripple_misc_controldata_stat_setrunning(void);

/* 设置状态为shutdown */
void ripple_misc_controldata_stat_setshutdown(void);

/* 设置状态为recovery */
void ripple_misc_controldata_stat_setrecovery(void);

/* 获取状态值 */
int ripple_misc_controldata_stat_get(void);

/* 设置dbid */
void ripple_misc_controldata_database_set(Oid database);

/* 获取 dbid */
Oid ripple_misc_controldata_database_get(void* invalid);

/* 设置dbname */
void ripple_misc_controldata_dbname_set(char* dbname);

/* 设置monetary */
void ripple_misc_controldata_monetary_set(char* monetary);

/* 获取monetary */
char* ripple_misc_controldata_monetary_get(void);

/* 设置numeric */
void ripple_misc_controldata_numeric_set(char* numeric);

/* 获取numeric */
char* ripple_misc_controldata_numeric_get(void);

/* 设置timezone */
void ripple_misc_controldata_timezone_set(char* timezone);

/* 获取timezone */
char* ripple_misc_controldata_timezone_get(void);

/* 设置orgencoding */
void ripple_misc_controldata_orgencoding_set(char* encoding);

/* 获取orgencoding */
char* ripple_misc_controldata_orgencoding_get(void);

/* 设置dstencoding */
void ripple_misc_controldata_dstencoding_set(char* encoding);

/* 获取dstencoding */
char* ripple_misc_controldata_dstencoding_get(void);

/* 获取dbname */
char* ripple_misc_controldata_dbname_get(void);

#endif
