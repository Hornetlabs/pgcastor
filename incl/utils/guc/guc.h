#ifndef _GUC_H
#define _GUC_H

#define GCTIME_DEFAULT               30

/*-----------------capture config begin------------------------*/
#define CFG_KEY_URL                          "url"
#define CFG_KEY_DATA                         "data"
#define CFG_KEY_DBTYPE                       "dbtype"
#define CFG_KEY_DBVERION                     "dbversion"
#define CFG_KEY_DBNAME                       "dbname"
#define CFG_KEY_CATALOGSCHEMA                "catalog_schema"
#define CFG_KEY_TRAIL_MAX_SIZE               "trail_max_size"
#define CFG_KEY_COMPATIBILITY                "compatibility"
#define CFG_KEY_CAPTURE_BUFFER               "capture_buffer"
#define CFG_KEY_WAL_DIR                      "wal_dir"
#define CFG_KEY_LOG_LEVEL                    "log_level"
#define CFG_KEY_TRAIL_DIR                    "trail_dir"
#define CFG_KEY_HOST                         "host"
#define CFG_KEY_PORT                         "port"
#define CFG_KEY_DDL                          "ddl"
#define CFG_KEY_MAX_WORK_PER_REFRESH         "max_work_per_refresh"
#define CFG_KEY_MAX_PAGE_PER_REFRESHSHARDING "max_page_per_refreshsharding"
#define CFG_KEY_REFRESHSTRAGETY              "refreshstragety"
#define CFG_KEY_COMPRESS_ALGORITHM           "compress_algorithm"
#define CFG_KEY_COMPRESS_LEVEL               "compress_level"
#define CFG_KEY_TCP_KEEPALIVE                "tcp_keepalive"
#define CFG_KEY_TCP_USER_TIMEOUT             "tcp_user_timeout"
#define CFG_KEY_TCP_KEEPALIVES_IDLE          "tcp_keepalives_idle"
#define CFG_KEY_TCP_KEEPALIVES_INTERVAL      "tcp_keepalives_interval"
#define CFG_KEY_TCP_KEEPALIVES_COUNT         "tcp_keepalives_count"
#define CFG_KEY_LOG_DIR                      "log_dir"
#define CFG_KEY_JOBNAME                      "jobname"
#define CFG_KEY_TRUNCATETABLE                "truncate_table"
#define CFG_KEY_FTPURL                       "ftpurl"
#define CFG_KEY_FTPDATA                      "ftpdata"
#define CFG_KEY_FTPSSL                       "enable_ssl"
#define CFG_KEY_GCTIME                       "gctime"
#define CFG_KEY_TXBUNDLESIZE                 "txbundlesize"
#define CFG_KEY_MERGETXN                     "mergetxn"
#define CFG_KEY_INTEGRATE_BUFFER             "integrate_buffer"
#define CFG_KEY_INTEGRATE_METHOD             "integrate_method"

#define CFG_KEY_ENABLE_REPLICA_IDENTITY      "enable_replica_identity"
#define CFG_KEY_XMANAGER_PORT                "xport"

#define CFG_KEY_CAPTURE_WAL_DECRYPT          "wal_decrypt"

/*
 * receivewal 配置项
*/
#define CFG_KEY_PRIMARY_CONN_INFO            "primary_conn_info"
#define CFG_KEY_SLOT_NAME                    "slot_name"
#define CFG_KEY_TIMELINE                     "timeline"
#define CFG_KEY_STARTPOS                     "startpos"
#define CFG_KEY_RESTORE_COMMAND              "restore_command"


/*
 * xmanager 配置项
*/
#define CFG_KEY_UNIXDOMAINDIR                "unixdomaindir"

/*-----------------capture config   end------------------------*/

typedef enum
{
	PGC_S_DEFAULT,
	PGC_S_FILE
} GucSource;

typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *filename;
	struct ConfigVariable *next;
} ConfigVariable;

typedef enum
{
	PGC_INTERNAL,
	PGC_SIGHUP,
} GucFlags;

/* capture 配置信息 */
typedef struct CAPTURE_CFG
{
    int             ddl;                        /* ddl解析                    */
    int             trailmaxsize;               /* trail 文件的最大值           */
    int             maxbuffersize;              /* 事务缓存占用的空间            */
    int             compatibility;              /* 兼容版本                    */
    int             port;
    int             xport;
    int             tcp_keepalive;
    int             tcp_user_timeout;
    int             tcp_keepalives_idle;
    int             tcp_keepalives_interval;
    int             tcp_keepalives_count;
    int             ddl_identity;               /* create table后是否开启replic full模式 */
    int             gctime;                     /* 内存回收 */
    int             wal_decrypt;                /* wal日志是否需要解密 */
    char*           szloglevel;                 /* 日志级别                    */
    char*           data;                       /* 数据存储目录                 */
    char*           url;                        /* 连接数据库的字符串            */
    char*           szddl;                      /* ddl 解析                    */
    char*           waldir;                     /* 事务日志目录                  */
    char*           dbtype;                     /* 数据库类型                   */
    char*           dbversion;                  /* 数据库版本                   */
    char*           xsynchschema;               /* xsynch 工具所在的模式         */
    char*           host;                       /* 目标端监听的IP地址           */
    char*           compress_algorithm;         /* 压缩程序 */
    char*           compress_level;             /* 压缩级别 */
    char*           logdir;                     /* 日志目录,默认为运行目录的 log */
    char*           jobname;                    /* 名称                       */
} capture_cfg;

typedef struct INTEGRATE_CFG
{
    int             trailmaxsize;               /* trail 文件的最大值           */
    int             txbundlesize;               /* 合并事务时，每个事务中包含的最大条数 */
    int             mergetxn;                 /* 是否合并事务 */
    int             xport;                      /* xmanager监听的端口          */
    int             tcp_keepalive;
    int             tcp_user_timeout;
    int             tcp_keepalives_idle;
    int             tcp_keepalives_interval;
    int             tcp_keepalives_count;
    int             maxbuffersize;              /* 事务缓存占用的空间            */
    char*           szloglevel;                 /* 日志级别                    */
    char*           data;                       /* 数据存储目录                 */
    char*           traildir;                   /* trail日志目录                */
    char*           url;                        /* 连接数据库的字符串            */
    int             compatibility;              /* 兼容版本                    */
    int             truncatetable;              /* 清空表 */
    int             gctime;                     /* 内存回收 */
    char*           xsynchschema;               /* xsynch 工具所在的模式         */
    char*           compress_algorithm;         /* 压缩程序 */
    char*           compress_level;             /* 压缩级别 */
    char*           logdir;                     /* 日志目录,默认为运行目录的 log */
    char*           jobname;                    /* 名称                        */
    char*           method;                     /* 模式 burst 或者为 空      */
} integrate_cfg;

typedef struct RECEIVEWAL_CFG
{
    int             tli;
    char*           loglevel;
    char*           data;
    char*           startpos;
    char*           jobname;                    /* 名称                        */
    char*           logdir;                     /* 日志目录,默认为运行目录的 log */
    char*           primaryconninfo;
    char*           primaryslotname;
    char*           restorecommand;
} receivewal_cfg;


typedef struct XMANAGER_CFG
{
    int             port;                       /* 目标端监听的端口             */
    int             tcp_keepalive;
    int             tcp_user_timeout;
    int             tcp_keepalives_idle;
    int             tcp_keepalives_interval;
    int             tcp_keepalives_count;
    char*           szloglevel;                 /* 日志级别                         */
    char*           data;                       /* 数据存储目录                     */
    char*           host;                       /* 目标端监听的IP地址               */
    char*           logdir;                     /* 日志目录,默认为运行目录的 log    */
    char*           jobname;                    /* 名称                             */
    char*           unixdomaindir;              /* unix 域描述符存储位置            */
} xmanager_cfg;


typedef bool (*GucBoolCheckHook) (bool *newval, void **extra, GucSource source);
typedef bool (*GucIntCheckHook) (int *newval, void **extra, GucSource source);
typedef bool (*GucStringCheckHook) (char **newval, void **extra, GucSource source);


typedef void (*GucBoolAssignHook) (bool newval, void *extra);
typedef void (*GucIntAssignHook) (int newval, void *extra);
typedef void (*GucStringAssignHook) (const char *newval, void *extra);


void guc_loadcfg(const char* in_cfg, bool reload);

char* guc_getdata(void);

char* guc_gettrail(void);

char* guc_getConfigOption(const char* name);

/* 获取 配置项值 */
int guc_getConfigOptionInt(const char* name);

void guc_debug(void);

void guc_destroy(void);


extern int             g_walcachemaxsize;
extern int             g_transmaxnum;
extern int             g_blocksize;
extern int             g_walsegsize;
extern int             g_idbtype;
extern int             g_idbversion;
extern int             g_parserddl;
extern int             g_refreshstragety;
extern int             g_max_work_per_refresh;
extern int             g_max_page_per_refreshsharding;
#endif
