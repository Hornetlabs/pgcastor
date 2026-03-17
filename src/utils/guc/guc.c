#include "app_incl.h"
#include "utils/guc/guc_tables.h"
#include "port/file/fd.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "utils/list/list_func.h"

/* 监听端口 */
int             g_walcachemaxsize   = 64;
int             g_transmaxnum       = 128;
int             g_blocksize         = 8192;                       /* 事务日志块大小 */
int             g_walsegsize        = 16;                         /* 事务日志文件大小 */
int             g_idbtype           = XK_DATABASE_TYPE_POSTGRESQL;
int             g_idbversion        = 0;
int             g_parserddl         = 1;
int             g_loglevel          = RLOG_INFO;
int             g_refreshstragety   = 1;
int             g_max_work_per_refresh = 10;
int             g_max_page_per_refreshsharding = 1000;

/* 监听ip地址 */
/*---------------------capture 配置项 begin------------------------*/
static capture_cfg              g_capturecfg = { 0 };

static struct config_int ConfigureNamesIntCapture[] =
{
    {
        {CFG_KEY_TRAIL_MAX_SIZE, PGC_INTERNAL,
            gettext_noop("trail file max size, unit MB"),
            NULL,
        },
        &(g_capturecfg.trailmaxsize),
        128, 0, 4096,
        NULL, NULL
    },
    {
        {CFG_KEY_CAPTURE_BUFFER, PGC_INTERNAL,
            gettext_noop("trail file max size, unit MB"),
            NULL,
        },
        &(g_capturecfg.maxbuffersize),
        128, 1, 32768,
        NULL, NULL
    },
    {
        {CFG_KEY_COMPATIBILITY, PGC_INTERNAL,
            gettext_noop("compatibility version"),
            NULL,
        },
        &(g_capturecfg.compatibility),
        10, 10, 99,
        NULL, NULL
    },
    {
        {CFG_KEY_PORT, PGC_INTERNAL,
            gettext_noop("target host listen port"),
            NULL
        },
        &(g_capturecfg.port),
        7529, 22, 999999,
        NULL, NULL
    },
    {
        {CFG_KEY_XMANAGER_PORT, PGC_INTERNAL,
            gettext_noop("xmanager listen port"),
            NULL
        },
        &(g_capturecfg.xport),
        6543, 22, 999999,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVE, PGC_INTERNAL,
            gettext_noop("tcp keepalive, enable 1, disable 0"),
            NULL
        },
        &(g_capturecfg.tcp_keepalive),
        1, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_USER_TIMEOUT, PGC_INTERNAL,
            gettext_noop("tcp user timeout"),
            NULL
        },
        &(g_capturecfg.tcp_user_timeout),
        60000, 0, 300000,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_IDLE, PGC_INTERNAL,
            gettext_noop("tcp keepalives idle"),
            NULL
        },
        &(g_capturecfg.tcp_keepalives_idle),
        30, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_INTERVAL, PGC_INTERNAL,
            gettext_noop("tcp keepalives interval"),
            NULL
        },
        &(g_capturecfg.tcp_keepalives_interval),
        5, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_COUNT, PGC_INTERNAL,
            gettext_noop("tcp keepalives count"),
            NULL
        },
        &(g_capturecfg.tcp_keepalives_count),
        10, 0, 100,
        NULL, NULL
    },
    {
        {CFG_KEY_GCTIME, PGC_INTERNAL,
            gettext_noop("memory reclamation time"),
            NULL
        },
        &(g_capturecfg.gctime),
        0, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_MAX_WORK_PER_REFRESH, PGC_INTERNAL,
            gettext_noop("max refresh work num"),
            NULL
        },
        &(g_max_work_per_refresh),
        1, 1, 100,
        NULL, NULL
    },
    {
        {CFG_KEY_MAX_PAGE_PER_REFRESHSHARDING, PGC_INTERNAL,
            gettext_noop("shard threshold"),
            NULL
        },
        &(g_max_page_per_refreshsharding),
        1000, 100, 10000000,
        NULL, NULL
    },
    {
        {CFG_KEY_REFRESHSTRAGETY, PGC_INTERNAL,
            gettext_noop("Stock data sync strategy"),
            NULL
        },
        &(g_refreshstragety),
        1, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_ENABLE_REPLICA_IDENTITY, PGC_INTERNAL,
            gettext_noop("enable replica identity"),
            NULL
        },
        &(g_capturecfg.ddl_identity),
        1, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_CAPTURE_WAL_DECRYPT, PGC_INTERNAL,
            gettext_noop("wal need decrypt"),
            NULL
        },
        &(g_capturecfg.wal_decrypt),
        0, 0, 1,
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL
    }
};

static struct config_string ConfigureNamesStringCapture[] =
{
    {
        {CFG_KEY_LOG_LEVEL, PGC_INTERNAL,
            gettext_noop("log level, DEBUG INFO WARNING ERROR"),
            NULL
        },
        &(g_capturecfg.szloglevel),
        "INFO",
        NULL, NULL
    },
    {
        {CFG_KEY_DATA, PGC_INTERNAL,
            gettext_noop("work dir"),
            NULL
        },
        &(g_capturecfg.data),
        "/opt/xsynch/data",
        NULL, NULL
    },
    {
        {CFG_KEY_URL, PGC_INTERNAL,
            gettext_noop("database connect string"),
            NULL
        },
        &(g_capturecfg.url),
        "INFO",
        NULL, NULL
    },
    {
        {CFG_KEY_DDL, PGC_INTERNAL,
            gettext_noop("log level, on parser /off unparser"),
            NULL
        },
        &(g_capturecfg.szddl),
        "on",
        NULL, NULL
    },
    {
        {CFG_KEY_WAL_DIR, PGC_INTERNAL,
            gettext_noop("wal log directory"),
            NULL
        },
        &(g_capturecfg.waldir),
        "/opt/pgsql/dat/pg_wal",
        NULL, NULL
    },
    {
        {CFG_KEY_DBTYPE, PGC_INTERNAL,
            gettext_noop("Type of parsing database(postgres)"),
            NULL
        },
        &(g_capturecfg.dbtype),
        "postgres",
        NULL, NULL
    },
    {
        {CFG_KEY_DBVERION, PGC_INTERNAL,
            gettext_noop("Type of parsing database version(127)"),
            NULL
        },
        &(g_capturecfg.dbversion),
        "127",
        NULL, NULL
    },
    {
        {CFG_KEY_CATALOGSCHEMA, PGC_INTERNAL,
            gettext_noop("The schema where the sync_status table is located"),
            NULL
        },
        &(g_capturecfg.xsynchschema),
        "xsynch",
        NULL, NULL
    },
    {
        {CFG_KEY_HOST, PGC_INTERNAL,
            gettext_noop("target host"),
            NULL
        },
        &(g_capturecfg.host),
        "127.0.0.1",
        NULL, NULL
    },
    {
        {CFG_KEY_COMPRESS_ALGORITHM, PGC_INTERNAL,
            gettext_noop("compress algorithm"),
            NULL
        },
        &(g_capturecfg.compress_algorithm),
        NULL,
        NULL, NULL
    },
    {
        {CFG_KEY_COMPRESS_LEVEL, PGC_INTERNAL,
            gettext_noop("compress level"),
            NULL
        },
        &(g_capturecfg.compress_level),
        "9",
        NULL, NULL
    },
    {
        {CFG_KEY_LOG_DIR, PGC_INTERNAL,
            gettext_noop("log dir"),
            NULL
        },
        &(g_capturecfg.logdir),
        "log",
        NULL, NULL
    },
    {
        {CFG_KEY_JOBNAME, PGC_INTERNAL,
            gettext_noop("jobname"),
            NULL
        },
        &(g_capturecfg.jobname),
        "ripple",
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, NULL, NULL, NULL, NULL
    }
};

/*---------------------capture 配置项   end------------------------*/

/*---------------------integrate 配置项 begin---------------------------*/
static integrate_cfg                 g_integratecfg = { 0 };

static struct config_int ConfigureNamesIntIntegrate[] =
{
    {
        {CFG_KEY_TRAIL_MAX_SIZE, PGC_INTERNAL,
            gettext_noop("trail file max size, unit MB"),
            NULL,
        },
        &(g_integratecfg.trailmaxsize),
        128, 1, 4096,
        NULL, NULL
    },
    {
        {CFG_KEY_COMPATIBILITY, PGC_INTERNAL,
            gettext_noop("compatibility version"),
            NULL,
        },
        &(g_integratecfg.compatibility),
        10, 10, 99,
        NULL, NULL
    },
    {
        {CFG_KEY_MAX_WORK_PER_REFRESH, PGC_INTERNAL,
            gettext_noop("max refresh work num"),
            NULL
        },
        &(g_max_work_per_refresh),
        1, 1, 100,
        NULL, NULL
    },
    {
        {CFG_KEY_XMANAGER_PORT, PGC_INTERNAL,
            gettext_noop("xmanager listen port"),
            NULL
        },
        &(g_integratecfg.xport),
        6543, 22, 999999,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVE, PGC_INTERNAL,
            gettext_noop("tcp keepalive, enable 1, disable 0"),
            NULL
        },
        &(g_integratecfg.tcp_keepalive),
        1, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_USER_TIMEOUT, PGC_INTERNAL,
            gettext_noop("tcp user timeout"),
            NULL
        },
        &(g_integratecfg.tcp_user_timeout),
        60000, 0, 300000,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_IDLE, PGC_INTERNAL,
            gettext_noop("tcp keepalives idle"),
            NULL
        },
        &(g_integratecfg.tcp_keepalives_idle),
        30, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_INTERVAL, PGC_INTERNAL,
            gettext_noop("tcp keepalives interval"),
            NULL
        },
        &(g_integratecfg.tcp_keepalives_interval),
        5, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_COUNT, PGC_INTERNAL,
            gettext_noop("tcp keepalives count"),
            NULL
        },
        &(g_integratecfg.tcp_keepalives_count),
        10, 0, 100,
        NULL, NULL
    },
    {
        {CFG_KEY_TRUNCATETABLE, PGC_INTERNAL,
            gettext_noop("truncate table"),
            NULL
        },
        &(g_integratecfg.truncatetable),
        0, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_GCTIME, PGC_INTERNAL,
            gettext_noop("memory reclamation time"),
            NULL
        },
        &(g_integratecfg.gctime),
        0, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TXBUNDLESIZE, PGC_INTERNAL,
            gettext_noop("txbundle size"),
            NULL
        },
        &(g_integratecfg.txbundlesize),
        0, 0, 10000000,
        NULL, NULL
    },
    {
        {CFG_KEY_MERGETXN, PGC_INTERNAL,
            gettext_noop("merge txn"),
            NULL
        },
        &(g_integratecfg.mergetxn),
        0, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_INTEGRATE_BUFFER, PGC_INTERNAL,
            gettext_noop("txn max size, unit MB"),
            NULL,
        },
        &(g_integratecfg.maxbuffersize),
        128, 1, 32768,
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL
    }
};


static struct config_string ConfigureNamesStringIntegrate[] =
{
    {
        {CFG_KEY_LOG_LEVEL, PGC_INTERNAL,
            gettext_noop("log level, DEBUG INFO WARNING ERROR"),
            NULL
        },
        &(g_integratecfg.szloglevel),
        "INFO",
        NULL, NULL
    },
    {
        {CFG_KEY_DATA, PGC_INTERNAL,
            gettext_noop("work dir"),
            NULL
        },
        &(g_integratecfg.data),
        "/opt/xsynch/data",
        NULL, NULL
    },
    {
        {CFG_KEY_URL, PGC_INTERNAL,
            gettext_noop("database connect string"),
            NULL
        },
        &(g_integratecfg.url),
        "",
        NULL, NULL
    },
    {
        {CFG_KEY_TRAIL_DIR, PGC_INTERNAL,
            gettext_noop("trail directory"),
            NULL
        },
        &(g_integratecfg.traildir),
        "/opt/ripple/capture/trail",
        NULL, NULL
    },
    {
        {CFG_KEY_CATALOGSCHEMA, PGC_INTERNAL,
            gettext_noop("The schema where the sync_status table is located"),
            NULL
        },
        &(g_integratecfg.xsynchschema),
        "xsynch",
        NULL, NULL
    },
    {
        {CFG_KEY_COMPRESS_ALGORITHM, PGC_INTERNAL,
            gettext_noop("compress algorithm"),
            NULL
        },
        &(g_integratecfg.compress_algorithm),
        NULL,
        NULL, NULL
    },
    {
        {CFG_KEY_COMPRESS_LEVEL, PGC_INTERNAL,
            gettext_noop("compress level"),
            NULL
        },
        &(g_integratecfg.compress_level),
        "9",
        NULL, NULL
    },
    {
        {CFG_KEY_LOG_DIR, PGC_INTERNAL,
            gettext_noop("log dir"),
            NULL
        },
        &(g_integratecfg.logdir),
        "log",
        NULL, NULL
    },
    {
        {CFG_KEY_JOBNAME, PGC_INTERNAL,
            gettext_noop("jobname"),
            NULL
        },
        &(g_integratecfg.jobname),
        "ripple",
        NULL, NULL
    },
    {
        {CFG_KEY_INTEGRATE_METHOD, PGC_INTERNAL,
            gettext_noop("method"),
            NULL
        },
        &(g_integratecfg.method),
        NULL,
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, NULL, NULL, NULL, NULL
    }
};


/*---------------------integrate 配置项   end---------------------------*/

/*---------------------receivelog 配置项 begin--------------------------*/


static receivewal_cfg                m_receivewalcfg = { 0 };

static struct config_int ConfigureNamesIntReceivewal[] =
{
    {
        {CFG_KEY_TIMELINE, PGC_INTERNAL,
            gettext_noop("start specified timeline"),
            NULL
        },
        &(m_receivewalcfg.tli),
        0, 0, 400000000,
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL
    }
};


static struct config_string ConfigureNamesStringReceivewal[] =
{
    {
        {CFG_KEY_LOG_LEVEL, PGC_INTERNAL,
            gettext_noop("log level, DEBUG INFO WARNING ERROR"),
            NULL
        },
        &(m_receivewalcfg.loglevel),
        "INFO",
        NULL, NULL
    },
    {
        {CFG_KEY_DATA, PGC_INTERNAL,
            gettext_noop("work dir"),
            NULL
        },
        &(m_receivewalcfg.data),
        "/opt/receivewal/data",
        NULL, NULL
    },
    {
        {CFG_KEY_JOBNAME, PGC_INTERNAL,
            gettext_noop("jobname"),
            NULL
        },
        &(m_receivewalcfg.jobname),
        "ripple",
        NULL, NULL
    },
    {
        {CFG_KEY_LOG_DIR, PGC_INTERNAL,
            gettext_noop("log dir"),
            NULL
        },
        &(m_receivewalcfg.logdir),
        "log",
        NULL, NULL
    },
    {
        {CFG_KEY_PRIMARY_CONN_INFO, PGC_INTERNAL,
            gettext_noop("database server connect string"),
            NULL
        },
        &(m_receivewalcfg.primaryconninfo),
        "",
        NULL, NULL
    },
    {
        {CFG_KEY_SLOT_NAME, PGC_INTERNAL,
            gettext_noop("physical replication use specified slot"),
            NULL
        },
        &(m_receivewalcfg.primaryslotname),
        "",
        NULL, NULL
    },
    {
        {CFG_KEY_STARTPOS, PGC_INTERNAL,
            gettext_noop("start replication replication lsn, XX/XXX"),
            NULL
        },
        &(m_receivewalcfg.startpos),
        "",
        NULL, NULL
    },
    {
        {CFG_KEY_RESTORE_COMMAND, PGC_INTERNAL,
            gettext_noop("After archiving at the source, use a command to retrieve it from the archive"),
            NULL
        },
        &(m_receivewalcfg.restorecommand),
        "",
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, NULL, NULL, NULL, NULL
    }
};

/*---------------------receivelog 配置项   end--------------------------*/

/*---------------------xmanager 配置项 begin----------------------------*/

static xmanager_cfg                m_xmanagercfg = { 0 };

static struct config_int ConfigureNamesIntXmanager[] =
{
    {
        {CFG_KEY_PORT, PGC_INTERNAL,
            gettext_noop("target host listen port"),
            NULL
        },
        &(m_xmanagercfg.port),
        6543, 22, 999999,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVE, PGC_INTERNAL,
            gettext_noop("tcp keepalive, enable 1, disable 0"),
            NULL
        },
        &(m_xmanagercfg.tcp_keepalive),
        1, 0, 1,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_USER_TIMEOUT, PGC_INTERNAL,
            gettext_noop("tcp user timeout"),
            NULL
        },
        &(m_xmanagercfg.tcp_user_timeout),
        60000, 0, 300000,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_IDLE, PGC_INTERNAL,
            gettext_noop("tcp keepalives idle"),
            NULL
        },
        &(m_xmanagercfg.tcp_keepalives_idle),
        30, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_INTERVAL, PGC_INTERNAL,
            gettext_noop("tcp keepalives interval"),
            NULL
        },
        &(m_xmanagercfg.tcp_keepalives_interval),
        5, 0, 300,
        NULL, NULL
    },
    {
        {CFG_KEY_TCP_KEEPALIVES_COUNT, PGC_INTERNAL,
            gettext_noop("tcp keepalives count"),
            NULL
        },
        &(m_xmanagercfg.tcp_keepalives_count),
        10, 0, 100,
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL
    }
};

static struct config_string ConfigureNamesStringXmanager[] =
{
    {
        {CFG_KEY_LOG_LEVEL, PGC_INTERNAL,
            gettext_noop("log level, DEBUG INFO WARNING ERROR"),
            NULL
        },
        &(m_xmanagercfg.szloglevel),
        "INFO",
        NULL, NULL
    },
    {
        {CFG_KEY_LOG_DIR, PGC_INTERNAL,
            gettext_noop("log dir"),
            NULL
        },
        &(m_xmanagercfg.logdir),
        "log",
        NULL, NULL
    },
    {
        {CFG_KEY_DATA, PGC_INTERNAL,
            gettext_noop("work dir"),
            NULL
        },
        &(m_xmanagercfg.data),
        "/opt/xsynch/xmanagerdata",
        NULL, NULL
    },
    {
        {CFG_KEY_HOST, PGC_INTERNAL,
            gettext_noop("listen host"),
            NULL
        },
        &(m_xmanagercfg.host),
        "127.0.0.1",
        NULL, NULL
    },
    {
        {CFG_KEY_JOBNAME, PGC_INTERNAL,
            gettext_noop("jobname"),
            NULL
        },
        &(m_xmanagercfg.jobname),
        "xmanager",
        NULL, NULL
    },
    {
        {CFG_KEY_UNIXDOMAINDIR, PGC_INTERNAL,
            gettext_noop("unix domain dir"),
            NULL
        },
        &(m_xmanagercfg.unixdomaindir),
        "/tmp",
        NULL, NULL
    },
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, NULL, NULL, NULL, NULL
    }
};

/*---------------------xmanager 配置项   end----------------------------*/

/*---------------------guc begin----------------------------------*/
typedef void (*gucdebug)();

typedef struct GUC_CFG
{
    int                     type;
    struct config_int*      cint;
    struct config_string*   cstr;
    gucdebug                debugfunc;
} guc_cfg;

/*---------------------guc   end----------------------------------*/


/*
 * Actual lookup of variables is done through this single, sorted array.
 */
static struct config_generic **guc_variables = NULL;
static int	num_guc_variables = 0;

/* Vector capacity */
static int	size_guc_variables;

List *g_table = NULL;
List *g_tableexclude = NULL;
List *g_addtablepattern = NULL;


static void guc_capturedebug(void)
{
    ListCell* lc = NULL;
    List* ls = NULL;
    printf("---------capture config begin--------------\n");
    printf("ddl:            %s\n", g_capturecfg.ddl == 0 ? "off" : "on");
    printf("szloglevel:     %s\n", g_capturecfg.szloglevel == NULL ? "NULL" : g_capturecfg.szloglevel);
    printf("data:           %s\n", g_capturecfg.data == NULL ? "NULL" : g_capturecfg.data);
    printf("url:            %s\n", g_capturecfg.url == NULL ? "NULL" : g_capturecfg.url);
    printf("waldir:         %s\n", g_capturecfg.waldir == NULL ? "NULL" : g_capturecfg.waldir);
    printf("dbtype:         %s\n", g_capturecfg.dbtype == NULL ? "NULL" : g_capturecfg.dbtype);
    printf("dbversion:      %s\n", g_capturecfg.dbversion == NULL ? "NULL" : g_capturecfg.dbversion);
    printf("xsynchschema:   %s\n", g_capturecfg.xsynchschema == NULL ? "NULL" : g_capturecfg.xsynchschema);
    printf("logdir:   %s\n", g_capturecfg.logdir == NULL ? "NULL" : g_capturecfg.logdir);
    
    if(NULL != g_table)
    {
        ls = (List*)g_table;
        foreach(lc, ls)
        {
            printf("tbinclude:%s\n", (char*)(lfirst(lc)));
        }
    }

    if(NULL != g_tableexclude)
    {
        ls = (List*)g_tableexclude;
        foreach(lc, ls)
        {
            printf("tbexclude:          %s\n", (char*)(lfirst(lc)));
        }
    }

    if(NULL != g_addtablepattern)
    {
        ls = (List*)g_addtablepattern;
        foreach(lc, ls)
        {
            printf("addtablepattern:          %s\n", (char*)(lfirst(lc)));
        }
    }
    printf("---------capture config   end--------------\n");
}

static void guc_integratedebug(void)
{
    ListCell* lc = NULL;
    List* ls = NULL;
    printf("---------integrate config begin--------------\n");
    printf("szloglevel:     %s\n", g_integratecfg.szloglevel == NULL ? "NULL" : g_integratecfg.szloglevel);
    printf("data:           %s\n", g_integratecfg.data == NULL ? "NULL" : g_integratecfg.data);
    printf("traildir:       %s\n", g_integratecfg.traildir == NULL ? "NULL" : g_integratecfg.traildir);
    printf("url:            %s\n", g_integratecfg.url == NULL ? "NULL" : g_integratecfg.url);
    
    if(NULL != g_table)
    {
        ls = (List*)g_table;
        foreach(lc, ls)
        {
            printf("tbinclude:%s\n", (char*)(lfirst(lc)));
        }
    }

    if(NULL != g_tableexclude)
    {
        ls = (List*)g_tableexclude;
        foreach(lc, ls)
        {
            printf("tbexclude:          %s\n", (char*)(lfirst(lc)));
        }
    }

    if(NULL != g_addtablepattern)
    {
        ls = (List*)g_addtablepattern;
        foreach(lc, ls)
        {
            printf("addtablepattern:          %s\n", (char*)(lfirst(lc)));
        }
    }
    printf("---------integrate config   end--------------\n");
}

static void guc_receivewaldebug(void)
{
    printf("---------receivewal config begin--------------\n");
    printf("data:     %s\n",
            m_receivewalcfg.data == NULL ? "NULL" : m_receivewalcfg.data);

    printf("timeline:     %d\n", m_receivewalcfg.tli);

    printf("startpos:     %s\n",
            m_receivewalcfg.startpos == NULL ? "NULL" : m_receivewalcfg.startpos);

    printf("primaryconninfo:            %s\n",
            m_receivewalcfg.primaryconninfo == NULL ? "NULL" : m_receivewalcfg.primaryconninfo);

    printf("primaryslotname:            %s\n",
            m_receivewalcfg.primaryslotname == NULL ? "NULL" : m_receivewalcfg.primaryslotname);

    printf("restorecommand:            %s\n",
            m_receivewalcfg.restorecommand == NULL ? "NULL" : m_receivewalcfg.restorecommand);
    
    printf("---------receivewal config   end--------------\n");
}

static void guc_xmanagerdebug(void)
{
    printf("---------xmanager config begin--------------\n");
    printf("jobname:    %s\n",
            m_xmanagercfg.jobname == NULL ? "NULL" : m_xmanagercfg.jobname);

    printf("data:       %s\n",
            m_xmanagercfg.data == NULL ? "NULL" : m_xmanagercfg.data);

    printf("logdir:     %s\n",
            m_xmanagercfg.logdir == NULL ? "NULL" : m_xmanagercfg.logdir);

    printf("loglevel:   %s\n",
            m_xmanagercfg.szloglevel == NULL ? "NULL" : m_xmanagercfg.szloglevel);

    printf("host:               %s\n",
            m_xmanagercfg.host == NULL ? "NULL" : m_xmanagercfg.host);

    printf("unixdomaindir:      %s\n",
            m_xmanagercfg.unixdomaindir == NULL ? RMANAGER_UNIXDOMAINDIR : m_xmanagercfg.unixdomaindir);

    printf("port:       %d\n", m_xmanagercfg.port);

    printf("tcp_keepalive:              %d\n", m_xmanagercfg.tcp_keepalive);
    printf("tcp_user_timeout:           %d\n", m_xmanagercfg.tcp_user_timeout);
    printf("tcp_keepalives_idle:        %d\n", m_xmanagercfg.tcp_keepalives_idle);
    printf("tcp_keepalives_interval:    %d\n", m_xmanagercfg.tcp_keepalives_interval);
    printf("tcp_keepalives_count:       %d\n", m_xmanagercfg.tcp_keepalives_count);
    
    printf("---------xmanager config   end--------------\n");
}

static guc_cfg m_guccfg[] =
{
    {
        PROC_TYPE_NOP,
        NULL,
        NULL,
        NULL
    },
    {
        PROC_TYPE_CAPTURE,
        ConfigureNamesIntCapture,
        ConfigureNamesStringCapture,
        guc_capturedebug
    },
    {
        PROC_TYPE_INTEGRATE,
        ConfigureNamesIntIntegrate,
        ConfigureNamesStringIntegrate,
        guc_integratedebug
    },
    {
        PROC_TYPE_PGRECEIVEWAL,
        ConfigureNamesIntReceivewal,
        ConfigureNamesStringReceivewal,
        guc_receivewaldebug
    },
    {
        PROC_TYPE_XMANAGER,
        ConfigureNamesIntXmanager,
        ConfigureNamesStringXmanager,
        guc_xmanagerdebug
    }
};

/* 打印信息 */
void guc_debug(void)
{
    m_guccfg[g_proctype].debugfunc();
}

static void *
guc_malloc(int elevel, size_t size)
{
	void	   *data;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	data = rmalloc0(size);
	if (data == NULL)
    {
		elog(elevel, "out of memory");
    }
	rmemset0(data, 0, '\0', size);
	return data;
}

static char *
guc_strdup(int elevel, const char *src)
{
	char	   *data;

	data = rstrdup(src);
	if (data == NULL)
    {
        elog(elevel, "out of memory");
    }
	return data;
}


/*
 * the bare comparison function for GUC names
 */
static int
guc_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * array ordering has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}

/*
 * comparator for qsorting and bsearching guc_variables array
 */
static int
guc_var_compare(const void *a, const void *b)
{
	const struct config_generic *confa = *(struct config_generic *const *) a;
	const struct config_generic *confb = *(struct config_generic *const *) b;

	return guc_name_compare(confa->name, confb->name);
}

static void guc_loadbuildin(proc_type type)
{
    int			i;
    int			num_vars = 0;
    int			size_vars;
    struct config_generic **guc_vars;

    /* 加载默认值 */
    for (i = 0; m_guccfg[type].cint[i].gen.name; i++)
    {
        struct config_int *conf = &m_guccfg[type].cint[i];

        conf->gen.vartype = RIPPLEC_INT;
        num_vars++;
    }

    for (i = 0; m_guccfg[type].cstr[i].gen.name; i++)
    {
        struct config_string *conf = &m_guccfg[type].cstr[i];

        conf->gen.vartype = RIPPLEC_STRING;
        num_vars++;
    }

    /*
	 * Create table with 20% slack
	 */
	size_vars = num_vars + num_vars / 4;
    guc_vars = (struct config_generic **)
		guc_malloc(RLOG_ERROR, size_vars * sizeof(struct config_generic *));
    
    num_vars = 0;

    for (i = 0; m_guccfg[type].cint[i].gen.name; i++)
    {
        guc_vars[num_vars++] = &m_guccfg[type].cint[i].gen;
    }

    for (i = 0; m_guccfg[type].cstr[i].gen.name; i++)
    {
        guc_vars[num_vars++] = &m_guccfg[type].cstr[i].gen;
    }

    /* 加载配置文件 */
    if (guc_variables)
    {
        rfree(guc_variables);
    }

    guc_variables = guc_vars;
    num_guc_variables = num_vars;
    size_guc_variables = size_vars;
    qsort((void *) guc_variables, num_guc_variables, sizeof(struct config_generic *), guc_var_compare);
}


static struct config_generic *
find_option(const char *name)
{
	const char **key = &name;
	struct config_generic **res;

	Assert(name);

	/*
	 * By equating const char ** with struct config_generic *, we are assuming
	 * the name field is first in config_generic.
	 */
	res = (struct config_generic **) bsearch((void *) &key,
											 (void *) guc_variables,
											 num_guc_variables,
											 sizeof(struct config_generic *),
											 guc_var_compare);
	if (res)
    {
        return *res;
    }

    /* Unknown name */
    return NULL;
}

static void
guc_parseConfigFile(const char *config_file,
                    ConfigVariable **head_p,
                    ConfigVariable **tail_p)
{
    FILE   *fp = NULL;
    char	fline[1024];
    char*   key = NULL;
    char*   value = NULL;

    fp = osal_allocate_file(config_file, "r");
    if (!fp)
    {
        elog(RLOG_ERROR, "could not open configuration file:%s", config_file);
    }

    /* 清空过滤规则 */
    list_free_deep(g_table);
    list_free_deep(g_tableexclude);
    list_free_deep(g_addtablepattern);
    g_table = NULL;
    g_tableexclude = NULL;
    g_addtablepattern = NULL;

    /* 读取一行数据 */
    rmemset1(fline, 0, '\0', sizeof(fline));
    while (fgets(fline, sizeof(fline), fp) != NULL)
	{
		bool quota = false;
        char* uptr = fline;
        ConfigVariable *item = NULL;
        int pos = 0;
        int len = 0;

        /* 跳过 开头的 空字符等信息 */
		while('\0' != *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
				&& '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* 跳过空行和注释行 */
        if('\0' == *uptr || '#' == *uptr)
        {
            continue;
        }

        /* 获取key */
        while('\0' != *uptr)
        {
            if(' ' == *uptr
                || '\t' == *uptr
                || '\r' == *uptr
                || '\n' == *uptr
                || '=' == *uptr)
            {
                break;
            }
            len++;
            uptr++;
        }

        /* 获取名称 */
        len += 1;
        key = (char*)rmalloc0(len);
        if(NULL == key)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(key, 0, '\0', len);
        rmemcpy0(key, 0, fline + pos, len);
        key[len - 1] = '\0';
        pos += (len - 1);

        /* 获取 value */
        len = 0;

        /* 跳过 SPACE TABE 和 换行 */
        while('\0'!= *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
                && '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            pos++;
            uptr++;
        }

        if('\0' == *uptr)
        {
            /* 结束了 */
            rfree(key);
            continue;
        }
        //table 获取;
        if('=' != *uptr)
        {
            /* 结束了 */
            elog(RLOG_ERROR, "config:%s error", key);
        }

        /* 获取 value 值 */
        /* 跳过 '=' 字符 */
        pos++;
        uptr++;

        /* 跳过 空格 等字符 */
        while('\0' != *uptr)
        {
            if(' ' != *uptr
                && '\t' != *uptr
				&& '\r' != *uptr
                && '\n' != *uptr)
            {
                break;
            }
            uptr++;
            pos++;
        }

        /* 跳过空行和注释行 */
        if('\0' == *uptr || '#' == *uptr)
        {
            /* 结束了 */
            rfree(key);
            elog(RLOG_ERROR, "config:%s error", key);
        }

        /* 跳过 空格 等字符 */
        /* 查看字符类型 */
        if(*uptr == '"')
        {
            uptr++;
            pos++;
            quota = true;
            /* 获取去下一个 " 字符 */
            while('\0' != *uptr)
            {
                if('"' == *uptr)
                {
                    quota = false;
                    break;
                }
                len++;
                uptr++;
            }

            if(true == quota)
            {
                elog(RLOG_ERROR, "configuration item:%s is incorrect, missing double quotation marks", key);
            }
        }
        else
        {
            while('\0'!= *uptr)
            {
                if(' ' == *uptr
                    || '\t' == *uptr
                    || '\r' == *uptr
                    || '\n' == *uptr)
                {
                    break;
                }
                len++;
                uptr++;
            }
        }

        len += 1;
        value = (char*)rmalloc0(len);
        if(NULL == value)
        {
            rfree(key);
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(value, 0, 0, len);
        rmemcpy0(value, 0, fline + pos, len - 1);
        if(strcmp(key, TBINCLUDE) == 0 )
        {
            g_table = lappend(g_table, value);
            rfree(key);
            continue;
        }
        if(strcmp(key, TBEXCLUDE) == 0)
        {
            g_tableexclude = lappend(g_tableexclude, value);
            rfree(key);
            continue;
        }
        if(strcmp(key, ADDTABLEPATTERN) == 0)
        {
            g_addtablepattern = lappend(g_addtablepattern, value);
            rfree(key);
            continue;
        }

        item = (ConfigVariable*)rmalloc0(sizeof(ConfigVariable));
        if(NULL == item)
        {
            rfree(key);
            rfree(value);
            elog(RLOG_ERROR, "out of memory");
        }

        item->name = key;
        item->value = value;
        item->next = NULL;
        if (*head_p == NULL)
            *head_p = item;
        else
            (*tail_p)->next = item;
        *tail_p = item;

        rmemset1(fline, 0, '\0', sizeof(fline));
    }
}

static bool
parse_int(const char *value, int *result)
{
	/*
	 * We assume here that double is wide enough to represent any integer
	 * value with adequate precision.
	 */
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;

	/*
	 * Try to parse as an integer (allowing octal or hex input).  If the
	 * conversion stops at a decimal point or 'e', or overflows, re-parse as
	 * float.  This should work fine as long as we have no unit names starting
	 * with 'e'.  If we ever do, the test could be extended to check for a
	 * sign or digit after 'e', but for now that's unnecessary.
	 */
	errno = 0;
	val = strtol(value, &endptr, 0);
	if (*endptr == '.' || *endptr == 'e' || *endptr == 'E' ||
		errno == ERANGE)
	{
		errno = 0;
		val = strtod(value, &endptr);
	}

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range check below) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Round to int, then check for overflow */
	val = rint(val);

	if (val > INT_MAX || val < INT_MIN)
	{
		return false;
	}

	if (result)
		*result = (int) val;
	return true;
}


static bool
parse_and_validate_value(struct config_generic *record,
						 const char *name, const char *value,
						 union config_var_val *newval)
{
	switch (record->vartype)
	{
		case RIPPLEC_INT:
			{
				struct config_int *conf = (struct config_int *) record;

				if (!parse_int(value, &newval->intval))
				{
					elog(RLOG_ERROR,"invalid value for parameter %s: %s", name, value);
				}

				if (newval->intval < conf->min || newval->intval > conf->max)
				{
					elog(RLOG_ERROR, "%d is outside the valid range for parameter %s (%d .. %d)",
									newval->intval,
									name,
									conf->min, conf->max);
					return false;
				}
			}
			break;
		case RIPPLEC_STRING:
			{
				/*
				 * The value passed by the caller could be transient, so we
				 * always rstrdup it.
				 */
				newval->stringval = guc_strdup(RLOG_ERROR, value);
				if (newval->stringval == NULL)
                {
					return false;
                }
			}
			break;
	}

	return true;
}

static void
set_string_field(struct config_string *conf, char **field, char *newval)
{
	char	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/* rfree old value if it's not NULL and isn't referenced anymore */
	if (oldval)
		rfree(oldval);
}


static int
set_config_option(const char *name, const char *value, bool reload)
{
	struct config_generic *record;
	union config_var_val newval_union;

	record = find_option(name);
	if (record == NULL)
	{
		elog(RLOG_ERROR, "unrecognized configuration parameter %s", name);
	}

	/*
	 * Evaluate value and set variable.
	 */
	switch (record->vartype)
	{
		case RIPPLEC_INT:
			{
				struct config_int *conf = (struct config_int *) record;

#define newval (newval_union.intval)

				if (conf->gen.reload == PGC_INTERNAL && true == reload)
				{
					break;
				}
				
				if (value)
				{
					if (!parse_and_validate_value(record, name, value,
												  &newval_union))
						return 0;
				}
				else
				{
					newval = conf->reset_val;
				}

				*conf->variable = newval;
                break;
#undef newval
			}
		case RIPPLEC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;

#define newval (newval_union.stringval)

				if (conf->gen.reload == PGC_INTERNAL && true == reload)
				{
					break;
				}
				if (value)
				{
					if (!parse_and_validate_value(record, name, value, &newval_union))
						return 0;
				}
				else
				{
					/*
					 * rstrdup not needed, since reset_val is already under
					 * guc.c's control
					 */
					newval = conf->reset_val;
				}

				set_string_field(conf, conf->variable, newval);
				break;
#undef newval
			}
	}

	return 1;
}

/* 获取 配置项值 */
char* guc_getConfigOption(const char* name)
{
    static char buffer[256];
    struct config_generic *record;

    record = find_option(name);
    if (record == NULL)
    {
        elog(RLOG_ERROR, "unrecognized configuration parameter:%s", name);
    }

    switch (record->vartype)
    {
        case RIPPLEC_INT:
            snprintf(buffer, sizeof(buffer), "%d",
                        *((struct config_int *) record)->variable);
            return buffer;
        case RIPPLEC_STRING:
            return *((struct config_string *) record)->variable;
    }

    return NULL;
}

/* 获取 配置项值 */
int guc_getConfigOptionInt(const char* name)
{
    struct config_generic *record;

    record = find_option(name);
    if (record == NULL)
    {
        elog(RLOG_ERROR, "unrecognized configuration parameter:%s", name);
    }

    switch (record->vartype)
    {
        case RIPPLEC_INT:
            return *((struct config_int *) record)->variable;
        default:
            elog(RLOG_ERROR, "config %s not int type");
            return 0;
    }

    return 0;
}


static void
guc_loadcfgInternal(const char* in_cfg, bool reload)
{
    int i = 0;
    ConfigVariable *item = NULL;
    ConfigVariable *head = NULL;
    ConfigVariable *tail = NULL;

    head = tail = NULL;
    guc_parseConfigFile(in_cfg, &head, &tail);

    for (i = 0; i < num_guc_variables; i++)
    {
        struct config_generic *gconf = guc_variables[i];

        gconf->status &= ~GUC_IS_IN_FILE;
    }

    for (item = head; item; item = item->next)
    {
        struct config_generic *record;

        /* 根据名称获取 record */
        record = find_option(item->name);

        if(NULL == record)
        {
            elog(RLOG_ERROR, "unkown config item:%s", item->name);
        }

        /* Now mark it as present in file */
        record->status |= GUC_IS_IN_FILE;
    }

    for (item = head; item; item = head)
    {
        int			scres;
        head = item->next;
        scres = set_config_option(item->name, item->value, reload);
        if (scres == 0)
        {
            elog(RLOG_ERROR,"setting could not be applied");
        }

        if(NULL != item->name)
        {
            rfree(item->name);
        }

        if(NULL != item->value)
        {
            rfree(item->value);
        }
        rfree(item);
        item = NULL;
    }

    return;
}

void guc_loadcfg(const char* in_cfg, bool reload)
{
    struct stat stat_buf;

    /* 查看文件是否存在 */
    if(NULL == in_cfg)
    {
        elog(RLOG_ERROR, "argument error");
    }

    if(0 != stat(in_cfg, &stat_buf))
    {
        elog(RLOG_ERROR, "could not access file:%s, %s", in_cfg, strerror(errno));
    }

    /* 加载默认值 */
    guc_loadbuildin(g_proctype);

    /* 加载配置文件 */
    guc_loadcfgInternal(in_cfg, reload);
}

char* guc_getdata(void)
{
    switch (g_proctype)
    {
        case PROC_TYPE_CAPTURE:
            return g_capturecfg.data;
            break;
        case PROC_TYPE_INTEGRATE:
            return g_integratecfg.data;
        case PROC_TYPE_PGRECEIVEWAL:
            return m_receivewalcfg.data;
        case PROC_TYPE_XMANAGER:
            return m_xmanagercfg.data;
        default:
            return NULL;
    }
}

char* guc_gettrail(void)
{
    switch (g_proctype)
    {
        case PROC_TYPE_INTEGRATE:
            return g_integratecfg.traildir;
        case PROC_TYPE_PGRECEIVEWAL:
        case PROC_TYPE_CAPTURE:
        case PROC_TYPE_XMANAGER:
            return NULL;
        default:
            return NULL;
    }
}

void guc_destroy(void)
{
    list_free_deep(g_table);
    list_free_deep(g_tableexclude);
    list_free_deep(g_addtablepattern);
}
