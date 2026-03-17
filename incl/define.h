#ifndef _DEFINE_H
#define _DEFINE_H

typedef enum DBTYPE
{
    DBTYPE_NOP       = 0x00,
    DBTYPE_POSTGRES  = 0x01
} dbtype;


typedef enum CATALOG_PGVERSION
{
    PGDBVERSION_NOP      = 0x00,
    PGDBVERSION_12       = 0x01
} catalog_pgversion;

typedef enum PROC_TYPE
{
    PROC_TYPE_NOP                = 0x00,
    PROC_TYPE_CAPTURE            ,
    PROC_TYPE_INTEGRATE          ,
    PROC_TYPE_PGRECEIVEWAL       ,
    PROC_TYPE_XMANAGER           
} proc_type;

typedef enum XSYNCHSTAT
{
    XSYNCHSTAT_INIT           = 0x00,
    XSYNCHSTAT_REWIND         = 0x01,
    XSYNCHSTAT_REWINDING      = 0x02,
    XSYNCHSTAT_RUNNING        = 0x03,
    XSYNCHSTAT_SHUTDOWN       = 0x04,
    XSYNCHSTAT_RECOVERY       = 0x05,
} xsynchstat;

typedef enum CAPTURERELOAD_STATUS
{
    CAPTURERELOAD_STATUS_UNSET                   = 0x00,
    CAPTURERELOAD_STATUS_RELOADING_PARSERWAL     = 0x01,
    CAPTURERELOAD_STATUS_RELOADING_WRITE         = 0x02
}capturereload_status;

#define DBTYPE_POSTGRES              "postgres"
#define DBVERSION_POSTGRES_12        "12"

#define LOCK_FILE                    "proc.lock"
#define CONTROL_FILE                 "ripple.ctrl"
#define DECODE_BASE_FILE             "base.dat"
#define CACHEDIR                     "cache"
#define STAT                         "stat"
#define STAT_DECODE                  "decode.stat"
#define CONSTRAINT_FILE              "catalog/constraint.bat"
#define SYSDICTS_FILE                "catalog/sysdicts.bat"
#define SYSDICTS_TMP_FILE            "catalog/sysdicts.bat.tmp"
#define SYNC_STATUSTABLE_NAME        "sync_status"
#define CATALOG_DIR                  "catalog"
#define FILTER_DIR                   "filter"
#define STORAGE_TRAIL_DIR            "trail"
#define STORAGE_BIG_TRANSACTION_DIR  "bigtxn"
#define FILTER_OIDS_FILE             "oids.bat"
#define CAPTURE_STATUS_FILE          "capture.stat"
#define INTEGRATE_STATUS_FILE        "integrate.stat"
#define CAPTURE_STATUS_FILE_TEMP     "capture.stat.tmp"
#define INTEGRATE_STATUS_FILE_TEMP   "integrate.stat.tmp"
#define FILTER_DATASET_TMP           "filterdataset.dat.tmp"
#define FILTER_DATASET               "filterdataset.dat"
#define ONLINEREFRESH_DAT            "onlinerefresh.dat"
#define ONLINEREFRESH_STATUS         "onlinerefresh.status"
#define REFRESH_STATS                "stats.dat"
#define BIGTRANSACTION_FILE          "bigtransaction.status"
#define BIGTRANSACTION_FILE_TEMP     "bigtransaction.status.tmp"
#define ONLINEREFRESHABANDON_DAT     "onlinerefreshabandon.dat"
#define REFRESH_STATUS               "refresh.status"

#define TIMEZONE                     "PRC"
#define NUMERIC                      "zh_CN.UTF-8"
#define MONETARY                     "zh_CN.UTF-8"
#define ORGENCODING                  "UTF8"
#define DSTENCODING                  "UTF8"

#define TBINCLUDE                    "table"
#define TBEXCLUDE                    "tableexclude"
#define ADDTABLEPATTERN              "addtablepattern"


#define REFRESH_INCREMENT            "increment"
#define REFRESH_REFRESH              "refresh"
#define REFRESH_PARTIAL              "partial"
#define REFRESH_COMPLETE             "complete"
#define REFRESH_ONLINEREFRESH        "onlinerefresh"

#define MAX_MAPPINGS                 62
#define CONTROL_FILE_SIZE            1024
#define DECODE_STAT                  CONTROL_FILE_SIZE
#define FILE_BLK_SIZE                8192
#define FILE_SIZE                    (1024*1024)
#define PAGE_HEADER_SIZE             4
#define FirstNormalObjectId          16384
#define SYNCNAMESPACE_MAXOID         15233

#define BIG_MEMORY                   (FILE_SIZE)

#define FILE_BLK_MOD(offset)         (FILE_BLK_SIZE - (offset % FILE_BLK_SIZE))

#define MAXPATH                      512
#define ABSPATH                      1024
#define LINESIZE                     1024
#define COMMANDSIZE                  2048


#define FILEID2DIR(fileid)           (fileid%256)

#define FILE_OFFSET_INVALID          0x80000000


#define PG_DFAULT_TABLESPACE         1663
#define MAX_EXEC_SQL_LEN             1024

#define MAGIC                        0x1571
#define SYSDICT_MAGIC                0x134DAD1

#define WAIT                         10
#define WAITS_PER_SEC                1


#define FILE_BUFFER_SIZE                 65536
#define FILE_BUFFER_MINSIZE              16
#define FILE_BUFFER_MAXSIZE              128
#define KB2BYTE(kbytes)                  ((uint64_t)kbytes*(uint64)1024)
#define MB2BYTE(mbytes)                  (((uint64)KB2BYTE(mbytes))*(uint64)1024)

typedef enum WORK_STATUS
{
    WORK_STATUS_NOP          = 0x00,                     /* 未有用               */
    WORK_STATUS_INIT         = 0x01,                     /* 增加线程时，初始状态 */
    WORK_STATUS_READY        = 0x02,                     /* 子线程执行时,设置为 READY */
    WORK_STATUS_WORK         = 0x03,                     /* 子线程工作时,设置为 WORK */
    WORK_STATUS_TERM         = 0x04,                     /* 主线程获取到 SIGTERM,设置线程的状态为退出 */
    WORK_STATUS_EXIT         = 0x05,                     /* 子线程退出时，设置状态为 EXIT，在主线程中会根据此状态用于判断是否为正常退出 */
    WORK_STATUS_DEAD         = 0x06,                     /* 主线程标识子线程退出完成 */
    WORK_STATUS_WAITSTART    = 0x07                      /* 等待启动              */
} work_status;

/* poll 超时时间 */
#define NET_POLLTIMEOUT                          50

#define NET_HBTIME                               5000

/* 网络的超时时间 */
#define NET_TIMEOUT                              60000

/*-------------------------integrate split status begin------------------*/
/*
 * split此状态时，操作如下:
 *  integrate等待设置fileid和offset
 */
#define INTEGRATE_STATUS_SPLIT_WAITSET        0x01

/*
 * split此状态时，操作如下:
 *  integrate已经设置fileid和offset，开始拆分
 */
#define INTEGRATE_STATUS_SPLIT_WORKING        0x02

/*--------------------------------split status end-----------------------------------*/

/*-------------------------integrate parser status begin------------------*/
/*
 * parser初始状态
 */
#define INTEGRATE_STATUS_PARSER_INIT        0x01

/*
 * parser此状态时，操作如下:
 *  integrate开始解析
 */
#define INTEGRATE_STATUS_PARSER_WORK         0X02

#define INTEGRATE_STATUS_PARSER_EXIT         0X03

/*-------------------------integrate parser status end-------------------------*/

#define UNUSED(x) (void) (x)

#define NAMEDATALEN                          64

#define FROZEN_TXNID                         2
#define REFRESH_TXNID                        1
#define BIGTXN_TXNID                         1

#define REFRESH_LSN                          1
#define FRISTVALID_LSN                       2
#define MAX_LSN                              0xFFFFFFFFFFFFFFFF

#define CHECK_TRANSIND_START_FALSE           0
#define CHECK_TRANSIND_START_TRUE            1
#define CHECK_TRANSIND_START_METADATA        2
#define CHECK_TRANSIND_START_OTHER           3

#endif
