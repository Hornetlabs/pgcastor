#ifndef _RIPPLE_DEFINE_H
#define _RIPPLE_DEFINE_H

typedef enum RIPPLE_DBTYPE
{
    RIPPLE_DBTYPE_NOP       = 0x00,
    RIPPLE_DBTYPE_POSTGRES  = 0x01
} ripple_dbtype;


typedef enum RIPPLE_CATALOG_PGVERSION
{
    RIPPLE_PGDBVERSION_NOP      = 0x00,
    RIPPLE_PGDBVERSION_12       = 0x01
} ripple_catalog_pgversion;

typedef enum RIPPLE_PROC_TYPE
{
    RIPPLE_PROC_TYPE_NOP                = 0x00,
    RIPPLE_PROC_TYPE_CAPTURE            ,
    RIPPLE_PROC_TYPE_INTEGRATE          ,
    RIPPLE_PROC_TYPE_PGRECEIVEWAL       ,
    RIPPLE_PROC_TYPE_XMANAGER           
} ripple_proc_type;

typedef enum RIPPLE_XSYNCHSTAT
{
    RIPPLE_XSYNCHSTAT_INIT           = 0x00,
    RIPPLE_XSYNCHSTAT_REWIND         = 0x01,
    RIPPLE_XSYNCHSTAT_REWINDING      = 0x02,
    RIPPLE_XSYNCHSTAT_RUNNING        = 0x03,
    RIPPLE_XSYNCHSTAT_SHUTDOWN       = 0x04,
    RIPPLE_XSYNCHSTAT_RECOVERY       = 0x05,
} ripple_xsynchstat;

typedef enum RIPPLE_CAPTURERELOAD_STATUS
{
    RIPPLE_CAPTURERELOAD_STATUS_UNSET                   = 0x00,
    RIPPLE_CAPTURERELOAD_STATUS_RELOADING_PARSERWAL     = 0x01,
    RIPPLE_CAPTURERELOAD_STATUS_RELOADING_WRITE         = 0x02
}ripple_capturereload_status;

#define RIPPLE_DBTYPE_POSTGRES              "postgres"
#define RIPPLE_DBVERSION_POSTGRES_12        "12"

#define RIPPLE_LOCK_FILE                    "proc.lock"
#define RIPPLE_CONTROL_FILE                 "ripple.ctrl"
#define RIPPLE_DECODE_BASE_FILE             "ripple_base.dat"
#define RIPPLE_CACHEDIR                     "cache"
#define RIPPLE_STAT                         "stat"
#define RIPPLE_STAT_DECODE                  "decode.stat"
#define RIPPLE_CONSTRAINT_FILE              "catalog/ripple_constraint.bat"
#define RIPPLE_SYSDICTS_FILE                "catalog/ripple_sysdicts.bat"
#define RIPPLE_SYSDICTS_TMP_FILE            "catalog/ripple_sysdicts.bat.tmp"
#define RIPPLE_SYNC_STATUSTABLE_NAME        "ripple_sync_status"
#define RIPPLE_CATALOG_DIR                  "catalog"
#define RIPPLE_FILTER_DIR                   "filter"
#define RIPPLE_STORAGE_TRAIL_DIR            "trail"
#define RIPPLE_STORAGE_BIG_TRANSACTION_DIR  "bigtxn"
#define RIPPLE_FILTER_OIDS_FILE             "oids.bat"
#define RIPPLE_CAPTURE_STATUS_FILE          "capture.stat"
#define RIPPLE_INTEGRATE_STATUS_FILE        "integrate.stat"
#define RIPPLE_CAPTURE_STATUS_FILE_TEMP     "capture.stat.tmp"
#define RIPPLE_INTEGRATE_STATUS_FILE_TEMP   "integrate.stat.tmp"
#define RIPPLE_FILTER_DATASET_TMP           "filterdataset.dat.tmp"
#define RIPPLE_FILTER_DATASET               "filterdataset.dat"
#define RIPPLE_ONLINEREFRESH_DAT            "onlinerefresh.dat"
#define RIPPLE_ONLINEREFRESH_STATUS         "onlinerefresh.status"
#define RIPPLE_REFRESH_STATS                "stats.dat"
#define RIPPLE_BIGTRANSACTION_FILE          "bigtransaction.status"
#define RIPPLE_BIGTRANSACTION_FILE_TEMP     "bigtransaction.status.tmp"
#define RIPPLE_ONLINEREFRESHABANDON_DAT     "onlinerefreshabandon.dat"
#define RIPPLE_REFRESH_STATUS               "refresh.status"

#define RIPPLE_TIMEZONE                     "PRC"
#define RIPPLE_NUMERIC                      "zh_CN.UTF-8"
#define RIPPLE_MONETARY                     "zh_CN.UTF-8"
#define RIPPLE_ORGENCODING                  "UTF8"
#define RIPPLE_DSTENCODING                  "UTF8"

#define RIPPLE_TBINCLUDE                    "table"
#define RIPPLE_TBEXCLUDE                    "tableexclude"
#define RIPPLE_ADDTABLEPATTERN              "addtablepattern"


#define RIPPLE_REFRESH_INCREMENT            "increment"
#define RIPPLE_REFRESH_REFRESH              "refresh"
#define RIPPLE_REFRESH_PARTIAL              "partial"
#define RIPPLE_REFRESH_COMPLETE             "complete"
#define RIPPLE_REFRESH_ONLINEREFRESH        "onlinerefresh"

#define RIPPLE_FILETRANSFER_DIR             "filetransfer"

#define RIPPLE_MAX_MAPPINGS                 62
#define RIPPLE_CONTROL_FILE_SIZE            1024
#define RIPPLE_DECODE_STAT                  RIPPLE_CONTROL_FILE_SIZE
#define RIPPLE_FILE_BLK_SIZE                8192
#define RIPPLE_FILE_SIZE                    (1024*1024)
#define RIPPLE_PAGE_HEADER_SIZE             4
#define RIPPLE_FirstNormalObjectId          16384
#define RIPPLE_SYNCNAMESPACE_MAXOID         15233

#define RIPPLE_BIG_MEMORY                   (RIPPLE_FILE_SIZE)

#define RIPPLE_FILE_BLK_MOD(offset)         (RIPPLE_FILE_BLK_SIZE - (offset % RIPPLE_FILE_BLK_SIZE))

#define RIPPLE_MAXPATH                      512
#define RIPPLE_ABSPATH                      1024
#define RIPPLE_LINESIZE                     1024
#define RIPPLE_COMMANDSIZE                  2048


#define RIPPLE_FILEID2DIR(fileid)           (fileid%256)

#define RIPPLE_FILE_OFFSET_INVALID          0x80000000


#define RIPPLE_PG_DFAULT_TABLESPACE         1663
#define RIPPLE_MAX_EXEC_SQL_LEN             1024

#define RIPPLE_MAGIC                        0x1571
#define RIPPLE_SYSDICT_MAGIC                0x134DAD1

#define RIPPLE_WAIT                         10
#define RIPPLE_WAITS_PER_SEC                1


#define RIPPLE_FILE_BUFFER_SIZE                 65536
#define RIPPLE_FILE_BUFFER_MINSIZE              16
#define RIPPLE_FILE_BUFFER_MAXSIZE              128
#define RIPPLE_KB2BYTE(kbytes)                  ((uint64_t)kbytes*(uint64)1024)
#define RIPPLE_MB2BYTE(mbytes)                  (((uint64)RIPPLE_KB2BYTE(mbytes))*(uint64)1024)

typedef enum RIPPLE_WORK_STATUS
{
    RIPPLE_WORK_STATUS_NOP          = 0x00,                     /* 未有用               */
    RIPPLE_WORK_STATUS_INIT         = 0x01,                     /* 增加线程时，初始状态 */
    RIPPLE_WORK_STATUS_READY        = 0x02,                     /* 子线程执行时,设置为 READY */
    RIPPLE_WORK_STATUS_WORK         = 0x03,                     /* 子线程工作时,设置为 WORK */
    RIPPLE_WORK_STATUS_TERM         = 0x04,                     /* 主线程获取到 SIGTERM,设置线程的状态为退出 */
    RIPPLE_WORK_STATUS_EXIT         = 0x05,                     /* 子线程退出时，设置状态为 EXIT，在主线程中会根据此状态用于判断是否为正常退出 */
    RIPPLE_WORK_STATUS_DEAD         = 0x06,                     /* 主线程标识子线程退出完成 */
    RIPPLE_WORK_STATUS_WAITSTART    = 0x07                      /* 等待启动              */
} ripple_work_status;

/* poll 超时时间 */
#define RIPPLE_NET_POLLTIMEOUT                          50

#define RIPPLE_NET_HBTIME                               5000

/* 网络的超时时间 */
#define RIPPLE_NET_TIMEOUT                              60000

/*-------------------------integrate split status begin------------------*/
/*
 * split此状态时，操作如下:
 *  integrate等待设置fileid和offset
 */
#define RIPPLE_INTEGRATE_STATUS_SPLIT_WAITSET        0x01

/*
 * split此状态时，操作如下:
 *  integrate已经设置fileid和offset，开始拆分
 */
#define RIPPLE_INTEGRATE_STATUS_SPLIT_WORKING        0x02

/*--------------------------------split status end-----------------------------------*/

/*-------------------------integrate parser status begin------------------*/
/*
 * parser初始状态
 */
#define RIPPLE_INTEGRATE_STATUS_PARSER_INIT        0x01

/*
 * parser此状态时，操作如下:
 *  integrate开始解析
 */
#define RIPPLE_INTEGRATE_STATUS_PARSER_WORK         0X02

#define RIPPLE_INTEGRATE_STATUS_PARSER_EXIT         0X03

/*-------------------------integrate parser status end-------------------------*/

#define RIPPLE_UNUSED(x) (void) (x)

#define RIPPLE_NAMEDATALEN                          64

#define RIPPLE_FROZEN_TXNID                         2
#define RIPPLE_REFRESH_TXNID                        1
#define RIPPLE_BIGTXN_TXNID                         1

#define RIPPLE_REFRESH_LSN                          1
#define RIPPLE_FRISTVALID_LSN                       2
#define RIPPLE_MAX_LSN                              0xFFFFFFFFFFFFFFFF

#define RIPPLE_CHECK_TRANSIND_START_FALSE           0
#define RIPPLE_CHECK_TRANSIND_START_TRUE            1
#define RIPPLE_CHECK_TRANSIND_START_METADATA        2
#define RIPPLE_CHECK_TRANSIND_START_OTHER           3

#endif
