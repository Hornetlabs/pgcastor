#ifndef _DEFINE_H
#define _DEFINE_H

typedef enum DBTYPE
{
    DBTYPE_NOP = 0x00,
    DBTYPE_POSTGRES = 0x01
} dbtype;

typedef enum CATALOG_PGVERSION
{
    PGDBVERSION_NOP = 0x00,
    PGDBVERSION_12 = 0x01
} catalog_pgversion;

typedef enum PROC_TYPE
{
    PROC_TYPE_NOP = 0x00,
    PROC_TYPE_CAPTURE,
    PROC_TYPE_INTEGRATE,
    PROC_TYPE_PGRECEIVEWAL,
    PROC_TYPE_XMANAGER
} proc_type;

typedef enum XSYNCHSTAT
{
    XSYNCHSTAT_INIT = 0x00,
    XSYNCHSTAT_REWIND = 0x01,
    XSYNCHSTAT_REWINDING = 0x02,
    XSYNCHSTAT_RUNNING = 0x03,
    XSYNCHSTAT_SHUTDOWN = 0x04,
    XSYNCHSTAT_RECOVERY = 0x05,
} xsynchstat;

typedef enum CAPTURERELOAD_STATUS
{
    CAPTURERELOAD_STATUS_UNSET = 0x00,
    CAPTURERELOAD_STATUS_RELOADING_PARSERWAL = 0x01,
    CAPTURERELOAD_STATUS_RELOADING_WRITE = 0x02
} capturereload_status;

#define DBTYPE_POSTGRES             "postgres"
#define DBVERSION_POSTGRES_12       "12"

#define LOCK_FILE                   "proc.lock"
#define CONTROL_FILE                "castor.ctrl"
#define DECODE_BASE_FILE            "base.dat"
#define CACHEDIR                    "cache"
#define STAT                        "stat"
#define STAT_DECODE                 "decode.stat"
#define CONSTRAINT_FILE             "catalog/constraint.bat"
#define SYSDICTS_FILE               "catalog/sysdicts.bat"
#define SYSDICTS_TMP_FILE           "catalog/sysdicts.bat.tmp"
#define SYNC_STATUSTABLE_NAME       "sync_status"
#define CATALOG_DIR                 "catalog"
#define FILTER_DIR                  "filter"
#define STORAGE_TRAIL_DIR           "trail"
#define STORAGE_BIG_TRANSACTION_DIR "bigtxn"
#define FILTER_OIDS_FILE            "oids.bat"
#define CAPTURE_STATUS_FILE         "capture.stat"
#define INTEGRATE_STATUS_FILE       "integrate.stat"
#define CAPTURE_STATUS_FILE_TEMP    "capture.stat.tmp"
#define INTEGRATE_STATUS_FILE_TEMP  "integrate.stat.tmp"
#define FILTER_DATASET_TMP          "filterdataset.dat.tmp"
#define FILTER_DATASET              "filterdataset.dat"
#define ONLINEREFRESH_DAT           "onlinerefresh.dat"
#define ONLINEREFRESH_STATUS        "onlinerefresh.status"
#define REFRESH_STATS               "stats.dat"
#define BIGTRANSACTION_FILE         "bigtransaction.status"
#define BIGTRANSACTION_FILE_TEMP    "bigtransaction.status.tmp"
#define ONLINEREFRESHABANDON_DAT    "onlinerefreshabandon.dat"
#define REFRESH_STATUS              "refresh.status"
#define REFRESH_REFRESHTABLES       "refreshtables.dat"

#define TIMEZONE                    "PRC"
#define NUMERIC                     "zh_CN.UTF-8"
#define MONETARY                    "zh_CN.UTF-8"
#define ORGENCODING                 "UTF8"
#define DSTENCODING                 "UTF8"

#define TBINCLUDE                   "table"
#define TBEXCLUDE                   "tableexclude"
#define ADDTABLEPATTERN             "addtablepattern"

#define REFRESH_INCREMENT           "increment"
#define REFRESH_REFRESH             "refresh"
#define REFRESH_PARTIAL             "partial"
#define REFRESH_COMPLETE            "complete"
#define REFRESH_ONLINEREFRESH       "onlinerefresh"

#define MAX_MAPPINGS                62
#define CONTROL_FILE_SIZE           1024
#define DECODE_STAT                 CONTROL_FILE_SIZE
#define FILE_BLK_SIZE               8192
#define FILE_SIZE                   (1024 * 1024)
#define PAGE_HEADER_SIZE            4
#define FirstNormalObjectId         16384
#define SYNCNAMESPACE_MAXOID        15233

#define BIG_MEMORY                  (FILE_SIZE)

#define FILE_BLK_MOD(offset)        (FILE_BLK_SIZE - (offset % FILE_BLK_SIZE))

#define MAXPATH                     512
#define ABSPATH                     1024
#define LINESIZE                    1024
#define COMMANDSIZE                 2048

#define FILEID2DIR(fileid)          (fileid % 256)

#define FILE_OFFSET_INVALID         0x80000000

#define PG_DFAULT_TABLESPACE        1663
#define MAX_EXEC_SQL_LEN            1024

#define MAGIC                       0x1571
#define SYSDICT_MAGIC               0x134DAD1

#define WAIT                        10
#define WAITS_PER_SEC               1

#define FILE_BUFFER_SIZE            65536
#define FILE_BUFFER_MINSIZE         16
#define FILE_BUFFER_MAXSIZE         128
#define KB2BYTE(kbytes)             ((uint64_t)kbytes * (uint64)1024)
#define MB2BYTE(mbytes)             (((uint64)KB2BYTE(mbytes)) * (uint64)1024)

typedef enum WORK_STATUS
{
    WORK_STATUS_NOP = 0x00,      /* unused                       */
    WORK_STATUS_INIT = 0x01,     /* initial state when thread is created */
    WORK_STATUS_READY = 0x02,    /* set to READY when sub-thread starts executing */
    WORK_STATUS_WORK = 0x03,     /* set to WORK when sub-thread is running */
    WORK_STATUS_TERM = 0x04,     /* main thread received SIGTERM, set thread state to exit */
    WORK_STATUS_EXIT = 0x05,     /* sub-thread exits, set state to EXIT; main thread checks this to
                                    determine if it exited normally */
    WORK_STATUS_DEAD = 0x06,     /* main thread marks sub-thread exit as complete */
    WORK_STATUS_WAITSTART = 0x07 /* waiting to start              */
} work_status;

/* poll timeout in milliseconds */
#define NET_POLLTIMEOUT 50

#define NET_HBTIME      5000

/* network timeout in milliseconds */
#define NET_TIMEOUT 60000

/*-------------------------integrate split status begin------------------*/
/*
 * In this split state:
 *  integrate is waiting for fileid and offset to be set
 */
#define INTEGRATE_STATUS_SPLIT_WAITSET 0x01

/*
 * In this split state:
 *  fileid and offset have been set, integrate starts splitting
 */
#define INTEGRATE_STATUS_SPLIT_WORKING 0x02

/*--------------------------------split status end-----------------------------------*/

/*-------------------------integrate parser status begin------------------*/
/*
 * parser initial state
 */
#define INTEGRATE_STATUS_PARSER_INIT 0x01

/*
 * In this parser state:
 *  integrate starts parsing
 */
#define INTEGRATE_STATUS_PARSER_WORK 0X02

#define INTEGRATE_STATUS_PARSER_EXIT 0X03

/*-------------------------integrate parser status end-------------------------*/

#define UNUSED(x)                     (void)(x)

#define NAMEDATALEN                   64

#define FROZEN_TXNID                  2
#define REFRESH_TXNID                 1
#define BIGTXN_TXNID                  1

#define REFRESH_LSN                   1
#define FRISTVALID_LSN                2
#define MAX_LSN                       0xFFFFFFFFFFFFFFFF

#define CHECK_TRANSIND_START_FALSE    0
#define CHECK_TRANSIND_START_TRUE     1
#define CHECK_TRANSIND_START_METADATA 2
#define CHECK_TRANSIND_START_OTHER    3

#endif
