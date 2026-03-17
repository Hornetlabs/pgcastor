#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/string/strtrim.h"
#include "translog/wal/translog_walcontrol.h"

/* control 文件初始化 */
static translog_walcontrol* translog_walcontrol_init(void)
{
    translog_walcontrol* control = NULL;

    control = rmalloc0(sizeof(translog_walcontrol));
    if(NULL == control)
    {
        elog(RLOG_WARNING, "walcontrol init error");
        return NULL;
    }
    rmemset0(control, 0, '\0', sizeof(translog_walcontrol));
    control->stat = TRANSLOG_WALCONTROL_STAT_INIT;
    control->segsize = 0;
    control->dbtli = InvalidTimeLineID;
    control->startpos = InvalidXLogRecPtr;
    control->tli = InvalidTimeLineID;
    control->slotname[0] = '\0';
    control->restorecmd[0] = '\0';

    return control;
}

/* 设置流复制的起始 lsn */
void translog_walcontrol_setstartpos(translog_walcontrol* walctrl, XLogRecPtr lsn)
{
    walctrl->startpos = lsn;
}

/* 设置流复制的 timeline */
void translog_walcontrol_settli(translog_walcontrol* walctrl, TimeLineID tli)
{
    walctrl->tli = tli;
}

/* 设置流复制中数据库当前的 timeline */
void translog_walcontrol_setdbtli(translog_walcontrol* walctrl, TimeLineID tli)
{
    walctrl->dbtli = tli;
}

/* 设置事务日志的大小 */
void translog_walcontrol_setsegsize(translog_walcontrol* walctrl, uint32 segsize)
{
    walctrl->segsize = segsize;
}

/* 设置复制槽名称 */
void translog_walcontrol_setslotname(translog_walcontrol* walctrl, char* slotname)
{
    if(NULL == slotname)
    {
        return;
    }
    rmemcpy1(walctrl->slotname, 0, slotname, strlen(slotname));
}

/* 设置restore command */
void translog_walcontrol_setrestorecmd(translog_walcontrol* walctrl, char* restorecmd)
{
    if(NULL == restorecmd || '\0' == restorecmd[0])
    {
        return;
    }
    rmemcpy1(walctrl->restorecmd, 0, restorecmd, strlen(restorecmd));
}

/* 加载 Control 文件 */
translog_walcontrol* translog_walcontrol_load(char* abspath)
{
    uint32 hi                           = 0;
    uint32 lo                           = 0;
    char* cptr                          = NULL;
    char* key                           = NULL;
    char* value                         = NULL;
    FILE* fp                            = NULL;
    translog_walcontrol* control = NULL;
    struct stat st;
    char content[LINESIZE]   = { 0 };

    control = translog_walcontrol_init();
    if(NULL == control)
    {
        elog(RLOG_WARNING, "load control file error");
        return NULL;
    }

    /* 校验文件是否存在，存在则打开 */
    if (-1 == stat(abspath, &st))
    {
        /* 打开文件失败, 那么查看错误码 */
        if (ENOENT != errno)
        {
            elog(RLOG_WARNING, "control load check %s stat error", strerror(errno));
            goto translog_walcontrol_load_error;
        }

        return control;
    }

    /* 打开文件 */
    fp = osal_file_fopen(abspath, "r");
    if (NULL == fp)
    {
        elog(RLOG_WARNING, "open file %s error %s", abspath, strerror(errno));
        return false;
    }

    while (NULL != osal_file_fgets(fp, LINESIZE, content))
    {
        cptr = leftstrtrim(content, LINESIZE);
        if ('\0' == cptr[0])
        {
            continue;
        }

        cptr = rightstrtrim(cptr);
        if ('\0' == cptr[0])
        {
            continue;
        }

        key = cptr;
        value = strstr(cptr, ":");
        value[0] = '\0';
        value++;

        if (0 == strcmp("STAT", key))
        {
            control->stat = atoi(value);
        }
        else if (0 == strcmp("SEGSIZE", key))
        {
            control->segsize = atoi(value);
        }
        else if (0 == strcmp("TIMELINE", key))
        {
            control->tli = atoi(value);
        }
        else if (0 == strcmp("DBTIMELINE", key))
        {
            control->dbtli = atoi(value);
        }
        else if (0 == strcmp("STARTPOS", key))
        {
            sscanf(value, "%X/%08X", &hi, &lo);
            control->startpos = (((uint64)hi)<<32) + lo;
        }
        else if (0 == strcmp("SLOTNAME", key))
        {
            rmemcpy1(control->slotname, 0, value, strlen(value));
        }
        else if (0 == strcmp("RESTORECOMMAND", key))
        {
            rmemcpy1(control->restorecmd, 0, value, strlen(value));
        }
        else
        {
            elog(RLOG_WARNING, "unknown control item:%s", key);
            goto translog_walcontrol_load_error;
        }
    }

    if (NULL != fp)
    {
        osal_free_file(fp);
    }

    return control;

translog_walcontrol_load_error:
    if (NULL != fp)
    {
        osal_free_file(fp);
    }
    rfree(control);
    return NULL;
}

/* Control 文件落盘 */
bool translog_walcontrol_flush(translog_walcontrol* walctrl, char* data)
{
    int len                         = -1;
    FILE* fp                        = NULL;
    char abspath[ABSPATH]    = { 0 };
    char tabspath[ABSPATH]   = { 0 };
    char content[LINESIZE]   = { 0 };

    /* 生成临时文件目录 */
    snprintf(abspath, ABSPATH, "%s/receivewal.control", data);
    snprintf(tabspath, ABSPATH, "%s/receivewal.control.tmp", data);

    /* 先清理掉上次残留 */
    osal_durable_unlink(tabspath, RLOG_DEBUG);

    /* 打开文件 */
    fp = osal_file_fopen(tabspath, "w+");
    if(NULL == fp)
    {
        elog(RLOG_WARNING, "open file %s error %s", tabspath, strerror(errno));
        return false;
    }

    /*
     * 写文件
     *  1、状态:              STAT:%d
     *  2、文件大小        SEGSIZE:%d
     *  3、时间线         TIMELINE:%d
     *  4、数据库时间线 DBTIMELINE:%d
     *  5、起点           STARTPOS:%X/%X
     *  6、复制槽         SLOTNAME:%s
     *  7、复制命令 RESTORECOMMAND:%s
     */
    /* 状态 */
    len = snprintf(content, LINESIZE, "STAT:%d\n", walctrl->stat);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    /* 文件大小 */
    len = snprintf(content, LINESIZE, "SEGSIZE:%d\n", walctrl->segsize);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    /* 时间线 */
    len = snprintf(content, LINESIZE, "TIMELINE:%d\n", walctrl->tli);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    /* 数据库时间线 */
    len = snprintf(content, LINESIZE, "DBTIMELINE:%d\n", walctrl->dbtli);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    /* 起点 */
    len = snprintf(content, LINESIZE, "STARTPOS:%X/%08X\n", (uint32)(walctrl->startpos>>32), (uint32)walctrl->startpos);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    /* 复制槽 */
    len = snprintf(content, LINESIZE, "SLOTNAME:%s\n", walctrl->slotname);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    /* 复制命令 */
    len = snprintf(content, LINESIZE, "RESTORECOMMAND:%s\n", walctrl->restorecmd);
    if (1 != osal_file_fwrite(fp, len, 1, content))
    {
        elog(RLOG_WARNING, "write file error, %s", strerror(errno));
        osal_free_file(fp);
        fp = NULL;
        return false;
    }

    osal_free_file(fp);

    /* 重命名 */
    osal_durable_rename(tabspath, abspath, RLOG_INFO);
    return true;
}
