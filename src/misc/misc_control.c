#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "misc/ripple_misc_control.h"
#include "catalog/ripple_control.h"

static ripple_controlfiledata* m_ctrlfiledata = NULL;


/* 写 control 文件 */
void ripple_misc_controldata_flush(void)
{
    int     fd;
    char buffer[RIPPLE_CONTROL_FILE_SIZE];    /* need not be aligned */

    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(buffer, 0, 0, RIPPLE_CONTROL_FILE_SIZE);
    rmemcpy1(buffer, 0, m_ctrlfiledata, sizeof(ripple_controlfiledata));

    fd = BasicOpenFile(RIPPLE_CONTROL_FILE,
                       O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_CONTROL_FILE);
    }

    if (RIPPLE_CONTROL_FILE_SIZE != FileWrite(fd, buffer, RIPPLE_CONTROL_FILE_SIZE))
    {
        elog(RLOG_ERROR, "could not write to file %s", RIPPLE_CONTROL_FILE);
    }

    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", RIPPLE_CONTROL_FILE);
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_CONTROL_FILE);
    }

}

/* 读控制文件 */
void ripple_misc_controldata_load(void)
{
    int r = 0;
    int fd = -1;

    /*
    * Read data...
    */
    fd = BasicOpenFile(RIPPLE_CONTROL_FILE,
                        O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_CONTROL_FILE);
    }

    if(NULL == m_ctrlfiledata)
    {
        m_ctrlfiledata = (ripple_controlfiledata*)rmalloc1(sizeof(ripple_controlfiledata));
        if(NULL == m_ctrlfiledata)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(m_ctrlfiledata, 0, '\0', sizeof(ripple_controlfiledata));
    }

    /* 读取文件 */
    r = FileRead(fd, (char*)m_ctrlfiledata, sizeof(ripple_controlfiledata));
    if(r != sizeof(ripple_controlfiledata))
    {
        if (r < 0)
        {
            elog(RLOG_ERROR, "could not read file %s", RIPPLE_CONTROL_FILE);
        }
        else
        {
            elog(RLOG_ERROR, "could not read file %s : %d of %zu",
                                RIPPLE_CONTROL_FILE,
                                r,
                                sizeof(ripple_controlfiledata));
        }
    }

    FileClose(fd);
}

/* 设置状态为init */
void ripple_misc_controldata_stat_setinit(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = RIPPLE_XSYNCHSTAT_INIT;
}

/* 设置状态为rewind */
void ripple_misc_controldata_stat_setrewind(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = RIPPLE_XSYNCHSTAT_REWIND;
}

/* 设置状态为rewinding */
void ripple_misc_controldata_stat_setrewinding(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = RIPPLE_XSYNCHSTAT_REWINDING;

}

/* 设置状态为trunning */
void ripple_misc_controldata_stat_setrunning(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = RIPPLE_XSYNCHSTAT_RUNNING;

}

/* 设置状态为shutdown */
void ripple_misc_controldata_stat_setshutdown(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = RIPPLE_XSYNCHSTAT_SHUTDOWN;

}

/* 设置状态为recovery */
void ripple_misc_controldata_stat_setrecovery(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = RIPPLE_XSYNCHSTAT_RECOVERY;

}

/* 获取状态值 */
int ripple_misc_controldata_stat_get(void)
{
    return m_ctrlfiledata->stat;
}

/* 设置dbid */
void ripple_misc_controldata_database_set(Oid database)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->database = database;

}

/* 获取 dbid */
Oid ripple_misc_controldata_database_get(void* invalid)
{
    return m_ctrlfiledata->database;
}

/* 设置dbname */
void ripple_misc_controldata_dbname_set(char* dbname)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->dbname, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->dbname, 0, dbname, strlen(dbname));

}

/* 获取dbname */
char* ripple_misc_controldata_dbname_get(void)
{
    return m_ctrlfiledata->dbname;
}

/* 设置monetary */
void ripple_misc_controldata_monetary_set(char* monetary)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->monetary, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->monetary, 0, monetary, strlen(monetary));

}

/* 获取monetary */
char* ripple_misc_controldata_monetary_get(void)
{
    return m_ctrlfiledata->monetary;
}

/* 设置numeric */
void ripple_misc_controldata_numeric_set(char* numeric)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->numeric, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->numeric, 0, numeric, strlen(numeric));

}

/* 获取numeric */
char* ripple_misc_controldata_numeric_get(void)
{
    return m_ctrlfiledata->numeric;
}

/* 设置timezone */
void ripple_misc_controldata_timezone_set(char* timezone)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->timezone, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->timezone, 0, timezone, strlen(timezone));

}

/* 获取timezone */
char* ripple_misc_controldata_timezone_get(void)
{
    return m_ctrlfiledata->timezone;
}

/* 设置orgencoding */
void ripple_misc_controldata_orgencoding_set(char* encoding)
{
        if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->orgencoding, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->orgencoding, 0, encoding, strlen(encoding));

}

/* 获取orgencoding */
char* ripple_misc_controldata_orgencoding_get(void)
{
    return m_ctrlfiledata->orgencoding;
}

/* 设置dstencoding */
void ripple_misc_controldata_dstencoding_set(char* encoding)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->dstencoding, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->dstencoding, 0, encoding, strlen(encoding));

}

/* 获取dstencoding */
char* ripple_misc_controldata_dstencoding_get(void)
{
    return m_ctrlfiledata->orgencoding;
}

/* 初始化 */
void ripple_misc_controldata_init(void)
{
    m_ctrlfiledata = (ripple_controlfiledata*)rmalloc1(sizeof(ripple_controlfiledata));
    if(NULL == m_ctrlfiledata)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(m_ctrlfiledata, 0, '\0', sizeof(ripple_controlfiledata));

    ripple_misc_controldata_stat_setrewind();

    /* 将文件落盘 */
    ripple_misc_controldata_flush();
 
}

/* 清理 */
void ripple_misc_controldata_destroy(void)
{
    if(NULL == m_ctrlfiledata)
    {
        return;
    }

    rfree(m_ctrlfiledata);
    m_ctrlfiledata = NULL;
}
