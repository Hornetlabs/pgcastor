#include "app_incl.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "misc/misc_control.h"
#include "catalog/control.h"

static controlfiledata* m_ctrlfiledata = NULL;

/* Write control file */
void misc_controldata_flush(void)
{
    int  fd;
    char buffer[CONTROL_FILE_SIZE]; /* need not be aligned */

    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(buffer, 0, 0, CONTROL_FILE_SIZE);
    rmemcpy1(buffer, 0, m_ctrlfiledata, sizeof(controlfiledata));

    fd = osal_basic_open_file(CONTROL_FILE, O_RDWR | O_CREAT | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", CONTROL_FILE);
    }

    if (CONTROL_FILE_SIZE != osal_file_write(fd, buffer, CONTROL_FILE_SIZE))
    {
        elog(RLOG_ERROR, "could not write to file %s", CONTROL_FILE);
    }

    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", CONTROL_FILE);
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", CONTROL_FILE);
    }
}

/* Read control file */
void misc_controldata_load(void)
{
    int r = 0;
    int fd = -1;

    /*
     * Read data...
     */
    fd = osal_basic_open_file(CONTROL_FILE, O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", CONTROL_FILE);
    }

    if (NULL == m_ctrlfiledata)
    {
        m_ctrlfiledata = (controlfiledata*)rmalloc1(sizeof(controlfiledata));
        if (NULL == m_ctrlfiledata)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(m_ctrlfiledata, 0, '\0', sizeof(controlfiledata));
    }

    /* Read file */
    r = osal_file_read(fd, (char*)m_ctrlfiledata, sizeof(controlfiledata));
    if (r != sizeof(controlfiledata))
    {
        if (r < 0)
        {
            elog(RLOG_ERROR, "could not read file %s", CONTROL_FILE);
        }
        else
        {
            elog(RLOG_ERROR,
                 "could not read file %s : %d of %zu",
                 CONTROL_FILE,
                 r,
                 sizeof(controlfiledata));
        }
    }

    osal_file_close(fd);
}

/* Set state to init */
void misc_controldata_stat_setinit(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = XSYNCHSTAT_INIT;
}

/* Set state to rewind */
void misc_controldata_stat_setrewind(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = XSYNCHSTAT_REWIND;
}

/* Set state to rewinding */
void misc_controldata_stat_setrewinding(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = XSYNCHSTAT_REWINDING;
}

/* Set state to running */
void misc_controldata_stat_setrunning(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = XSYNCHSTAT_RUNNING;
}

/* Set state to shutdown */
void misc_controldata_stat_setshutdown(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = XSYNCHSTAT_SHUTDOWN;
}

/* Set state to recovery */
void misc_controldata_stat_setrecovery(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->stat = XSYNCHSTAT_RECOVERY;
}

/* Get state value */
int misc_controldata_stat_get(void)
{
    return m_ctrlfiledata->stat;
}

/* Set database ID */
void misc_controldata_database_set(Oid database)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    m_ctrlfiledata->database = database;
}

/* Get database ID */
Oid misc_controldata_database_get(void* invalid)
{
    return m_ctrlfiledata->database;
}

/* Set database name */
void misc_controldata_dbname_set(char* dbname)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->dbname, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->dbname, 0, dbname, strlen(dbname));
}

/* Get database name */
char* misc_controldata_dbname_get(void)
{
    return m_ctrlfiledata->dbname;
}

/* Set monetary */
void misc_controldata_monetary_set(char* monetary)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->monetary, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->monetary, 0, monetary, strlen(monetary));
}

/* Get monetary */
char* misc_controldata_monetary_get(void)
{
    return m_ctrlfiledata->monetary;
}

/* Set numeric */
void misc_controldata_numeric_set(char* numeric)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->numeric, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->numeric, 0, numeric, strlen(numeric));
}

/* Get numeric */
char* misc_controldata_numeric_get(void)
{
    return m_ctrlfiledata->numeric;
}

/* Set timezone */
void misc_controldata_timezone_set(char* timezone)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->timezone, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->timezone, 0, timezone, strlen(timezone));
}

/* Get timezone */
char* misc_controldata_timezone_get(void)
{
    return m_ctrlfiledata->timezone;
}

/* Set original encoding */
void misc_controldata_orgencoding_set(char* encoding)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->orgencoding, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->orgencoding, 0, encoding, strlen(encoding));
}

/* Get original encoding */
char* misc_controldata_orgencoding_get(void)
{
    return m_ctrlfiledata->orgencoding;
}

/* Set destination encoding */
void misc_controldata_dstencoding_set(char* encoding)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rmemset1(m_ctrlfiledata->dstencoding, 0, '\0', NAMEDATALEN);
    rmemcpy1(m_ctrlfiledata->dstencoding, 0, encoding, strlen(encoding));
}

/* Get destination encoding */
char* misc_controldata_dstencoding_get(void)
{
    return m_ctrlfiledata->orgencoding;
}

/* Initialize */
void misc_controldata_init(void)
{
    m_ctrlfiledata = (controlfiledata*)rmalloc1(sizeof(controlfiledata));
    if (NULL == m_ctrlfiledata)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(m_ctrlfiledata, 0, '\0', sizeof(controlfiledata));

    misc_controldata_stat_setrewind();

    /* Flush file to disk */
    misc_controldata_flush();
}

/* Cleanup */
void misc_controldata_destroy(void)
{
    if (NULL == m_ctrlfiledata)
    {
        return;
    }

    rfree(m_ctrlfiledata);
    m_ctrlfiledata = NULL;
}
