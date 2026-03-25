#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dttime/dttimestamp.h"
#include "utils/conn/conn.h"
#include "utils/init/databaserecv.h"
#include "translog/translog_recvlogdb.h"
#include "translog/wal/translog_walcontrol.h"
#include "translog/wal/translog_waltimeline.h"
#include "translog/wal/translog_walmsg.h"
#include "translog/wal/translog_recvlog.h"
#include "translog/wal/translog_walam.h"

typedef enum TRANSLOG_RECVLOG_STAT
{
    TRANSLOG_RECVLOG_STAT_NOP = 0x00,

    /* connect to database */
    TRANSLOG_RECVLOG_STAT_CONN,

    /* get database timeline and pos */
    TRANSLOG_RECVLOG_STAT_IDENTIFY_SYSTEM,

    /* read timeline */
    TRANSLOG_RECVLOG_STAT_TIMELINE,

    /* start streaming replication */
    TRANSLOG_RECVLOG_STAT_STARTREPLICATION,

    /* streaming replication */
    TRANSLOG_RECVLOG_STAT_STREAMING,

} translog_recvlog_stat;

/* initialize structure */
translog_recvlog* translog_recvlog_init(void)
{
    translog_recvlog* recvwal = NULL;

    recvwal = rmalloc0(sizeof(translog_recvlog));
    if (NULL == recvwal)
    {
        elog(RLOG_WARNING, "recwal init error");
        return NULL;
    }
    rmemset0(recvwal, 0, '\0', sizeof(translog_recvlog));
    recvwal->dbtype = TRANSLOG_RECVLOG_DBTYPE_NOP;
    recvwal->dbversion = TRANSLOG_RECVLOG_VERSION_NOP;
    recvwal->segsize = 0;
    recvwal->fd = -1;
    recvwal->senddone = 0;
    recvwal->data = NULL;
    recvwal->dbtli = InvalidTimeLineID;
    recvwal->startpos = InvalidXLogRecPtr;
    recvwal->sysidentifier = NULL;
    recvwal->tli = InvalidTimeLineID;
    recvwal->slotname = NULL;
    recvwal->restorecmd = NULL;
    return recvwal;
}

/* timeline */
void translog_recvlog_settli(translog_recvlog* recvwal, TimeLineID tli)
{
    recvwal->tli = tli;
}

/* database timeline */
void translog_recvlog_setdbtli(translog_recvlog* recvwal, TimeLineID tli)
{
    recvwal->dbtli = tli;
}

/* set startpos */
void translog_recvlog_setstartpos(translog_recvlog* recvwal, XLogRecPtr lsn)
{
    recvwal->startpos = lsn;
}

/* set segsize */
void translog_recvlog_setsegsize(translog_recvlog* recvwal, uint32 segsize)
{
    recvwal->segsize = segsize;
}

/* set dbtype */
void translog_recvlog_setdbtype(translog_recvlog* recvwal, translog_recvlog_dbtype dbtype)
{
    recvwal->dbtype = dbtype;
}

/* set data directory */
bool translog_recvlog_setdata(translog_recvlog* recvwal, char* data)
{
    int dlen = 0;

    dlen = strlen(data);
    dlen += 1;
    recvwal->data = rmalloc0(dlen);
    if (NULL == recvwal->data)
    {
        elog(RLOG_WARNING, "set data error");
        return false;
    }
    rmemset0(recvwal->data, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->data, 0, data, dlen);
    return true;
}

/* set slotname directory */
bool translog_recvlog_setslotname(translog_recvlog* recvwal, char* slotname)
{
    int dlen = 0;

    dlen = strlen(slotname);
    dlen += 1;
    recvwal->slotname = rmalloc0(dlen);
    if (NULL == recvwal->slotname)
    {
        elog(RLOG_WARNING, "set slotname error");
        return false;
    }
    rmemset0(recvwal->slotname, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->slotname, 0, slotname, dlen);
    return true;
}

/* set restore command directory */
bool translog_recvlog_setrestorecmd(translog_recvlog* recvwal, char* restorecmd)
{
    bool charf = false;
    bool charp = false;
    int  dlen = 0;
    int  index = 0;

    if (NULL == restorecmd || '\0' == restorecmd[0])
    {
        return true;
    }

    dlen = strlen(restorecmd);
    dlen += 1;
    recvwal->restorecmd = rmalloc0(dlen);
    if (NULL == recvwal->restorecmd)
    {
        elog(RLOG_WARNING, "set restorecmd error");
        return false;
    }
    rmemset0(recvwal->restorecmd, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->restorecmd, 0, restorecmd, dlen);

    /* replace %f and %p with %s in command */
    for (index = 0; index < dlen; index++)
    {
        if ('%' != recvwal->restorecmd[index])
        {
            continue;
        }

        index++;
        if (index >= dlen)
        {
            elog(RLOG_WARNING, "config item restore_command:%s is incorrectly configured.",
                 restorecmd);
            return false;
        }

        if ('f' != recvwal->restorecmd[index] && 'p' != recvwal->restorecmd[index])
        {
            elog(
                RLOG_WARNING,
                "config item restore_command:%s is incorrectly configured, only support \%f or \%p",
                restorecmd);
            return false;
        }

        if ('f' == recvwal->restorecmd[index])
        {
            charf = true;
        }
        else
        {
            charp = true;
        }
    }

    if (false == charf || false == charp)
    {
        elog(RLOG_WARNING, "config item restore_command:%s must include \%f \%p", restorecmd);
        return false;
    }
    return true;
}

/* set sysidentifier */
bool translog_recvlog_setsysidentifier(translog_recvlog* recvwal, char* sysidentifier)
{
    int dlen = 0;

    dlen = strlen(sysidentifier);
    dlen += 1;
    recvwal->sysidentifier = rmalloc0(dlen);
    if (NULL == recvwal->sysidentifier)
    {
        elog(RLOG_WARNING, "set sysidentifier error");
        return false;
    }
    rmemset0(recvwal->sysidentifier, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->sysidentifier, 0, sysidentifier, dlen);
    return true;
}

/*------------------------------------execute statement operations begin-------------------------*/

/*
 * execute SHOW wal_segment_size to get transaction log size
 */
static bool translog_recvlog_showwalsegsize(translog_recvlog* recvwal, PGconn* conn,
                                            translog_walcontrol* walctrl)
{
    uint32 segsize = 0;

    if (false == databaserecv_showwalsegmentsize(conn, &segsize))
    {
        elog(RLOG_WARNING, "SHOW wal_segment_size error");
        return false;
    }

    translog_recvlog_setsegsize(recvwal, segsize);
    translog_walcontrol_setsegsize(walctrl, segsize);
    return true;
}

/*
 * execute identify system
 *  1. update timeline and startpos
 *  2. set control file status to work
 *  3. flush control file to disk
 */
static bool translog_recvlog_identifysystem(translog_recvlog* recvwal, PGconn* conn,
                                            translog_walcontrol* walctrl)
{
    TimeLineID dbtli = InvalidTimeLineID;
    XLogRecPtr dblsn = InvalidXLogRecPtr;

    /* execute identify system command, get startpos and database timeline */
    if (false == databaserecv_identifysystem(conn, &dbtli, &dblsn))
    {
        elog(RLOG_WARNING, "identify system error");
        return false;
    }

    /* calculate startpos and segno */
    if (InvalidXLogRecPtr == recvwal->startpos)
    {
        recvwal->startpos = dblsn;
    }

    /* start from file header */
    recvwal->startpos -= PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize);
    translog_walcontrol_setstartpos(walctrl, recvwal->startpos);
    recvwal->segno = PGWALBYTETOSEG(recvwal->startpos, recvwal->segsize);

    if (recvwal->dbtli > dbtli)
    {
        elog(RLOG_WARNING, "recvwal's database timeline %u large than database timeline %u",
             recvwal->dbtli, dbtli);
        return false;
    }
    translog_recvlog_setdbtli(recvwal, dbtli);
    translog_walcontrol_setdbtli(walctrl, dbtli);
    if (InvalidTimeLineID == recvwal->tli)
    {
        translog_recvlog_settli(recvwal, dbtli);
        translog_walcontrol_settli(walctrl, dbtli);
    }

    return true;
}

/*
 * get timeline file
 *  1. when timeline is 1, no processing needed
 *  2. get timeline file
 *      2.1 if exists locally, don't fetch
 *      2.2 if not exists locally, execute command to fetch
 */
static bool translog_recvlog_timelinehistory(translog_recvlog* recvwal, PGconn* conn)
{
    char* filename = NULL;
    char* content = NULL;
    if (1 == recvwal->tli)
    {
        return true;
    }

    /* check if file exists */
    if (true == translog_waltimeline_exist(recvwal->data, recvwal->tli))
    {
        return true;
    }

    /* get timeline */
    if (false == databaserecv_timelinehistory(conn, recvwal->tli, &filename, &content))
    {
        elog(RLOG_WARNING, "exec timline history command error");
        if (NULL != filename)
        {
            rfree(filename);
        }

        if (NULL != content)
        {
            rfree(content);
        }
        return false;
    }

    /* write timeline file */
    if (false == translog_waltimeline_flush(recvwal->data, filename, content))
    {
        elog(RLOG_WARNING, "write timeline history file error");
        rfree(filename);
        rfree(content);
        return false;
    }
    rfree(filename);
    rfree(content);
    return true;
}

/*
 * start streaming replication
 */
static bool translog_recvlog_startreplication(translog_recvlog* recvwal, PGconn* conn)
{
    if (false ==
        databaserecv_startreplication(conn, recvwal->tli, recvwal->startpos, recvwal->slotname))
    {
        elog(RLOG_WARNING, "start replication error");
        return false;
    }
    return true;
}

/*------------------------------------execute statement operations   end-------------------------*/

/* get data from archive file */
static bool translog_recvlog_execrestorecmd(translog_recvlog* recvwal)
{
    /*
     * 1. assemble restorecommand
     * 2. execute restorecommand
     * 3. get execution result
     */
    int   ret = 0;
    int   index = 0;
    int   cindex = 0;
    int   clen = 0;
    char* cptr = NULL;
    FILE* fp = NULL;
    char  walfile[NAMEDATALEN] = {0};
    char  resultfile[NAMEDATALEN] = {0};
    char  command[COMMANDSIZE] = {0};
    char  fileline[LINESIZE] = {0};

    if (NULL == recvwal->restorecmd || '\0' == recvwal->restorecmd[0])
    {
        elog(RLOG_WARNING, "unconfig restore_command item");
        return false;
    }

    /* filename */
    snprintf(walfile, NAMEDATALEN, "%08X%08X%08X", /* directory/timeline segno size */
             recvwal->tli, (uint32)((recvwal->segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)((recvwal->segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    /* command */
    snprintf(resultfile, NAMEDATALEN, "%s", "rcmd.result");

    /* replace %f and %p */
    cptr = command;
    clen = strlen(recvwal->restorecmd);
    for (index = 0; index < clen; index++)
    {
        if ('%' != recvwal->restorecmd[index])
        {
            cptr[cindex] = recvwal->restorecmd[index];
            cindex++;
            continue;
        }

        index++;
        if ('f' == recvwal->restorecmd[index])
        {
            snprintf(cptr + cindex, COMMANDSIZE - cindex, "%s", walfile);
            cindex = strlen(cptr);
        }
        else if ('p' == recvwal->restorecmd[index])
        {
            snprintf(cptr + cindex, COMMANDSIZE - cindex, "%s/%s", recvwal->data, walfile);
            cindex = strlen(cptr);
        }
    }

    /* add output file */
    snprintf(cptr + cindex, COMMANDSIZE - cindex, " >%s 2>&1", resultfile);

translog_recvwal_execrestorecmd_retry:
    /* execute command */
    ret = system(command);
    if (0 == ret)
    {
        return true;
    }

    /* execution failed, check rcmd.result file */
    fp = osal_file_fopen(resultfile, "r");
    if (NULL == fp)
    {
        elog(RLOG_WARNING, "open file %s error, %s", resultfile, strerror(errno));
        return false;
    }

    while (NULL != fgets(fileline, LINESIZE, fp))
    {
        break;
    }
    osal_free_file(fp);
    fp = NULL;

    if ('\0' == fileline[0])
    {
        elog(RLOG_WARNING, "can not got %s result", command);
        return false;
    }

    if (NULL != strstr(fileline, "Connection timed out"))
    {
        /* timeout */
        elog(RLOG_WARNING, "%s", fileline);
        goto translog_recvwal_execrestorecmd_retry;
    }
    else
    {
        elog(RLOG_WARNING, "%s", fileline);
        return false;
    }
    return true;
}

/* streaming replication log receive */
bool translog_recvlog_main(translog_recvlog* recvwal)
{
    /*
     * 1. connect to database
     * 2. get segsize
     * 3. execute identity system
     *      update control file based on execution result
     * 4. get timeline file
     * 5. start streaming replication
     */
    translog_recvlog_stat jobstat = TRANSLOG_RECVLOG_STAT_NOP;
    /* flag to indicate receiving end command from source */
    bool                        endcmd = false;
    int                         error = 0;
    int                         dberror = 0;
    int                         recvlen = 0;
    char*                       buffer = NULL;
    char*                       conninfo = NULL;
    PGconn*                     conn = NULL;
    translog_recvlog_amroutine* method = NULL;
    translog_walcontrol         walctrl = {0};

    conninfo = guc_getConfigOption(CFG_KEY_PRIMARY_CONN_INFO);
    if (NULL == conninfo || '\0' == conninfo[0])
    {
        elog(RLOG_WARNING, "receivewal need %s config item", CFG_KEY_PRIMARY_CONN_INFO);
        return false;
    }

    /* set walctrl information */
    translog_walcontrol_setstartpos(&walctrl, recvwal->startpos);
    translog_walcontrol_settli(&walctrl, recvwal->tli);
    translog_walcontrol_setdbtli(&walctrl, recvwal->dbtli);
    translog_walcontrol_setsegsize(&walctrl, recvwal->segsize);
    translog_walcontrol_setslotname(&walctrl, recvwal->slotname);
    translog_walcontrol_setrestorecmd(&walctrl, recvwal->restorecmd);
    walctrl.stat = TRANSLOG_WALCONTROL_STAT_INIT;

    jobstat = TRANSLOG_RECVLOG_STAT_CONN;
    while (1)
    {
        if (true == g_gotsigterm)
        {
            /* if in database connection state, need to send 'c' message */
            if (NULL == conn || CONNECTION_OK != PQstatus(conn) || true == endcmd)
            {
                break;
            }

            if (0 == recvwal->senddone)
            {
                if (false == translog_walmsg_senddone(conn))
                {
                    elog(RLOG_WARNING, "send replica done error");
                    break;
                }
                recvwal->senddone = 1;
            }
        }

        if (TRANSLOG_RECVLOG_STAT_CONN == jobstat)
        {
            /* connect to database using streaming replication mode */
            /*
             * reset flag
             */
            endcmd = false;
            recvwal->senddone = 0;
            /* close descriptor, re-fetch data from file header each time */
            if (-1 != recvwal->fd)
            {
                osal_file_close(recvwal->fd);
                recvwal->fd = -1;
            }
            conn = conn_getphysical(conninfo, "receivewal");
            if (NULL == conn)
            {
                elog(RLOG_WARNING, "connect database server error");
                sleep(1);
                continue;
            }

            /* get database version */
            if (false == translog_recvlog_getdbversion(recvwal->dbtype, conn, &recvwal->dbversion))
            {
                elog(RLOG_WARNING, "get dbversion error");
                goto translog_recvwallog_done;
            }

            /* check if FDE encryption is enabled */
            if (false ==
                translog_recvlog_getconfigurefde(recvwal->dbtype, conninfo, &recvwal->enablefde))
            {
                elog(RLOG_WARNING, "get configure enable-fde error");
                goto translog_recvwallog_done;
            }

            /* get handler based on database version */
            method = translog_recvlog_getroutine(recvwal->dbversion);
            if (NULL == method)
            {
                elog(RLOG_WARNING, "set routine by version error");
                goto translog_recvwallog_done;
            }

            if (0 == recvwal->segsize)
            {
                /* get transaction log size */
                if (false == translog_recvlog_showwalsegsize(recvwal, conn, &walctrl))
                {
                    elog(RLOG_WARNING, "got walsize error");
                    conn_close(conn);
                    conn = NULL;
                    sleep(1);
                    continue;
                }
            }

            /* move to next stage */
            jobstat = TRANSLOG_RECVLOG_STAT_IDENTIFY_SYSTEM;
            continue;
        }
        else if (TRANSLOG_RECVLOG_STAT_IDENTIFY_SYSTEM == jobstat)
        {
            /*
             * after getting timeline and lsn from database, check if startpos and timeline need
             * update
             */
            if (false == translog_recvlog_identifysystem(recvwal, conn, &walctrl))
            {
                jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                conn_close(conn);
                conn = NULL;
                sleep(1);
                continue;
            }

            jobstat = TRANSLOG_RECVLOG_STAT_TIMELINE;
            continue;
        }
        else if (TRANSLOG_RECVLOG_STAT_TIMELINE == jobstat)
        {
            /* supplement timeline logic */
            if (false == translog_recvlog_timelinehistory(recvwal, conn))
            {
                jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                conn_close(conn);
                conn = NULL;
                sleep(1);
                continue;
            }
            jobstat = TRANSLOG_RECVLOG_STAT_STARTREPLICATION;
            continue;
        }
        else if (TRANSLOG_RECVLOG_STAT_STARTREPLICATION == jobstat)
        {
            elog(RLOG_INFO, "start replication at timeline:%u, pos:%08X/%08X,", recvwal->tli,
                 (uint32)(recvwal->startpos >> 32), (uint32)recvwal->startpos);

            /* execute start replication */
            if (false == translog_recvlog_startreplication(recvwal, conn))
            {
                jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                conn_close(conn);
                conn = NULL;
                sleep(1);
                continue;
            }

            /* flush control file */
            walctrl.stat = TRANSLOG_WALCONTROL_STAT_WORK;
            translog_walcontrol_flush(&walctrl, recvwal->data);

            jobstat = TRANSLOG_RECVLOG_STAT_STREAMING;
            continue;
        }
        else if (TRANSLOG_RECVLOG_STAT_STREAMING == jobstat)
        {
            /*
             * 1. when server actively disconnects, it sends 'c' message, then need to respond with
             * 'c' message
             * 2. after sending 'c' message, need to determine if server sent 'T' or 'C'
             *      'T'         last timeline data sent, need to use new timeline to send
             *      'C'         exit, two choices: continue waiting or exit
             */
            if (false == translog_walmsg_getdata(conn, &buffer, &error, &recvlen))
            {
                if (ERROR_REPLICATION == error)
                {
                    jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                    conn_close(conn);
                    conn = NULL;
                    if (NULL != buffer)
                    {
                        PQfreemem(buffer);
                        buffer = NULL;
                    }
                    sleep(1);
                    continue;
                }
            }

            if (ERROR_SUCCESS == error)
            {
                /* process message */
                if (false == method->msgop(recvwal, conn, buffer, recvlen))
                {
                    /* reconnect */
                    jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                    conn_close(conn);
                    conn = NULL;
                    if (NULL != buffer)
                    {
                        PQfreemem(buffer);
                        buffer = NULL;
                    }
                    sleep(1);
                    continue;
                }

                if (0 == PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize) &&
                    recvwal->startpos != walctrl.startpos)
                {
                    walctrl.startpos = recvwal->startpos;

                    /* update control file */
                    translog_walcontrol_flush(&walctrl, recvwal->data);
                }
            }
            else if (ERROR_RETRY == error)
            {
                continue;
            }
            else if (ERROR_ENDREPLICATION == error)
            {
                /* received 'c' command from database */
                if (true == method->endreplication(recvwal, &walctrl, conn, &endcmd, &dberror))
                {
                    if (false == endcmd)
                    {
                        /* not ended, there are still messages to process */
                        continue;
                    }
                }
                else
                {
                    /* error occurred, reset start position */
                    if (ERROR_FILEREMOVED == dberror)
                    {
                        /* execute restore command to try get from archive */
                        if (false == translog_recvlog_execrestorecmd(recvwal))
                        {
                            elog(RLOG_WARNING,
                                 "exec restore command error, stop replication at timeline:%u, "
                                 "pos:%08X/%08X",
                                 recvwal->tli, (uint32)(recvwal->startpos >> 32),
                                 (uint32)recvwal->startpos);
                            goto translog_recvwallog_done;
                        }

                        recvwal->startpos -=
                            PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize);
                        recvwal->startpos += recvwal->segsize;
                        walctrl.startpos = recvwal->startpos;
                        dberror = 0;
                    }
                    else
                    {
                        sleep(1);
                    }
                }

                /*
                 * 1. received end command flag
                 * 2. process error
                 *
                 * do the following:
                 *  1. open file needed
                 *  2. close connection
                 *  3. free memory
                 *  4. set state to connect database
                 */
                if (-1 != recvwal->fd)
                {
                    osal_file_close(recvwal->fd);
                    recvwal->fd = -1;
                }

                conn_close(conn);
                conn = NULL;
                if (NULL != buffer)
                {
                    PQfreemem(buffer);
                    buffer = NULL;
                }
                jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                continue;
            }
        }
    }

translog_recvwallog_done:
    if (NULL != buffer)
    {
        PQfreemem(buffer);
        buffer = NULL;
    }

    if (NULL != conn)
    {
        conn_close(conn);
        conn = NULL;
    }

    return true;
}

/* free resources */
void translog_recvlog_free(translog_recvlog* recvwal)
{
    if (NULL == recvwal)
    {
        return;
    }

    if (NULL != recvwal->data)
    {
        rfree(recvwal->data);
        recvwal->data = NULL;
    }

    if (NULL != recvwal->sysidentifier)
    {
        rfree(recvwal->sysidentifier);
        recvwal->sysidentifier = NULL;
    }

    if (NULL != recvwal->slotname)
    {
        rfree(recvwal->slotname);
        recvwal->slotname = NULL;
    }

    if (NULL != recvwal->restorecmd)
    {
        rfree(recvwal->restorecmd);
        recvwal->restorecmd = NULL;
    }

    rfree(recvwal);
}
