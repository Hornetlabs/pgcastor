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
    TRANSLOG_RECVLOG_STAT_NOP                    = 0x00,

    /* 连接数据库 */
    TRANSLOG_RECVLOG_STAT_CONN                   ,

    /* 获取数据库时间线和pos */
    TRANSLOG_RECVLOG_STAT_IDENTIFY_SYSTEM        ,

    /* 读取时间线 */
    TRANSLOG_RECVLOG_STAT_TIMELINE               ,

    /* 启动流复制 */
    TRANSLOG_RECVLOG_STAT_STARTREPLICATION            ,

    /* 流复制 */
    TRANSLOG_RECVLOG_STAT_STREAMING              ,

} translog_recvlog_stat;

/* 初始化结构 */
translog_recvlog* translog_recvlog_init(void)
{
    translog_recvlog* recvwal = NULL;

    recvwal = rmalloc0(sizeof(translog_recvlog));
    if(NULL == recvwal)
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

/* 数据库 timeline */
void translog_recvlog_setdbtli(translog_recvlog* recvwal, TimeLineID tli)
{
    recvwal->dbtli = tli;
}

/* 设置 startpos */
void translog_recvlog_setstartpos(translog_recvlog* recvwal, XLogRecPtr lsn)
{
    recvwal->startpos = lsn;
}

/* 设置 segsize */
void translog_recvlog_setsegsize(translog_recvlog* recvwal, uint32 segsize)
{
    recvwal->segsize = segsize;
}

/* 设置 dbtype */
void translog_recvlog_setdbtype(translog_recvlog* recvwal, translog_recvlog_dbtype dbtype)
{
    recvwal->dbtype = dbtype;
}

/* 设置 data 目录 */
bool translog_recvlog_setdata(translog_recvlog* recvwal, char* data)
{
    int dlen = 0;

    dlen = strlen(data);
    dlen += 1;
    recvwal->data = rmalloc0(dlen);
    if(NULL == recvwal->data)
    {
        elog(RLOG_WARNING, "set data error");
        return false;
    }
    rmemset0(recvwal->data, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->data, 0, data, dlen);
    return true;
}

/* 设置 slotname 目录 */
bool translog_recvlog_setslotname(translog_recvlog* recvwal, char* slotname)
{
    int dlen = 0;

    dlen = strlen(slotname);
    dlen += 1;
    recvwal->slotname = rmalloc0(dlen);
    if(NULL == recvwal->slotname)
    {
        elog(RLOG_WARNING, "set slotname error");
        return false;
    }
    rmemset0(recvwal->slotname, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->slotname, 0, slotname, dlen);
    return true;
}

/* 设置 restorcommand 目录 */
bool translog_recvlog_setrestorecmd(translog_recvlog* recvwal, char* restorecmd)
{
    bool charf = false;
    bool charp = false;
    int dlen = 0;
    int index = 0;

    if(NULL == restorecmd || '\0' == restorecmd[0])
    {
        return true;
    }

    dlen = strlen(restorecmd);
    dlen += 1;
    recvwal->restorecmd = rmalloc0(dlen);
    if(NULL == recvwal->restorecmd)
    {
        elog(RLOG_WARNING, "set restorecmd error");
        return false;
    }
    rmemset0(recvwal->restorecmd, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->restorecmd, 0, restorecmd, dlen);

    /* 替换命令中的 %f 和 %p 为 %s */
    for (index = 0; index < dlen; index++)
    {
        if ('%' != recvwal->restorecmd[index])
        {
            continue;
        }

        index++;
        if (index >= dlen)
        {
            elog(RLOG_WARNING, "config item restore_command:%s is incorrectly configured.", restorecmd);
            return false;
        }

        if ('f' != recvwal->restorecmd[index] && 'p' != recvwal->restorecmd[index])
        {
            elog(RLOG_WARNING, "config item restore_command:%s is incorrectly configured, only support \%f or \%p", restorecmd);
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

    if(false == charf || false == charp)
    {
        elog(RLOG_WARNING, "config item restore_command:%s must include \%f \%p", restorecmd);
        return false;
    }
    return true;
}

/* 设置 sysidentifier */
bool translog_recvlog_setsysidentifier(translog_recvlog* recvwal, char* sysidentifier)
{
    int dlen = 0;

    dlen = strlen(sysidentifier);
    dlen += 1;
    recvwal->sysidentifier = rmalloc0(dlen);
    if(NULL == recvwal->sysidentifier)
    {
        elog(RLOG_WARNING, "set sysidentifier error");
        return false;
    }
    rmemset0(recvwal->sysidentifier, 0, '\0', dlen);
    dlen -= 1;

    rmemcpy0(recvwal->sysidentifier, 0, sysidentifier, dlen);
    return true;
}

/*------------------------------------执行语句操作 begin-------------------------*/

/*
 * 执行 SHOW wal_segment_size 获取事务日志大小
*/
static bool translog_recvlog_showwalsegsize(translog_recvlog* recvwal, PGconn*conn, translog_walcontrol* walctrl)
{
    uint32 segsize = 0;

    if(false == databaserecv_showwalsegmentsize(conn, &segsize))
    {
        elog(RLOG_WARNING, "SHOW wal_segment_size error");
        return false;
    }

    translog_recvlog_setsegsize(recvwal, segsize);
    translog_walcontrol_setsegsize(walctrl, segsize);
    return true;
}

/*
 * 执行 identify system
 *  1、更新 timeline 和 startpos
 *  2、设置 control 文件的状态为 work
 *  3、将 control 文件落盘
*/
static bool translog_recvlog_identifysystem(translog_recvlog* recvwal, PGconn* conn, translog_walcontrol* walctrl)
{
    TimeLineID dbtli                    = InvalidTimeLineID;
    XLogRecPtr dblsn                    = InvalidXLogRecPtr;

    /* 执行 identify system 命令, 获取 startpos 和 数据库时间线 */
    if (false == databaserecv_identifysystem(conn, &dbtli, &dblsn))
    {
        elog(RLOG_WARNING, "identify system error");
        return false;
    }

    /* 计算 startpos 和 segno */
    if (InvalidXLogRecPtr == recvwal->startpos)
    {
        recvwal->startpos = dblsn;
    }

    /* 在文件头开始 */
    recvwal->startpos -= PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize);
    translog_walcontrol_setstartpos(walctrl, recvwal->startpos);
    recvwal->segno = PGWALBYTETOSEG(recvwal->startpos, recvwal->segsize);

    if (recvwal->dbtli > dbtli)
    {
        elog(RLOG_WARNING, "recvwal's database timeline %u large than database timeline %u", recvwal->dbtli, dbtli);
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
 * 获取时间线文件
 *  1、当时间线为 1 时, 不需要处理
 *  2、获取时间线文件
 *      2.1 本地存在则不获取
 *      2.2 本地不存在时执行命令获取
*/
static bool translog_recvlog_timelinehistory(translog_recvlog* recvwal, PGconn* conn)
{
    char* filename = NULL;
    char* content = NULL;
    if(1 == recvwal->tli)
    {
        return true;
    }

    /* 查看文件是否存在 */
    if (true == translog_waltimeline_exist(recvwal->data, recvwal->tli))
    {
        return true;
    }

    /* 获取时间线 */
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

    /* 写时间线文件 */
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
 * 启动流复制
*/
static bool translog_recvlog_startreplication(translog_recvlog* recvwal, PGconn*conn)
{
    if(false == databaserecv_startreplication(conn, recvwal->tli, recvwal->startpos, recvwal->slotname))
    {
        elog(RLOG_WARNING, "start replication error");
        return false;
    }
    return true;
}


/*------------------------------------执行语句操作   end-------------------------*/

/* 在归档文件中获取数据 */
static bool translog_recvlog_execrestorecmd(translog_recvlog* recvwal)
{
    /*
     * 1、组装 restorecommand 
     * 2、执行 restorecommand
     * 3、获取执行结果
     */
    int ret                             = 0;
    int index                           = 0;
    int cindex                          = 0;
    int clen                            = 0;
    char* cptr                          = NULL;
    FILE* fp                            = NULL;
    char walfile[NAMEDATALEN]    = { 0 };
    char resultfile[NAMEDATALEN] = { 0 };
    char command[COMMANDSIZE]    = { 0 };
    char fileline[LINESIZE]      = { 0 };

    if (NULL == recvwal->restorecmd || '\0' == recvwal->restorecmd[0])
    {
        elog(RLOG_WARNING, "unconfig restore_command item");
        return false;
    }

    /* 文件名 */
    snprintf(walfile,
             NAMEDATALEN,
             "%08X%08X%08X",                         /* 目录/timeline segno size */
             recvwal->tli,
             (uint32)(( recvwal->segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)(( recvwal->segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    /* 命令 */
    snprintf(resultfile, NAMEDATALEN, "%s", "rcmd.result");

    /* 替换 %f 和 %p */
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
            snprintf(cptr + cindex,
                     COMMANDSIZE - cindex,
                     "%s/%s",
                     recvwal->data, walfile);
            cindex = strlen(cptr);
        }
    }

    /* 添加输出文件 */
    snprintf(cptr + cindex, COMMANDSIZE - cindex, " >%s 2>&1", resultfile);

translog_recvwal_execrestorecmd_retry:
    /* 执行命令 */
    ret = system(command);
    if (0 == ret)
    {
        return true;
    }

    /* 没有执行成功, 那么查看 rcmd.result 文件 */
    fp = osal_file_fopen(resultfile, "r");
    if (NULL == fp)
    {
        elog(RLOG_WARNING, "open file %s error, %s", resultfile, strerror(errno));
        return false;
    }

    while(NULL != fgets(fileline, LINESIZE, fp))
    {
        break;
    }
    osal_free_file(fp);
    fp = NULL;

    if('\0' == fileline)
    {
        elog(RLOG_WARNING, "can not got %s result", command);
        return false;
    }

    if(NULL != strstr(fileline, "Connection timed out"))
    {
        /* 超时 */
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

/* 流复制获取日志 */
bool translog_recvlog_main(translog_recvlog* recvwal)
{
    /*
     * 1、连接数据库
     * 2、获取 segsize
     * 3、执行 identity system
     *      根据执行结果更新 control 文件
     * 4、获取时间线文件
     * 5、启动流复制
     */
    translog_recvlog_stat jobstat        = TRANSLOG_RECVLOG_STAT_NOP;
    /* 用于标识接收到源端发送的接收 end command 的标识 */
    bool endcmd                                 = false;
    int error                                   = 0;
    int dberror                                 = 0;
    int recvlen                                 = 0;
    char* buffer                                = NULL;
    char* conninfo                              = NULL;
    PGconn* conn                                = NULL;
    translog_recvlog_amroutine* method   = NULL;
    translog_walcontrol walctrl          = { 0 };

    conninfo = guc_getConfigOption(CFG_KEY_PRIMARY_CONN_INFO);
    if(NULL == conninfo || '\0' == conninfo[0])
    {
        elog(RLOG_WARNING, "receivewal need %s config item", CFG_KEY_PRIMARY_CONN_INFO);
        return false;
    }

    /* 设置 walctrl 内的信息 */
    translog_walcontrol_setstartpos(&walctrl, recvwal->startpos);
    translog_walcontrol_settli(&walctrl, recvwal->tli);
    translog_walcontrol_setdbtli(&walctrl, recvwal->dbtli);
    translog_walcontrol_setsegsize(&walctrl, recvwal->segsize);
    translog_walcontrol_setslotname(&walctrl, recvwal->slotname);
    translog_walcontrol_setrestorecmd(&walctrl, recvwal->restorecmd);
    walctrl.stat = TRANSLOG_WALCONTROL_STAT_INIT;

    jobstat = TRANSLOG_RECVLOG_STAT_CONN;
    while(1)
    {
        if(true == g_gotsigterm)
        {
            /* 若为连接数据库的状态, 那么需要发送 ‘c' 消息 */
            if(NULL == conn || CONNECTION_OK != PQstatus(conn) || true == endcmd)
            {
                break;
            }

            if(0 == recvwal->senddone)
            {
                if(false == translog_walmsg_senddone(conn))
                {
                    elog(RLOG_WARNING, "send replica done error");
                    break;
                }
                recvwal->senddone = 1;
            }
        }

        if (TRANSLOG_RECVLOG_STAT_CONN == jobstat)
        {
            /* 使用流复制模式连接数据库 */
            /* 
             * 重置标识
             */
            endcmd = false;
            recvwal->senddone = 0;
            /* 关闭描述符, 每次都在文件头重新获取数据 */
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

            /* 获取数据库版本 */
            if (false == translog_recvlog_getdbversion(recvwal->dbtype, conn, &recvwal->dbversion))
            {
                elog(RLOG_WARNING, "get dbversion error");
                goto translog_recvwallog_done;
            }

            /* 查看是否开启 FDE 加密 */
            if (false == translog_recvlog_getconfigurefde(recvwal->dbtype, conninfo, &recvwal->enablefde))
            {
                elog(RLOG_WARNING, "get configure enable-fde error");
                goto translog_recvwallog_done;
            }

            /* 根据数据库版本获取处理器 */
            method = translog_recvlog_getroutine(recvwal->dbversion);
            if (NULL == method)
            {
                elog(RLOG_WARNING, "set routine by version error");
                goto translog_recvwallog_done;
            }

            if (0 == recvwal->segsize)
            {
                /* 在获取中获取事务日志大小 */
                if (false == translog_recvlog_showwalsegsize(recvwal, conn, &walctrl))
                {
                    elog(RLOG_WARNING, "got walsize error");
                    conn_close(conn);
                    conn = NULL;
                    sleep(1);
                    continue;
                }
            }

            /* 转入下阶段 */
            jobstat = TRANSLOG_RECVLOG_STAT_IDENTIFY_SYSTEM;
            continue;
        }
        else if(TRANSLOG_RECVLOG_STAT_IDENTIFY_SYSTEM == jobstat)
        {
            /*
             * 在数据库中获取到时间线和 lsn 后, 查看是否需要更新 startpos 和 timeline
             */
            if(false == translog_recvlog_identifysystem(recvwal, conn, &walctrl))
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
            /* 补充时间线逻辑 */
            if(false == translog_recvlog_timelinehistory(recvwal, conn))
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
            elog(RLOG_INFO,
                 "start replication at timeline:%u, pos:%08X/%08X,",
                 recvwal->tli,
                 (uint32)(recvwal->startpos>>32),
                 (uint32)recvwal->startpos);

            /* 执行 start replication */
            if(false == translog_recvlog_startreplication(recvwal, conn))
            {
                jobstat = TRANSLOG_RECVLOG_STAT_CONN;
                conn_close(conn);
                conn = NULL;
                sleep(1);
                continue;
            }

            /* 落盘 control 文件 */
            walctrl.stat = TRANSLOG_WALCONTROL_STAT_WORK;
            translog_walcontrol_flush(&walctrl, recvwal->data);

            jobstat = TRANSLOG_RECVLOG_STAT_STREAMING;
            continue;
        }
        else if(TRANSLOG_RECVLOG_STAT_STREAMING == jobstat)
        {
            /*
             * 1、当服务端主动断开连接时, 那么会发送 'c' 消息, 那么此时需要反馈 'c' 消息
             * 2、发送完 'c' 消息后需要判断服务端发送的消息为: 'T' 还是 'C'
             *      'T'         上个时间线的数据发送完了, 此时需要使用新的时间线发送
             *      'C'         退出, 有两个选择, 继续等待或退出
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
                /* 处理消息 */
                if (false == method->msgop(recvwal, conn, buffer, recvlen))
                {
                    /* 再次连接 */
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

                if (0 == PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize) && recvwal->startpos != walctrl.startpos)
                {
                    walctrl.startpos = recvwal->startpos;

                    /* 更新 control 文件 */
                    translog_walcontrol_flush(&walctrl, recvwal->data);
                }
            }
            else if (ERROR_RETRY == error)
            {
                continue;
            }
            else if (ERROR_ENDREPLICATION == error)
            {
                /* 接收到了数据库发送的 'c' 命令 */
                if(true == method->endreplication(recvwal, &walctrl, conn, &endcmd, &dberror))
                {
                    if (false == endcmd)
                    {
                        /* 没有结束, 那么还有消息需要处理 */
                        continue;
                    }
                }
                else
                {
                    /* 出错了, 那么重新设置起点 */
                    if(ERROR_FILEREMOVED == dberror)
                    {
                        /* 执行 restore command 在归档中尝试获取 */
                        if(false == translog_recvlog_execrestorecmd(recvwal))
                        {
                            elog(RLOG_WARNING,
                                 "exec restore command error, stop replication at timeline:%u, pos:%08X/%08X",
                                 recvwal->tli,
                                 (uint32)(recvwal->startpos>>32),
                                 (uint32)recvwal->startpos);
                            goto translog_recvwallog_done;
                        }

                        recvwal->startpos -= PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize);
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
                 * 1、接收到了 end command 标识
                 * 2、流程处理错误
                 * 
                 * 那么做如下处理:
                 *  1、需要打开的文件
                 *  2、关闭连接
                 *  3、释放内存
                 *  4、设置状态为连接数据库
                 */
                if(-1 != recvwal->fd)
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

    if(NULL != conn)
    {
        conn_close(conn);
        conn = NULL;
    }

    return true;
}

/* 释放 */
void translog_recvlog_free(translog_recvlog* recvwal)
{
    if(NULL == recvwal)
    {
        return;
    }

    if(NULL != recvwal->data)
    {
        rfree(recvwal->data);
        recvwal->data = NULL;
    }

    if(NULL != recvwal->sysidentifier)
    {
        rfree(recvwal->sysidentifier);
        recvwal->sysidentifier = NULL;
    }

    if(NULL != recvwal->slotname)
    {
        rfree(recvwal->slotname);
        recvwal->slotname = NULL;
    }

    if(NULL != recvwal->restorecmd)
    {
        rfree(recvwal->restorecmd);
        recvwal->restorecmd = NULL;
    }

    rfree(recvwal);
}
