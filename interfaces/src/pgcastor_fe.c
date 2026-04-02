#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <unistd.h>
#include <memory.h>
#include <errno.h>

#include "app_c.h"
#include "config.h"
#include "pgcastor_exbufferdata.h"
#include "pgcastor_fe.h"
#include "pgcastor_int.h"
#include "pgcastor_fenet.h"
#include "pgcastor_febuildmsg.h"
#include "pgcastor_feparsemsg.h"

typedef void (*commandfuncfree)(pgcastor_cmd* cmd);

typedef struct PGCASTOR_COMMANDOPS
{
    pgcastor_cmdtag   type;
    char*           desc;
    commandfuncfree free;
} pgcastor_commandops;

static bool PGCastorParseoptions(pgcastorconn* conn, char* connstr)
{
    bool  foundsplit = false;
    int   index = 0;
    int   connstrlen = 0;
    char* key = NULL;
    char* value = NULL;

    connstrlen = strlen(connstr);

    while (index < connstrlen)
    {
        while (' ' == connstr[index] || '\t' == connstr[index] || '\r' == connstr[index] || '\n' == connstr[index])
        {
            index++;
            continue;
        }

        /* parse key */
        foundsplit = false;
        key = connstr + index;
        while (index < connstrlen)
        {
            if (' ' != connstr[index] && '=' != connstr[index])
            {
                if (true == foundsplit)
                {
                    return false;
                }
                index++;
                continue;
            }

            foundsplit = true;
            if (' ' == connstr[index])
            {
                connstr[index] = '\0';
                index++;
                continue;
            }
            else if ('=' == connstr[index])
            {
                connstr[index] = '\0';
                index++;
                break;
            }
            break;
        }

        /* get value */
        while (index < connstrlen)
        {
            while (' ' == connstr[index] || '\t' == connstr[index] || '\r' == connstr[index] || '\n' == connstr[index])
            {
                index++;
                continue;
            }
            break;
        }

        /* parse value */
        value = connstr + index;
        while (index < connstrlen)
        {
            if (' ' != connstr[index])
            {
                index++;
                continue;
            }

            connstr[index] = '\0';
            index++;
            break;
        }

        /* check if key matches rule */
        if (0 == strcmp("host", key))
        {
            memcpy(conn->host, value, strlen(value));
        }
        else if (0 == strcmp("port", key))
        {
            memcpy(conn->port, value, strlen(value));
        }
        else if (0 == strcmp("protocol", key))
        {
            if (0 == strcmp("TCP", value))
            {
                conn->socktype = PGCASTOR_SOCKTYPE_TCP;
            }
            else if (0 == strcmp("UNIXDOMAIN", value))
            {
                conn->socktype = PGCASTOR_SOCKTYPE_UNIXDOMAIN;
            }
            else
            {
                return false;
            }
        }
        else if (0 == strcmp("keepalive", key))
        {
            conn->keepalive = atoi(value);
        }
        else if (0 == strcmp("keepaliveidle", key))
        {
            conn->keepaliveidle = atoi(value);
        }
        else if (0 == strcmp("keepaliveinterval", key))
        {
            conn->keepaliveinterval = atoi(value);
        }
        else if (0 == strcmp("keepalivecount", key))
        {
            conn->keepalivecount = atoi(value);
        }
        else if (0 == strcmp("usertimeout", key))
        {
            conn->usertimeout = atoi(value);
        }
        else
        {
            return false;
        }
    }

    return true;
}

/* initialize pgcastor_result */
void* PGCastorResultInit(void)
{
    pgcastor_result* result = NULL;

    result = (pgcastor_result*)malloc(sizeof(pgcastor_result));
    if (NULL == result)
    {
        return NULL;
    }
    memset(result, 0, sizeof(pgcastor_result));
    result->rowcnt = 0;
    result->rows = NULL;

    return result;
}

/* reset pgcastor_result content */
void PGCastorResultReset(void* in_result)
{
    pgcastor_result* result = NULL;

    result = (pgcastor_result*)in_result;

    if (NULL == result)
    {
        return;
    }

    if (NULL != result->rows)
    {
        PGCastorRowFree(result->rowcnt, result->rows);
    }

    result->rowcnt = 0;
    result->rows = NULL;

    return;
}

/* initialize row */
pgcastorrow* PGCastorRowInit(int rowcnt)
{
    pgcastorrow* row = NULL;

    row = (pgcastorrow*)malloc(sizeof(pgcastorrow) * rowcnt);
    if (NULL == row)
    {
        return NULL;
    }
    memset(row, 0, sizeof(pgcastorrow) * rowcnt);
    row->columncnt = 0;
    row->columns = NULL;

    return row;
}

/* initialize pgcastorpair */
pgcastorpair* PGCastorPairInit(int colcnt)
{
    pgcastorpair* col = NULL;

    col = (pgcastorpair*)malloc(sizeof(pgcastorpair) * colcnt);
    if (NULL == col)
    {
        return NULL;
    }
    memset(col, 0, sizeof(pgcastorpair) * colcnt);

    col->key = NULL;
    col->keylen = 0;
    col->value = NULL;
    col->valuelen = 0;
    return col;
}

/* free pair resources */
void PGCastorPairFree(int colcnt, void* in_col)
{
    int         idx_col = 0;
    pgcastorpair* col = NULL;
    pgcastorpair* tmpcol = NULL;

    col = (pgcastorpair*)in_col;

    if (0 == colcnt || NULL == col)
    {
        return;
    }

    for (idx_col = 0; idx_col < colcnt; idx_col++)
    {
        tmpcol = &col[idx_col];
        if (tmpcol->key)
        {
            free(tmpcol->key);
        }

        if (tmpcol->value)
        {
            free(tmpcol->value);
        }
    }
    free(col);

    return;
}

/* free rows resources */
void PGCastorRowFree(int rowcnt, void* in_rows)
{
    int        idx_row = 0;
    pgcastorrow* rows = NULL;
    pgcastorrow* tmprow = NULL;
    rows = (pgcastorrow*)in_rows;

    if (0 == rowcnt || NULL == rows)
    {
        return;
    }

    for (idx_row = 0; idx_row < rowcnt; idx_row++)
    {
        tmprow = &rows[idx_row];
        PGCastorPairFree(tmprow->columncnt, tmprow->columns);
    }
    free(rows);

    return;
}

/* set connection parameters */
pgcastorconn* PGCastorSetParam(char* connstr)
{
    /*
     * 1. parse connstr and write to pgcastorconn structure
     * 2. check if port is valid, if not set port to RMANAGER_PORT from config.h
     * 3. check if host is valid, if not set host to RMANAGER_UNIXDOMAINPREFIX from config.h
     * concatenated with port 4. execute connection 5. check if connected 6. tcpkeepalive settings
     * etc 7. send identity message
     * 8. receive identity message
     */
    pgcastorconn* xconn = NULL;

    xconn = malloc(sizeof(pgcastorconn));
    if (NULL == xconn)
    {
        return NULL;
    }
    memset(xconn, '\0', sizeof(pgcastorconn));

    if (NULL == connstr)
    {
        snprintf(xconn->host, 512, "%s/%s.%s", RMANAGER_UNIXDOMAINDIR, RMANAGER_UNIXDOMAINPREFIX, RMANAGER_PORT);
        snprintf(xconn->port, 128, "%s", RMANAGER_PORT);
        xconn->keepalive = 0;
        xconn->socktype = PGCASTOR_SOCKTYPE_UNIXDOMAIN;
    }
    else
    {
        if (false == PGCastorParseoptions(xconn, connstr))
        {
            free(xconn);
            return NULL;
        }

        if (PGCASTOR_SOCKTYPE_NOP == xconn->socktype)
        {
            return NULL;
        }
    }

    xconn->sendmsg = pgcastor_exbufferdata_init();
    if (NULL == xconn->sendmsg)
    {
        free(xconn);
        return NULL;
    }

    xconn->recvmsg = pgcastor_exbufferdata_init();
    if (NULL == xconn->recvmsg)
    {
        pgcastor_exbufferdata_free(xconn->sendmsg);
        free(xconn);
        return NULL;
    }

    xconn->errmsg = pgcastor_exbufferdata_init();
    if (NULL == xconn->errmsg)
    {
        pgcastor_exbufferdata_free(xconn->sendmsg);
        pgcastor_exbufferdata_free(xconn->recvmsg);
        free(xconn);
        return NULL;
    }

    xconn->result = (pgcastor_result*)PGCastorResultInit();
    if (NULL == xconn->result)
    {
        pgcastor_exbufferdata_free(xconn->sendmsg);
        pgcastor_exbufferdata_free(xconn->recvmsg);
        pgcastor_exbufferdata_free(xconn->errmsg);
        free(xconn);
        return NULL;
    }

    /* connect to xmanager */
    if (false == pgcastor_fenet_conn(xconn))
    {
        return xconn;
    }
    return xconn;
}

/* connect */
bool PGCastorConn(pgcastorconn* conn)
{
    int                rownumber = 0;
    pgcastorrow*         rows = NULL;
    pgcastor_identitycmd icmd = {{0}};

    /* check if connected */
    if (false == pgcastor_fenet_isconn(conn))
    {
        /* connect to xmanager */
        if (false == pgcastor_fenet_conn(conn))
        {
            return false;
        }
    }

    /* build command */
    icmd.type.type = T_PGCASTOR_IDENTITYCMD;
    icmd.jobname = "xscsci";
    icmd.user = NULL;
    icmd.passwd = NULL;
    icmd.kind = PGCASTOR_JOBKIND_XSCSCI;

    if (false == pgcastor_febuildmsg_cmd2msg(&icmd.type, conn->sendmsg))
    {
        return false;
    }

    /* send cmd */
    if (false == pgcastor_fenet_sendcmd(conn))
    {
        return false;
    }

    PGCastorGetResult(conn, &rownumber, &rows);

    /* parse data */
    return true;
}

/* send command */
bool PGCastorSendCmd(pgcastorconn* conn, pgcastor_cmd* cmd)
{
    /*
     * 1. check if connection is valid, return error if invalid
     * 2. assemble data stream based on cmd
     *      pgcastor_febuildmsg_cmd2msg
     *
     * 3. send data to xmanager
     * 4. get xmanager return result
     */

    /* check if connected */
    if (false == pgcastor_fenet_isconn(conn))
    {
        /* connect to xmanager */
        if (false == PGCastorConn(conn))
        {
            return false;
        }
    }

    /* assemble data stream based on cmd */
    if (false == pgcastor_febuildmsg_cmd2msg(cmd, conn->sendmsg))
    {
        return false;
    }

    /* send cmd */
    if (false == pgcastor_fenet_sendcmd(conn))
    {
        return false;
    }
    return true;
}

/* test if xmanager is started */
bool PGCastorPing(pgcastorconn* conn)
{
    /* check if connected */
    if (false == pgcastor_fenet_isconn(conn))
    {
        /* connect to xmanager */
        if (false == PGCastorConn(conn))
        {
            return false;
        }
    }

    return true;
}

/* get return result */
void PGCastorGetResult(pgcastorconn* conn, int* rownumber, pgcastorrow** rows)
{
    /*
     * 1. parse data in conn->recvmsg
     *      pgcastor_feparsemsg_msg2result
     * 2. clean data in conn->recvmsg
     * 3. return rownumber and rows
     */

    *rownumber = 0;
    *rows = NULL;

    /* error getting result, print error message and clean data */
    if (false == pgcastor_feparsemsg_msg2result(conn->recvmsg, conn))
    {
        printf("get result failed:%s \n", conn->errmsg->data);
        pgcastor_exbufferdata_reset(conn->recvmsg);
        pgcastor_exbufferdata_reset(conn->errmsg);
        PGCastorResultReset(conn->result);
        return;
    }

    /* set return value and release resources */
    *rownumber = conn->result->rowcnt;
    *rows = conn->result->rows;
    conn->result->rowcnt = 0;
    conn->result->rows = NULL;

    return;
}

/* get error information */
void PGCastorGetErrmsg(pgcastorconn* conn)
{
    /*
     * output error information
     */
    if (NULL == conn || PGCASTORCONN_STATUS_OK != conn->connstatus)
    {
        return;
    }

    if (0 != conn->errcode)
    {
        printf("%s \n", conn->errmsg->data);
    }

    return;
}

/* clean return result */
void PGCastorClear(pgcastorconn* conn)
{
    if (NULL == conn)
    {
        return;
    }

    pgcastor_exbufferdata_reset(conn->sendmsg);
    pgcastor_exbufferdata_reset(conn->recvmsg);
    pgcastor_exbufferdata_reset(conn->errmsg);

    if (conn->result)
    {
        PGCastorResultReset(conn->result);
    }
    return;
}

/* cleanup pgcastorconn */
void PGCastorFinish(pgcastorconn* conn)
{
    if (NULL == conn)
    {
        return;
    }

    pgcastor_exbufferdata_free(conn->sendmsg);
    pgcastor_exbufferdata_free(conn->recvmsg);
    pgcastor_exbufferdata_free(conn->errmsg);

    if (conn->result)
    {
        PGCastorResultReset(conn->result);
        free(conn->result);
    }

    free(conn);

    return;
}

void pgcastor_rangvar_destroy(pgcastor_rangevar* rs)
{
    if (NULL == rs)
    {
        return;
    }

    if (NULL != rs->schema)
    {
        free(rs->schema);
    }

    if (NULL != rs->table)
    {
        free(rs->table);
    }

    free(rs);
    rs = NULL;
}

pgcastor_rangevar* pgcastor_rangvar_init(char* schema, char* table)
{
    pgcastor_rangevar* rs = NULL;

    rs = malloc(sizeof(pgcastor_rangevar));
    if (NULL == rs)
    {
        return NULL;
    }
    memset(rs, 0, sizeof(pgcastor_rangevar));
    rs->schema = schema;
    rs->table = table;
    return rs;
}

void pgcastor_rangvar_free(pgcastor_rangevar* rangevar)
{
    if (NULL == rangevar)
    {
        return;
    }

    if (NULL != rangevar->schema)
    {
        free(rangevar->schema);
    }

    if (NULL != rangevar->table)
    {
        free(rangevar->table);
    }
    free(rangevar);
    return;
}

pgcastor_job* pgcastor_job_init(int jobkind, char* jobname)
{
    pgcastor_job* job = NULL;

    job = malloc(sizeof(pgcastor_job));
    if (NULL == job)
    {
        return NULL;
    }
    memset(job, 0, sizeof(pgcastor_job));
    job->kind = jobkind;
    job->jobname = jobname;
    return job;
}

void pgcastor_job_free(pgcastor_job* job)
{
    if (NULL == job)
    {
        return;
    }

    if (job->jobname)
    {
        free(job->jobname);
    }

    free(job);
    return;
}

/* identity resource release */
static void pgcastor_command_identityfree(pgcastor_cmd* cmd)
{
    pgcastor_identitycmd* identity = NULL;

    if (NULL == cmd)
    {
        return;
    }

    identity = (pgcastor_identitycmd*)cmd;

    if (NULL != identity->jobname)
    {
        free(identity->jobname);
    }

    if (NULL != identity->passwd)
    {
        free(identity->passwd);
    }

    if (NULL != identity->user)
    {
        free(identity->user);
    }

    free(identity);
}

/* create resource release */
static void pgcastor_command_createfree(pgcastor_cmd* cmd)
{
    ListCell*         lc = NULL;
    pgcastor_job*       job = NULL;
    pgcastor_createcmd* create = NULL;

    if (NULL == cmd)
    {
        return;
    }

    create = (pgcastor_createcmd*)cmd;

    if (NULL != create->name)
    {
        free(create->name);
    }

    if (NULL != create->job)
    {
        foreach (lc, create->job)
        {
            job = (pgcastor_job*)lfirst(lc);
            pgcastor_job_free(job);
        }

        list_free(create->job);
    }

    free(create);
}

/* alter resource release */
static void pgcastor_command_alterfree(pgcastor_cmd* cmd)
{
    ListCell*        lc = NULL;
    pgcastor_job*      job = NULL;
    pgcastor_altercmd* alter = NULL;

    if (NULL == cmd)
    {
        return;
    }

    alter = (pgcastor_altercmd*)cmd;

    if (NULL != alter->name)
    {
        free(alter->name);
    }

    if (NULL != alter->job)
    {
        foreach (lc, alter->job)
        {
            job = (pgcastor_job*)lfirst(lc);
            pgcastor_job_free(job);
        }

        list_free(alter->job);
    }

    free(alter);
    return;
}

/* remove resource release */
static void pgcastor_command_removefree(pgcastor_cmd* cmd)
{
    pgcastor_removecmd* remove = NULL;

    if (NULL == cmd)
    {
        return;
    }

    remove = (pgcastor_removecmd*)cmd;

    if (NULL != remove->name)
    {
        free(remove->name);
    }

    free(remove);
    return;
}

/* drop resource release */
static void pgcastor_command_dropfree(pgcastor_cmd* cmd)
{
    pgcastor_dropcmd* drop = NULL;

    if (NULL == cmd)
    {
        return;
    }

    drop = (pgcastor_dropcmd*)cmd;

    if (NULL != drop->name)
    {
        free(drop->name);
    }

    free(drop);
    return;
}

/* init resource release */
static void pgcastor_command_initfree(pgcastor_cmd* cmd)
{
    pgcastor_initcmd* init = NULL;

    if (NULL == cmd)
    {
        return;
    }

    init = (pgcastor_initcmd*)cmd;

    if (NULL != init->name)
    {
        free(init->name);
    }

    free(init);
    return;
}

/* edit resource release */
static void pgcastor_command_editfree(pgcastor_cmd* cmd)
{
    pgcastor_editcmd* edit = NULL;

    if (NULL == cmd)
    {
        return;
    }

    edit = (pgcastor_editcmd*)cmd;

    if (NULL != edit->name)
    {
        free(edit->name);
    }

    free(edit);
    return;
}

/* start resource release */
static void pgcastor_command_startfree(pgcastor_cmd* cmd)
{
    pgcastor_startcmd* start = NULL;

    if (NULL == cmd)
    {
        return;
    }

    start = (pgcastor_startcmd*)cmd;

    if (NULL != start->name)
    {
        free(start->name);
    }

    free(start);
    return;
}

/* stop resource release */
static void pgcastor_command_stopfree(pgcastor_cmd* cmd)
{
    pgcastor_stopcmd* stop = NULL;

    if (NULL == cmd)
    {
        return;
    }

    stop = (pgcastor_stopcmd*)cmd;

    if (NULL != stop->name)
    {
        free(stop->name);
    }

    free(stop);
    return;
}

/* reload resource release */
static void pgcastor_command_reloadfree(pgcastor_cmd* cmd)
{
    pgcastor_reloadcmd* reload = NULL;

    if (NULL == cmd)
    {
        return;
    }

    reload = (pgcastor_reloadcmd*)cmd;

    if (NULL != reload->name)
    {
        free(reload->name);
    }

    free(reload);
    return;
}

/* info resource release */
static void pgcastor_command_infofree(pgcastor_cmd* cmd)
{
    pgcastor_infocmd* info = NULL;

    if (NULL == cmd)
    {
        return;
    }

    info = (pgcastor_infocmd*)cmd;

    if (NULL != info->name)
    {
        free(info->name);
    }

    free(info);
    return;
}

/* watch resource release */
static void pgcastor_command_watchfree(pgcastor_cmd* cmd)
{
    pgcastor_watchcmd* watch = NULL;

    if (NULL == cmd)
    {
        return;
    }

    watch = (pgcastor_watchcmd*)cmd;

    if (NULL != watch->name)
    {
        free(watch->name);
    }

    free(watch);
    return;
}

/* cfgfile resource release */
static void pgcastor_command_cfgfilefree(pgcastor_cmd* cmd)
{
    pgcastor_cfgfilecmd* cfgfile = NULL;

    if (NULL == cmd)
    {
        return;
    }

    cfgfile = (pgcastor_cfgfilecmd*)cmd;

    if (NULL != cfgfile->name)
    {
        free(cfgfile->name);
    }

    if (NULL != cfgfile->filename)
    {
        free(cfgfile->filename);
    }

    if (NULL != cfgfile->data)
    {
        free(cfgfile->data);
    }

    free(cfgfile);
    return;
}

/* refresh resource release */
static void pgcastor_command_refreshfree(pgcastor_cmd* cmd)
{
    ListCell*          lc = NULL;
    pgcastor_rangevar*   rs = NULL;
    pgcastor_refreshcmd* refresh = NULL;

    if (NULL == cmd)
    {
        return;
    }

    refresh = (pgcastor_refreshcmd*)cmd;

    if (NULL != refresh->name)
    {
        free(refresh->name);
    }

    if (NULL != refresh->tables)
    {
        foreach (lc, refresh->tables)
        {
            rs = (pgcastor_rangevar*)lfirst(lc);
            pgcastor_rangvar_free(rs);
        }

        list_free(refresh->tables);
    }

    free(refresh);
    return;
}

/* list resource release */
static void pgcastor_command_listfree(pgcastor_cmd* cmd)
{
    if (NULL == cmd)
    {
        return;
    }

    free(cmd);
    return;
}

static pgcastor_commandops m_commandfree[] = {
    {T_PGCASTOR_NOP,         "NOP",              NULL                       },
    {T_PGCASTOR_IDENTITYCMD, "IDENTITY COMMAND", pgcastor_command_identityfree},
    {T_PGCASTOR_CREATECMD,   "CREATE COMMAND",   pgcastor_command_createfree  },
    {T_PGCASTOR_ALTERCMD,    "ALTER COMMAND",    pgcastor_command_alterfree   },
    {T_PGCASTOR_REMOVECMD,   "REMOVE COMMAND",   pgcastor_command_removefree  },
    {T_PGCASTOR_DROPCMD,     "DROP COMMAND",     pgcastor_command_dropfree    },
    {T_PGCASTOR_INITCMD,     "INIT COMMAND",     pgcastor_command_initfree    },
    {T_PGCASTOR_EDITCMD,     "EDIT COMMAND",     pgcastor_command_editfree    },
    {T_PGCASTOR_STARTCMD,    "START COMMAND",    pgcastor_command_startfree   },
    {T_PGCASTOR_STOPCMD,     "STOP COMMAND",     pgcastor_command_stopfree    },
    {T_PGCASTOR_RELOADCMD,   "RELOAD COMMAND",   pgcastor_command_reloadfree  },
    {T_PGCASTOR_INFOCMD,     "INFO COMMAND",     pgcastor_command_infofree    },
    {T_PGCASTOR_WATCHCMD,    "WATCH COMMAND",    pgcastor_command_watchfree   },
    {T_PGCASTOR_CFGfILECMD,  "never trigger",    pgcastor_command_cfgfilefree },
    {T_PGCASTOR_REFRESHCMD,  "REFRESH COMMAND",  pgcastor_command_refreshfree },
    {T_PGCASTOR_LISTCMD,     "LIST COMMAND",     pgcastor_command_listfree    },
    {T_PGCASTOR_MAX,         "MAX COMMAND",      NULL                       }
};

void pgcastor_command_free(pgcastor_cmd* cmd)
{
    if (T_PGCASTOR_MAX < cmd->type)
    {
        printf("command unknown cmd type %d\n", cmd->type);
        return;
    }

    if (NULL == m_commandfree[cmd->type].free)
    {
        printf("command unsupport %s\n", m_commandfree[cmd->type].desc);
        return;
    }

    return m_commandfree[cmd->type].free(cmd);
}