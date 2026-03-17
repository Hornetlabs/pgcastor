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
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"
#include "xsynch_int.h"
#include "xsynch_fenet.h"
#include "xsynch_febuildmsg.h"
#include "xsynch_feparsemsg.h"

typedef void (*commandfuncfree)(xsynch_cmd* cmd);

typedef struct XSYNCH_COMMANDOPS
{
    xsynch_cmdtag                   type;
    char*                           desc;
    commandfuncfree                 free;
} xsynch_commandops;

static bool XSynchParseoptions(xsynchconn* conn, char* connstr)
{
    bool foundsplit = false;
    int index       = 0;
    int connstrlen  = 0;
    char* key       = NULL;
    char* value     = NULL;

    connstrlen = strlen(connstr);

    while (index < connstrlen)
    {
        while (' ' == connstr[index]
               || '\t' == connstr[index]
               || '\r' == connstr[index]
               || '\n' == connstr[index])
        {
            index++;
            continue;
        }

        /* 拆解 key */
        foundsplit = false;
        key = connstr + index;
        while(index < connstrlen)
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

        /* 获取 value */
        while(index < connstrlen)
        {
            while (' ' == connstr[index]
               || '\t' == connstr[index]
               || '\r' == connstr[index]
               || '\n' == connstr[index])
            {
                index++;
                continue;
            }
            break;
        }

        /* 拆解值 */
        value = connstr + index;
        while(index < connstrlen)
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

        /* 查看 key 是否符合规则 */
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
                conn->socktype = XSYNCH_SOCKTYPE_TCP;
            }
            else if (0 == strcmp("UNIXDOMAIN", value))
            {
                conn->socktype = XSYNCH_SOCKTYPE_UNIXDOMAIN;
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

/* 初始化xsynch_result */
void* XsynchResultInit(void)
{
    xsynch_result* result = NULL;

    result = (xsynch_result*)malloc(sizeof(xsynch_result));
    if (NULL == result)
    {
        return NULL;
    }
    memset(result, 0, sizeof(xsynch_result));
    result->rowcnt = 0;
    result->rows = NULL;

    return result;
}

/* 重置xsynch_result内容 */
void XsynchResultReset(void* in_result)
{
    xsynch_result* result = NULL;

    result = (xsynch_result*)in_result;

    if (NULL == result)
    {
        return;
    }
    

    if (NULL != result->rows)
    {
        XsynchRowFree(result->rowcnt, result->rows);
    }

    result->rowcnt = 0;
    result->rows = NULL;

    return;
}

/* 初始化row */
xsynchrow* XsynchRowInit(int rowcnt)
{
    xsynchrow* row = NULL;

    row = (xsynchrow*)malloc(sizeof(xsynchrow) * rowcnt);
    if (NULL == row)
    {
        return NULL;
    }
    memset(row, 0, sizeof(xsynchrow) * rowcnt);
    row->columncnt = 0;
    row->columns = NULL;

    return row;
}

/* 初始化xsynchpair */
xsynchpair* XsynchPairInit(int colcnt)
{
    xsynchpair* col = NULL;

    col = (xsynchpair*)malloc(sizeof(xsynchpair) * colcnt);
    if (NULL == col)
    {
        return NULL;
    }
    memset(col, 0, sizeof(xsynchpair) * colcnt);

    col->key = NULL;
    col->keylen = 0;
    col->value = NULL;
    col->valuelen = 0;
    return col;
}

/* 释放pari资源 */
void XsynchPairFree(int colcnt, void* in_col)
{
    int idx_col = 0;
    xsynchpair* col = NULL;
    xsynchpair* tmpcol = NULL;

    col = (xsynchpair*)in_col;

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

/* 释放rows资源 */
void XsynchRowFree(int rowcnt, void* in_rows)
{
    int idx_row = 0;
    xsynchrow* rows = NULL;
    xsynchrow* tmprow = NULL;
    rows = (xsynchrow*) in_rows;

    if (0 == rowcnt || NULL == rows)
    {
        return;
    }

    for (idx_row = 0; idx_row < rowcnt; idx_row++)
    {
        tmprow = &rows[idx_row];
        XsynchPairFree(tmprow->columncnt, tmprow->columns);
    }
    free(rows);

    return;
}

/* 设置连接参数 */
xsynchconn* XSynchSetParam(char* connstr)
{
    /*
     * 1、解析 connstr, 并写入到 xsynchconn 结构中
     * 2、查看 port 是否为有效值, 非有效值, 那么设置 port 为 config.h 中的 RMANAGER_PORT
     * 3、查看 host 是否为有效值, 非有效值, 那么设置 host 为 config.h 中的 RMANAGER_UNIXDOMAINPREFIX 与 port 的拼接
     * 4、执行链接
     * 5、检测是否连接上
     * 6、tcpkeepalive 设置等
     * 7、发送 identity 消息
     * 8、接收 identity 消息
     */
    xsynchconn* xconn = NULL;

    xconn = malloc(sizeof(xsynchconn));
    if (NULL == xconn)
    {
        return NULL;
    }
    memset(xconn, '\0', sizeof(xsynchconn));

    if (NULL == connstr)
    {
        snprintf(xconn->host, 512, "%s/%s.%s", RMANAGER_UNIXDOMAINDIR, RMANAGER_UNIXDOMAINPREFIX, RMANAGER_PORT);
        snprintf(xconn->port, 128, "%s", RMANAGER_PORT);
        xconn->keepalive = 0;
        xconn->socktype = XSYNCH_SOCKTYPE_UNIXDOMAIN;
    }
    else
    {
        if (false == XSynchParseoptions(xconn, connstr))
        {
            free(xconn);
            return NULL;
        }

        if (XSYNCH_SOCKTYPE_NOP == xconn->socktype)
        {
            return NULL;
        }
    }
    
    xconn->sendmsg = xsynch_exbufferdata_init();
    if (NULL == xconn->sendmsg)
    {
        free(xconn);
        return NULL;
    }

    xconn->recvmsg = xsynch_exbufferdata_init();
    if (NULL == xconn->recvmsg)
    {
        xsynch_exbufferdata_free(xconn->sendmsg);
        free(xconn);
        return NULL;
    }

    xconn->errmsg = xsynch_exbufferdata_init();
    if (NULL == xconn->errmsg)
    {
        xsynch_exbufferdata_free(xconn->sendmsg);
        xsynch_exbufferdata_free(xconn->recvmsg);
        free(xconn);
        return NULL;
    }

    xconn->result = (xsynch_result *)XsynchResultInit();
    if (NULL == xconn->result)
    {
        xsynch_exbufferdata_free(xconn->sendmsg);
        xsynch_exbufferdata_free(xconn->recvmsg);
        xsynch_exbufferdata_free(xconn->errmsg);
        free(xconn);
        return NULL;
    }

    /* 连接 xmanager */
    if (false == xsynch_fenet_conn(xconn))
    {
        return xconn;
    }
    return xconn;
}

/* 连接 */
bool XSynchConn(xsynchconn* conn)
{
    int rownumber = 0;
    xsynchrow* rows = NULL;
    xsynch_identitycmd icmd = { {0} };

    /* 检测是否连接 */
    if (false == xsynch_fenet_isconn(conn))
    {
        /* 连接 xmanager */
        if (false == xsynch_fenet_conn(conn))
        {
            return false;
        }
    }

    /* 构建 command */
    icmd.type.type = T_XSYNCH_IDENTITYCMD;
    icmd.jobname = "xscsci";
    icmd.user = NULL;
    icmd.passwd = NULL;
    icmd.kind = XSYNCH_JOBKIND_XSCSCI;

    if (false == xsynch_febuildmsg_cmd2msg(&icmd.type, conn->sendmsg))
    {
        return false;
    }

    /* 发送 cmd */
    if (false == xsynch_fenet_sendcmd(conn))
    {
        return false;
    }

    XSynchGetResult(conn, &rownumber, &rows);

    /* 解析数据 */
    return true;
}

/* 发送命令 */
bool XSynchSendCmd(xsynchconn* conn, xsynch_cmd* cmd)
{
    /*
     * 1、检测连接是否有效，无效返回错误信息
     * 2、根据 cmd 组装数据流
     *      xsynch_febuildmsg_cmd2msg
     * 
     * 3、发送数据至 xmanager
     * 4、获取 xmanager 返回结果
     */

     /* 检测是否连接 */
    if (false == xsynch_fenet_isconn(conn))
    {
        /* 连接 xmanager */
        if (false == XSynchConn(conn))
        {
            return false;
        }
    }

    /* 根据 cmd 组装数据流 */
    if (false == xsynch_febuildmsg_cmd2msg(cmd, conn->sendmsg))
    {
        return false;
    }

    /* 发送 cmd */
    if (false == xsynch_fenet_sendcmd(conn))
    {
        return false;
    }
    return true;
}

/* 测试xmanager是否启动 */
bool XSynchPing(xsynchconn* conn)
{
     /* 检测是否连接 */
    if (false == xsynch_fenet_isconn(conn))
    {
        /* 连接 xmanager */
        if (false == XSynchConn(conn))
        {
            return false;
        }
    }

    return true;
}

/* 获取返回结果 */
void XSynchGetResult(xsynchconn* conn, int* rownumber, xsynchrow** rows)
{
    /*
     * 1、解析 conn->recvmsg 中的数据
     *      xsynch_feparsemsg_msg2result
     * 2、清理 conn->recvmsg 中的数据
     * 3、返回 rownumber 和 rows
     */

    *rownumber = 0;
    *rows = NULL;

    /* 获取结果出错，打印错误信息清理数据 */
    if (false == xsynch_feparsemsg_msg2result(conn->recvmsg, conn))
    {
        printf("get result failed:%s \n", conn->errmsg->data);
        xsynch_exbufferdata_reset(conn->recvmsg);
        xsynch_exbufferdata_reset(conn->errmsg);
        XsynchResultReset(conn->result);
        return;
    }

    /* 设置返回值释放资源 */
    *rownumber = conn->result->rowcnt;
    *rows = conn->result->rows;
    conn->result->rowcnt = 0;
    conn->result->rows = NULL;

    return;
}

/* 获取错误信息 */
void XSynchGetErrmsg(xsynchconn* conn)
{
    /*
     * 输出错误信息
     */
    if (NULL == conn || XSYNCHCONN_STATUS_OK != conn->connstatus)
    {
        return;
    }

    if (0 != conn->errcode)
    {
        printf("%s \n", conn->errmsg->data);
    }

    return;
}

/* 清理返回结果 */
void XSynchClear(xsynchconn* conn)
{
    if (NULL == conn)
    {
        return;
    }

    xsynch_exbufferdata_reset(conn->sendmsg);
    xsynch_exbufferdata_reset(conn->recvmsg);
    xsynch_exbufferdata_reset(conn->errmsg);

    if (conn->result)
    {
        XsynchResultReset(conn->result);
    }
    return;
}

/* 清理 xsynchconn */
void XSynchFinish(xsynchconn* conn)
{
    if (NULL == conn)
    {
        return;
    }
    
    xsynch_exbufferdata_free(conn->sendmsg);
    xsynch_exbufferdata_free(conn->recvmsg);
    xsynch_exbufferdata_free(conn->errmsg);

    if (conn->result)
    {
        XsynchResultReset(conn->result);
        free(conn->result);
    }

    free(conn);

    return;
}


void xsynch_rangvar_destroy(xsynch_rangevar* rs)
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

xsynch_rangevar *xsynch_rangvar_init(char* schema, char* table)
{
    xsynch_rangevar    *rs = NULL;

    rs = malloc(sizeof(xsynch_rangevar));
    if (NULL == rs)
    {
        return NULL;
    }
    memset(rs, 0, sizeof(xsynch_rangevar));
    rs->schema = schema;
    rs->table = table;
    return rs;
}

void xsynch_rangvar_free(xsynch_rangevar *rangevar)
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

xsynch_job *xsynch_job_init(int jobkind, char* jobname)
{
    xsynch_job *job = NULL;

    job = malloc(sizeof(xsynch_job));
    if (NULL == job)
    {
        return NULL;
    }
    memset(job, 0, sizeof(xsynch_job));
    job->kind = jobkind;
    job->jobname = jobname;
    return job;
}

void xsynch_job_free(xsynch_job* job)
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

/* identity 资源释放 */
static void xsynch_command_identityfree(xsynch_cmd* cmd)
{
    xsynch_identitycmd* identity = NULL;

    if (NULL == cmd)
    {
        return;
    }

    identity = (xsynch_identitycmd*)cmd;

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

/* create 资源释放 */
static void xsynch_command_createfree(xsynch_cmd* cmd)
{
    ListCell* lc = NULL;
    xsynch_job* job = NULL;
    xsynch_createcmd* create = NULL;

    if (NULL == cmd)
    {
        return;
    }

    create = (xsynch_createcmd*)cmd;

    if (NULL != create->name)
    {
        free(create->name);
    }

    if (NULL != create->job)
    {
        foreach(lc, create->job)
        {
            job = (xsynch_job*)lfirst(lc);
            xsynch_job_free(job);
        }

        list_free(create->job);
    }

    free(create);
}

/* alter 资源释放 */
static void xsynch_command_alterfree(xsynch_cmd* cmd)
{
    ListCell* lc = NULL;
    xsynch_job* job = NULL;
    xsynch_altercmd* alter = NULL;

    if (NULL == cmd)
    {
        return;
    }

    alter = (xsynch_altercmd*)cmd;

    if (NULL != alter->name)
    {
        free(alter->name);
    }

    if (NULL != alter->job)
    {
        foreach(lc, alter->job)
        {
            job = (xsynch_job*)lfirst(lc);
            xsynch_job_free(job);
        }

        list_free(alter->job);
    }

    free(alter);
    return;
}

/* remove 资源释放 */
static void xsynch_command_removefree(xsynch_cmd* cmd)
{
    xsynch_removecmd* remove = NULL;

    if (NULL == cmd)
    {
        return;
    }

    remove = (xsynch_removecmd*)cmd;

    if (NULL != remove->name)
    {
        free(remove->name);
    }

    free(remove);
    return;
}

/* drop 资源释放 */
static void xsynch_command_dropfree(xsynch_cmd* cmd)
{
    xsynch_dropcmd* drop = NULL;

    if (NULL == cmd)
    {
        return;
    }

    drop = (xsynch_dropcmd*)cmd;

    if (NULL != drop->name)
    {
        free(drop->name);
    }

    free(drop);
    return;
}

/* init 资源释放 */
static void xsynch_command_initfree(xsynch_cmd* cmd)
{
    xsynch_initcmd* init = NULL;

    if (NULL == cmd)
    {
        return;
    }

    init = (xsynch_initcmd*)cmd;

    if (NULL != init->name)
    {
        free(init->name);
    }

    free(init);
    return;
}

/* edit 资源释放 */
static void xsynch_command_editfree(xsynch_cmd* cmd)
{
    xsynch_editcmd* edit = NULL;

    if (NULL == cmd)
    {
        return;
    }

    edit = (xsynch_editcmd*)cmd;

    if (NULL != edit->name)
    {
        free(edit->name);
    }

    free(edit);
    return;
}

/* start 资源释放 */
static void xsynch_command_startfree(xsynch_cmd* cmd)
{
    xsynch_startcmd* start = NULL;

    if (NULL == cmd)
    {
        return;
    }

    start = (xsynch_startcmd*)cmd;

    if (NULL != start->name)
    {
        free(start->name);
    }

    free(start);
    return;
}

/* stop 资源释放 */
static void xsynch_command_stopfree(xsynch_cmd* cmd)
{
    xsynch_stopcmd* stop = NULL;

    if (NULL == cmd)
    {
        return;
    }

    stop = (xsynch_stopcmd*)cmd;

    if (NULL != stop->name)
    {
        free(stop->name);
    }

    free(stop);
    return;
}

/* reload 资源释放 */
static void xsynch_command_reloadfree(xsynch_cmd* cmd)
{
    xsynch_reloadcmd* reload = NULL;

    if (NULL == cmd)
    {
        return;
    }

    reload = (xsynch_reloadcmd*)cmd;

    if (NULL != reload->name)
    {
        free(reload->name);
    }

    free(reload);
    return;
}

/* info 资源释放 */
static void xsynch_command_infofree(xsynch_cmd* cmd)
{
    xsynch_infocmd* info = NULL;

    if (NULL == cmd)
    {
        return;
    }

    info = (xsynch_infocmd*)cmd;

    if (NULL != info->name)
    {
        free(info->name);
    }

    free(info);
    return;
}

/* watch 资源释放 */
static void xsynch_command_watchfree(xsynch_cmd* cmd)
{
    xsynch_watchcmd* watch = NULL;

    if (NULL == cmd)
    {
        return;
    }

    watch = (xsynch_watchcmd*)cmd;

    if (NULL != watch->name)
    {
        free(watch->name);
    }

    free(watch);
    return;
}

/* cfgfile 资源释放 */
static void xsynch_command_cfgfilefree(xsynch_cmd* cmd)
{
    xsynch_cfgfilecmd* cfgfile = NULL;
    
    if (NULL == cmd)
    {
        return;
    }

    cfgfile = (xsynch_cfgfilecmd*)cmd;

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

/* refresh 资源释放 */
static void xsynch_command_refreshfree(xsynch_cmd* cmd)
{
    ListCell* lc = NULL;
    xsynch_rangevar *rs = NULL;
    xsynch_refreshcmd* refresh = NULL;

    if (NULL == cmd)
    {
        return;
    }

    refresh = (xsynch_refreshcmd*)cmd;

    if (NULL != refresh->name)
    {
        free(refresh->name);
    }

    if (NULL != refresh->tables)
    {
        foreach(lc, refresh->tables)
        {
            rs = (xsynch_rangevar*)lfirst(lc);
            xsynch_rangvar_free(rs);
        }

        list_free(refresh->tables);
    }

    free(refresh);
    return;
}

/* list 资源释放 */
static void xsynch_command_listfree(xsynch_cmd* cmd)
{
    if (NULL == cmd)
    {
        return;
    }
    
    free(cmd);
    return;
}

static xsynch_commandops m_commandfree[] =
{
    {
        T_XSYNCH_NOP,
        "NOP",
        NULL
    },
    {
        T_XSYNCH_IDENTITYCMD,
        "IDENTITY COMMAND",
        xsynch_command_identityfree
    },
    {
        T_XSYNCH_CREATECMD,
        "CREATE COMMAND",
        xsynch_command_createfree
    },
    {
        T_XSYNCH_ALTERCMD,
        "ALTER COMMAND",
        xsynch_command_alterfree
    },
    {
        T_XSYNCH_REMOVECMD,
        "REMOVE COMMAND",
        xsynch_command_removefree
    },
    {
        T_XSYNCH_DROPCMD,
        "DROP COMMAND",
        xsynch_command_dropfree
    },
    {
        T_XSYNCH_INITCMD,
        "INIT COMMAND",
        xsynch_command_initfree
    },
    {
        T_XSYNCH_EDITCMD,
        "EDIT COMMAND",
        xsynch_command_editfree
    },
    {
        T_XSYNCH_STARTCMD,
        "START COMMAND",
        xsynch_command_startfree
    },
    {
        T_XSYNCH_STOPCMD,
        "STOP COMMAND",
        xsynch_command_stopfree
    },
    {
        T_XSYNCH_RELOADCMD,
        "RELOAD COMMAND",
        xsynch_command_reloadfree
    },
    {
        T_XSYNCH_INFOCMD,
        "INFO COMMAND",
        xsynch_command_infofree
    },
    {
        T_XSYNCH_WATCHCMD,
        "WATCH COMMAND",
        xsynch_command_watchfree
    },
    {
        T_XSYNCH_CFGfILECMD,
        "never trigger",
        xsynch_command_cfgfilefree
    },
    {
        T_XSYNCH_REFRESHCMD,
        "REFRESH COMMAND",
        xsynch_command_refreshfree
    },
    {
        T_XSYNCH_LISTCMD,
        "LIST COMMAND",
        xsynch_command_listfree
    },
    {
        T_XSYNCH_MAX,
        "MAX COMMAND",
        NULL
    }
};

void xsynch_command_free(xsynch_cmd* cmd)
{
    if(T_XSYNCH_MAX < cmd->type)
    {
        printf("command unknown cmd type %d\n", cmd->type);
        return;
    }

    if(NULL == m_commandfree[cmd->type].free)
    {
        printf("command unsupport %s\n", m_commandfree[cmd->type].desc);
        return ;
    }

    return m_commandfree[cmd->type].free(cmd);
}