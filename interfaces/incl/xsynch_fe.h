#ifndef _XSYNCH_FE_H_
#define _XSYNCH_FE_H_

#ifndef bool
typedef int8_t bool;
#endif

#ifndef true
#define true	((bool) 1)
#endif

#ifndef false
#define false	((bool) 0)
#endif

typedef unsigned int                    Oid;


#include "list_func.h"

typedef enum XSYNCH_CMDTAG
{
    T_XSYNCH_NOP                            = 0,
    T_XSYNCH_IDENTITYCMD                    ,
    T_XSYNCH_CREATECMD                      ,
    T_XSYNCH_ALTERCMD                       ,
    T_XSYNCH_REMOVECMD                      ,
    T_XSYNCH_DROPCMD                        ,
    T_XSYNCH_INITCMD                        ,
    T_XSYNCH_EDITCMD                        ,
    T_XSYNCH_STARTCMD                       ,
    T_XSYNCH_STOPCMD                        ,
    T_XSYNCH_RELOADCMD                      ,
    T_XSYNCH_INFOCMD                        ,
    T_XSYNCH_WATCHCMD                       ,
    T_XSYNCH_CFGfILECMD                     ,
    T_XSYNCH_REFRESHCMD                     ,
    T_XSYNCH_LISTCMD                        ,

    /* 在此前添加 */
    T_XSYNCH_MAX                            
} xysnch_cmdtag;

typedef struct XSYNCH_CMD
{
    xysnch_cmdtag                          type;
} xsynch_cmd;

typedef enum XSYNCH_JOBKIND
{
    XSYNCH_JOBKIND_NOP                      = 0,
    XSYNCH_JOBKIND_CAPTURE                  ,
    XSYNCH_JOBKIND_PUMP                     ,
    XSYNCH_JOBKIND_COLLECTOR                ,
    XSYNCH_JOBKIND_INTEGRATE                ,
    XSYNCH_JOBKIND_HGRECEIVELOG             ,
    XSYNCH_JOBKIND_PGRECEIVELOG             ,
    XSYNCH_JOBKIND_PROCESS                  ,

    /*---------有具体业务类型的在此上添加 ------*/

    /* 对 start/stop/info/watch 起作用 */
    XSYNCH_JOBKIND_ALL                      ,

    /* xmanager */
    XSYNCH_JOBKIND_MANAGER                  ,

    /* XSCSCI */
    XSYNCH_JOBKIND_XSCSCI                   ,

    /* 在此之前添加 */
    XSYNCH_JOBKIND_MAX
} xsynch_jobkind;

typedef enum XSYNCH_ACTION
{
    XSYNCH_ACTION_NOP                        = 0,
    XSYNCH_ACTION_ADD                       ,
    XSYNCH_ACTION_REMOVE                    ,

    /* 在此前添加 */
    XSYNCH_ACTION_MAX
} xsynch_action;

typedef struct XSYNCH_RANGEVAR
{
    char*                                   schema;
    char*                                   table;
} xsynch_rangevar;

typedef struct XSYNCH_JOB
{
    xsynch_jobkind                          kind;
    char*                                   jobname;
} xsynch_job;

/* 创建指定作业类型 */
typedef struct XSYNCH_IDENTITYCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /*
     * 暂无用途
     */
    /* 用户名 */
    char*                                   user;

    /* 密码 */
    char*                                   passwd;

    /* 作业名称 */
    char*                                   jobname;
} xsynch_identitycmd;

typedef struct XSYNCH_CREATECMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;

    /* xsynch_job */
    List*                                   job;
} xsynch_createcmd;

/* 修改 process 作业的成员 */
typedef struct XSYNCH_ALTERCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 操作类型, add/remove */
    xsynch_action                           action;

    /* 名称 */
    char*                                   name;

    /* xsynch_job */
    List*                                   job;
} xsynch_altercmd;

/* 删除指定作业类型的配置文件 */
typedef struct XSYNCH_REMOVECMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
} xsynch_removecmd;

/* 删除指定作业 */
typedef struct XSYNCH_DROPCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
} xsynch_dropcmd;

/* 初始化指定作业 */
typedef struct XSYNCH_INITCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
}  xsynch_initcmd;

/* 编辑指定作业配置文件 */
typedef struct XSYNCH_EDITCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
}  xsynch_editcmd;

/* 启动指定作业 */
typedef struct XSYNCH_STARTCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
}  xsynch_startcmd;

/* 停止指定作业 */
typedef struct XSYNCH_STOPCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
}  xsynch_stopcmd;

/* 重载指定作业的配置文件 */
typedef struct XSYNCH_RELOADCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
}  xsynch_reloadcmd;

/* 查看指定作业的基础信息 */
typedef struct XSYNCH_INFOCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;
}  xsynch_infocmd;

/* 查看指定作业的基础信息 */
typedef struct XSYNCH_LISTCMD
{
    xsynch_cmd                              type;
}  xsynch_listcmd;

/* 定时返回指定作业的信息 */
typedef struct XSYNCH_WATCHCMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 间隔, 单位秒 */
    int                                     interval;

    /* 作业名称 */
    char*                                   name;
} xsynch_watchcmd;

/* 发送配置文件信息 */
typedef struct XSYNCH_CFGFILECMD
{
    xsynch_cmd                              type;

    /* 作业类型 */
    xsynch_jobkind                          kind;

    /* 作业名称 */
    char*                                   name;

    /* 文件名称 */
    char*                                   filename;

    /* 文件内容 */
    int                                     datalen;

    /* 文件内容 */
    char*                                   data;
} xsynch_cfgfilecmd;

/* refresh 命令 */
typedef struct XSYNCH_REFRESHCMD
{
    xsynch_cmd                              type;

    /* capture 名称 */
    char*                                   name;

    /* xsynch_rangevar */
    List*                                   tables;
} xsynch_refreshcmd;

#define XSYNCH_NEWCMD(size, tag) \
({ \
    xsynch_cmd *_result = NULL; \
    _result = (xsynch_cmd *) malloc(size); \
    _result->type = (tag); \
    _result; \
})

#define XSYNCH_MAKECMD(_type_)                     ((struct _type_ *) XSYNCH_NEWCMD(sizeof(struct _type_),T_##_type_))

/* key/value 数值 */
typedef struct XSYNCHPAIR
{
    int                 keylen;
    int                 valuelen;
    char*               key;

    /* value 可能为空, 当 value 为空时, 说明未取到值或没有此列信息 */
    char*               value;
} xsynchpair;

/* 行数值 */
typedef struct XSYNCHROW
{
    int                 columncnt;
    xsynchpair*         columns;
} xsynchrow;

/* 连接状态 */
typedef enum XSYNCHCONN_STATUS
{
    XSYNCHCONN_STATUS_NOP                   = 0x00,

    /* 连接中 */
    XSYNCHCONN_STATUS_INPROCESS             ,

    /* 链接上 */
    XSYNCHCONN_STATUS_OK                    
} xsynchconn_status;

typedef struct XSYNCH_CONN           xsynchconn;

/* 初始化xsynch_result */
void* XsynchResultInit(void);

/* 重置xsynch_result内容 */
void XsynchResultReset(void* in_result);

/* 释放pari资源 */
xsynchrow* XsynchRowInit(int rowcnt);

/* 初始化xsynchpair */
xsynchpair* XsynchPairInit(int colcnt);

/* 释放rows资源 */
void XsynchRowFree(int rowcnt, void* in_rows);

/* 释放pair资源 */
void XsynchPairFree(int colcnt, void* in_col);

/* 设置连接参数 */
xsynchconn* XSynchSetParam(char* connstr);

/* 链接 xmanager */
bool XSynchConn(xsynchconn* conn);

/* 测试 xmanager 是否启动 */
bool XSynchPing(xsynchconn* conn);

/* 发送命令 */
bool XSynchSendCmd(xsynchconn* conn, xsynch_cmd* cmd);

/* 获取返回结果 */
void XSynchGetResult(xsynchconn* conn, int* rownumber, xsynchrow** rows);

/* 清理返回结果 */
void XSynchClear(xsynchconn* conn);

void XSynchGetErrmsg(xsynchconn* conn);

/* 清理 xsynchconn */
void XSynchFinish(xsynchconn* conn);

void xsynch_rangvar_destroy(xsynch_rangevar* rs);

xsynch_rangevar *xsynch_rangvar_init(char* schema, char* table);

void xsynch_rangvar_free(xsynch_rangevar *rangevar);

xsynch_job *xsynch_job_init(int jobkind, char* jobname);

void xsynch_job_free(xsynch_job* job);

void xsynch_command_free(xsynch_cmd* cmd);

#endif
