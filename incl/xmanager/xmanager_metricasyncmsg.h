#ifndef _XMANAGER_METRICASYNCMSG_H_
#define _XMANAGER_METRICASYNCMSG_H_

/* 消息超时时间 */
#define XMANAGER_METRICASYNCHMSG_TIMEOUT              10000

typedef struct XMANAGER_METRICASYNCMSG
{
    /* 节点类型 */
    xmanager_metricnodetype          type;

    /* 消息类型 */
    xmanager_msg                     msgtype;

    /* 结果, 0 成功, 1 失败 */
    int8                                    result;

    /* 错误码 */
    int                                     errcode;

    /* 节点名称 */
    char*                                   name;

    /* 错误消息 */
    char*                                   errormsg;
} xmanager_metricasyncmsg;

typedef struct XMANAGER_METRICASYNCMSGS
{
    /* 超时时间 */
    int                                     timeout;

    /* xmanager_metricasyncmsg */
    dlist*                                  msgs;

    /* xmanager_metricasyncmsg */
    dlist*                                  results;
} xmanager_metricasyncmsgs;

extern xmanager_metricasyncmsg* xmanager_metricasyncmsg_init(void);

extern xmanager_metricasyncmsgs* xmanager_metricasyncmsgs_init(void);

/* 删除 metricasyncmsg */
extern void xmanager_metricasyncmsg_destroy(xmanager_metricasyncmsg* xmetricasyncmsg);

/* 删除 metricasyncmsg */
extern void xmanager_metricasyncmsg_destroyvoid(void* args);

extern void xmanager_metricasyncmsgs_destroy(xmanager_metricasyncmsgs* xmetricasyncmsgs);

#endif
