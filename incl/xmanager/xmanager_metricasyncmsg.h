#ifndef _RIPPLE_XMANAGER_METRICASYNCMSG_H_
#define _RIPPLE_XMANAGER_METRICASYNCMSG_H_

/* 消息超时时间 */
#define RIPPLE_XMANAGER_METRICASYNCHMSG_TIMEOUT              10000

typedef struct RIPPLE_XMANAGER_METRICASYNCMSG
{
    /* 节点类型 */
    ripple_xmanager_metricnodetype          type;

    /* 消息类型 */
    ripple_xmanager_msg                     msgtype;

    /* 结果, 0 成功, 1 失败 */
    int8                                    result;

    /* 错误码 */
    int                                     errcode;

    /* 节点名称 */
    char*                                   name;

    /* 错误消息 */
    char*                                   errormsg;
} ripple_xmanager_metricasyncmsg;

typedef struct RIPPLE_XMANAGER_METRICASYNCMSGS
{
    /* 超时时间 */
    int                                     timeout;

    /* ripple_xmanager_metricasyncmsg */
    dlist*                                  msgs;

    /* ripple_xmanager_metricasyncmsg */
    dlist*                                  results;
} ripple_xmanager_metricasyncmsgs;

extern ripple_xmanager_metricasyncmsg* ripple_xmanager_metricasyncmsg_init(void);

extern ripple_xmanager_metricasyncmsgs* ripple_xmanager_metricasyncmsgs_init(void);

/* 删除 metricasyncmsg */
extern void ripple_xmanager_metricasyncmsg_destroy(ripple_xmanager_metricasyncmsg* xmetricasyncmsg);

/* 删除 metricasyncmsg */
extern void ripple_xmanager_metricasyncmsg_destroyvoid(void* args);

extern void ripple_xmanager_metricasyncmsgs_destroy(ripple_xmanager_metricasyncmsgs* xmetricasyncmsgs);

#endif
