#ifndef _XMANAGER_METRICASYNCMSG_H_
#define _XMANAGER_METRICASYNCMSG_H_

/* Message timeout */
#define XMANAGER_METRICASYNCHMSG_TIMEOUT 10000

typedef struct XMANAGER_METRICASYNCMSG
{
    /* Node type */
    xmanager_metricnodetype type;

    /* Message type */
    xmanager_msg            msgtype;

    /* Result, 0 success, 1 failure */
    int8                    result;

    /* Error code */
    int                     errcode;

    /* Node name */
    char*                   name;

    /* Error message */
    char*                   errormsg;
} xmanager_metricasyncmsg;

typedef struct XMANAGER_METRICASYNCMSGS
{
    /* Timeout */
    int    timeout;

    /* xmanager_metricasyncmsg */
    dlist* msgs;

    /* xmanager_metricasyncmsg */
    dlist* results;
} xmanager_metricasyncmsgs;

extern xmanager_metricasyncmsg* xmanager_metricasyncmsg_init(void);

extern xmanager_metricasyncmsgs* xmanager_metricasyncmsgs_init(void);

/* Delete metricasyncmsg */
extern void xmanager_metricasyncmsg_destroy(xmanager_metricasyncmsg* xmetricasyncmsg);

/* Delete metricasyncmsg */
extern void xmanager_metricasyncmsg_destroyvoid(void* args);

extern void xmanager_metricasyncmsgs_destroy(xmanager_metricasyncmsgs* xmetricasyncmsgs);

#endif
