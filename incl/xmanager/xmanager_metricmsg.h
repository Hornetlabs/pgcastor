#ifndef _XMANAGER_METRICMSG_H_
#define _XMANAGER_METRICMSG_H_

extern bool xmanager_metricmsg_assemblecmdresult(xmanager_metric* xmetric,
                                                 netpoolentry*    npoolentry,
                                                 xmanager_msg     msgtype);

/* Assemble error message */
extern bool xmanager_metricmsg_assembleerrormsg(xmanager_metric* xmetric,
                                                queue*           queue,
                                                int              type,
                                                int              errorcode,
                                                char*            errormsg);

extern char* xmanager_metricmsg_getdesc(xmanager_msg msgtype);

/*
 * Parse data packet
 *  When returning false, need to release npoolentry outside
 */
extern bool xmanager_metricmsg_assembleresponse(xmanager_metric* xmetric,
                                                netpoolentry*    npoolentry,
                                                xmanager_msg     msgtype,
                                                dlist*           dlmsgs);

/*
 * Parse data packet
 */
extern bool xmanager_metricmsg_parsenetpacket(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket);

#endif
