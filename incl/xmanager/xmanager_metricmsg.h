#ifndef _XMANAGER_METRICMSG_H_
#define _XMANAGER_METRICMSG_H_

extern bool xmanager_metricmsg_assemblecmdresult(xmanager_metric* xmetric,
                                                        netpoolentry* npoolentry,
                                                        xmanager_msg msgtype);

/* 组装错误信息 */
extern bool xmanager_metricmsg_assembleerrormsg(xmanager_metric* xmetric,
                                                       queue* queue,
                                                       int type,
                                                       int errorcode,
                                                       char* errormsg);

extern char* xmanager_metricmsg_getdesc(xmanager_msg msgtype);

/* 
 * 解析数据包
 *  返回 false 时, 需要在外面释放 npoolentry
*/
extern bool xmanager_metricmsg_assembleresponse(xmanager_metric* xmetric,
                                                       netpoolentry* npoolentry,
                                                       xmanager_msg msgtype,
                                                       dlist* dlmsgs);

/* 
 * 解析数据包
*/
extern bool xmanager_metricmsg_parsenetpacket(xmanager_metric* xmetric,
                                                     netpoolentry* npoolentry,
                                                     netpacket* npacket);

#endif
