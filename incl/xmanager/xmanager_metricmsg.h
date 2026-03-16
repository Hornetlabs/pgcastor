#ifndef _RIPPLE_XMANAGER_METRICMSG_H_
#define _RIPPLE_XMANAGER_METRICMSG_H_

extern bool ripple_xmanager_metricmsg_assemblecmdresult(ripple_xmanager_metric* xmetric,
                                                        ripple_netpoolentry* npoolentry,
                                                        ripple_xmanager_msg msgtype);

/* 组装错误信息 */
extern bool ripple_xmanager_metricmsg_assembleerrormsg(ripple_xmanager_metric* xmetric,
                                                       ripple_queue* queue,
                                                       int type,
                                                       int errorcode,
                                                       char* errormsg);

extern char* ripple_xmanager_metricmsg_getdesc(ripple_xmanager_msg msgtype);

/* 
 * 解析数据包
 *  返回 false 时, 需要在外面释放 npoolentry
*/
extern bool ripple_xmanager_metricmsg_assembleresponse(ripple_xmanager_metric* xmetric,
                                                       ripple_netpoolentry* npoolentry,
                                                       ripple_xmanager_msg msgtype,
                                                       dlist* dlmsgs);

/* 
 * 解析数据包
*/
extern bool ripple_xmanager_metricmsg_parsenetpacket(ripple_xmanager_metric* xmetric,
                                                     ripple_netpoolentry* npoolentry,
                                                     ripple_netpacket* npacket);

#endif
