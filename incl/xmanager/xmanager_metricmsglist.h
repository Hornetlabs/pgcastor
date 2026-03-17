#ifndef _XMANAGER_METRICMSGLIST_H_
#define _XMANAGER_METRICMSGLIST_H_

/*
 * 处理 list 命令
 *  1、返回metricnode信息
*/
extern bool xmanager_metricmsg_parselist(xmanager_metric* xmetric,
                                                  netpoolentry* npoolentry,
                                                  netpacket* npacket);

#endif
