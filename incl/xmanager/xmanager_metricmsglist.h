#ifndef _RIPPLE_XMANAGER_METRICMSGLIST_H_
#define _RIPPLE_XMANAGER_METRICMSGLIST_H_

/*
 * 处理 list 命令
 *  1、返回metricnode信息
*/
extern bool ripple_xmanager_metricmsg_parselist(ripple_xmanager_metric* xmetric,
                                                  ripple_netpoolentry* npoolentry,
                                                  ripple_netpacket* npacket);

#endif
