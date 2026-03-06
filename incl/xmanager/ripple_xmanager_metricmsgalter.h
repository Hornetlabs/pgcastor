#ifndef _RIPPLE_XMANAGER_METRICMSGALTER_H_
#define _RIPPLE_XMANAGER_METRICMSGALTER_H_

/*
 * 处理 alter 命令
 *  1、jobtype 等于 PROCESS
 *  2、如果是capture, 检查是否有同类型job
 *  3、add/remove job
 *  4、组装返回消息
*/
extern bool ripple_xmanager_metricmsg_parsealter(ripple_xmanager_metric* xmetric,
                                                 ripple_netpoolentry* npoolentry,
                                                 ripple_netpacket* npacket);

#endif
