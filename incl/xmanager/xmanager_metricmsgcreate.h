#ifndef _XMANAGER_METRICMSGCREATE_H_
#define _XMANAGER_METRICMSGCREATE_H_

/*
 * 处理 create 命令
 *  1、jobtype 需要小于 ALL
 *  2、校验 job 是否已经存在
 *  3、将 job 加入到 xmetric->metricnodes 中
*/
extern bool xmanager_metricmsg_parsecreate(xmanager_metric* xmetric,
                                                  netpoolentry* npoolentry,
                                                  netpacket* npacket);

#endif
