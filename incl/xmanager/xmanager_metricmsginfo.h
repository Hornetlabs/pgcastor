#ifndef _XMANAGER_METRICMSGINFO_H_
#define _XMANAGER_METRICMSGINFO_H_

/*
 * 处理 info 命令
 *  1、jobtype 需要小于 ALL
 *  2、校验 job 是否已经存在
 *  3、获取信息并返回
*/
extern bool xmanager_metricmsg_parseinfo(xmanager_metric* xmetric,
                                                  netpoolentry* npoolentry,
                                                  netpacket* npacket);

#endif
