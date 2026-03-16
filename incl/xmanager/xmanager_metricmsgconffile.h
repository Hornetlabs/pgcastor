#ifndef _RIPPLE_XMANAGER_METRICMSGCONFFILE_H_
#define _RIPPLE_XMANAGER_METRICMSGCONFFILE_H_

/*
 * 处理 conffile 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否已经存在
 *  3、覆盖conf文件
*/
bool ripple_xmanager_metricmsg_parseconffile(ripple_xmanager_metric* xmetric,
                                             ripple_netpoolentry* npoolentry,
                                             ripple_netpacket* npacket);

#endif
