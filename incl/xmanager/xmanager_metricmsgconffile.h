#ifndef _XMANAGER_METRICMSGCONFFILE_H_
#define _XMANAGER_METRICMSGCONFFILE_H_

/*
 * 处理 conffile 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否已经存在
 *  3、覆盖conf文件
*/
bool xmanager_metricmsg_parseconffile(xmanager_metric* xmetric,
                                             netpoolentry* npoolentry,
                                             netpacket* npacket);

#endif
