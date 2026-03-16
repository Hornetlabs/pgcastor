#ifndef _RIPPLE_XMANAGER_METRICMSGEDIT_H_
#define _RIPPLE_XMANAGER_METRICMSGEDIT_H_

/*
 * 处理 edit 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否存在, 不存在报错
 *  3、获取配置文件内容
 *  4、组装返回消息
*/
extern bool ripple_xmanager_metricmsg_parseedit(ripple_xmanager_metric* xmetric,
                                                 ripple_netpoolentry* npoolentry,
                                                 ripple_netpacket* npacket);

#endif
