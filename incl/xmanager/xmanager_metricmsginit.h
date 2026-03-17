#ifndef _XMANAGER_METRICMSGINIT_H_
#define _XMANAGER_METRICMSGINIT_H_

/*
 * 处理 init 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否存在, 不存在报错
 *  3、创建异步消息挂载到 xscsci 节点上
 *  4、执行初始化命令
*/
extern bool xmanager_metricmsg_parseinit(xmanager_metric* xmetric,
                                                netpoolentry* npoolentry,
                                                netpacket* npacket);

/*
 * 组装 init 返回消息
*/
extern bool xmanager_metricmsg_assembleinit(xmanager_metric* xmetric,
                                                   netpoolentry* npoolentry,
                                                   dlist* dlmsgs);


#endif
