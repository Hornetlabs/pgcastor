#ifndef _RIPPLE_XMANAGER_METRICMSGDROP_H_
#define _RIPPLE_XMANAGER_METRICMSGDROP_H_

/*
 * 处理 drop 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否已经存在
 *  3、检验是否在运行中
 *  4、删除data和conf文件
 *  5、返回成功消息
*/
extern bool ripple_xmanager_metricmsg_parsedrop(ripple_xmanager_metric* xmetric,
                                                 ripple_netpoolentry* npoolentry,
                                                 ripple_netpacket* npacket);


#endif
