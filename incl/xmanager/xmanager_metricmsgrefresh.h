#ifndef _RIPPLE_XMANAGER_METRICMSGREFRESH_H_
#define _RIPPLE_XMANAGER_METRICMSGREFRESH_H_


/*
 * 处理 refresh 命令
 *  1、校验 job 是否存在, 不存在报错
 *  2、将 refresh 消息转发到 capture
 *  3、创建异步消息挂载到 xscsci 节点上
*/
bool ripple_xmanager_metricmsg_parserefresh(ripple_xmanager_metric* xmetric,
                                            ripple_netpoolentry* npoolentry,
                                            ripple_netpacket* npacket);

/*
 * 组装 refresh 返回消息
*/
bool ripple_xmanager_metricmsg_assemblerefresh(ripple_xmanager_metric* xmetric,
                                               ripple_netpoolentry* npoolentry,
                                               dlist* dlmsgs);


#endif
