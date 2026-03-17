#ifndef _XMANAGER_METRICMSGREFRESH_H_
#define _XMANAGER_METRICMSGREFRESH_H_


/*
 * 处理 refresh 命令
 *  1、校验 job 是否存在, 不存在报错
 *  2、将 refresh 消息转发到 capture
 *  3、创建异步消息挂载到 xscsci 节点上
*/
bool xmanager_metricmsg_parserefresh(xmanager_metric* xmetric,
                                            netpoolentry* npoolentry,
                                            netpacket* npacket);

/*
 * 组装 refresh 返回消息
*/
bool xmanager_metricmsg_assemblerefresh(xmanager_metric* xmetric,
                                               netpoolentry* npoolentry,
                                               dlist* dlmsgs);


#endif
