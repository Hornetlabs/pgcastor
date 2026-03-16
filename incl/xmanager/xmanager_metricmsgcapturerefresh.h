#ifndef _RIPPLE_XMANAGER_METRICMSGCAPTUREREFRESH_H_
#define _RIPPLE_XMANAGER_METRICMSGCAPTUREREFRESH_H_

/*
 * 处理 capture refresh 命令
*/
extern bool ripple_xmanager_metricmsg_parsecapturerefresh(ripple_xmanager_metric* xmetric,
                                                          ripple_netpoolentry* npoolentry,
                                                          ripple_netpacket* npacket);


#endif
