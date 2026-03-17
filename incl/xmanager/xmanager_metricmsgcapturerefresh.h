#ifndef _XMANAGER_METRICMSGCAPTUREREFRESH_H_
#define _XMANAGER_METRICMSGCAPTUREREFRESH_H_

/*
 * 处理 capture refresh 命令
*/
extern bool xmanager_metricmsg_parsecapturerefresh(xmanager_metric* xmetric,
                                                          netpoolentry* npoolentry,
                                                          netpacket* npacket);


#endif
