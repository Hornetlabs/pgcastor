#ifndef _XMANAGER_METRICMSGCREATE_H_
#define _XMANAGER_METRICMSGCREATE_H_

/*
 * Handle create command
 *  1. jobtype must be less than ALL
 *  2. Verify if job already exists
 *  3. Add job to xmetric->metricnodes
 */
extern bool xmanager_metricmsg_parsecreate(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                           netpacket* npacket);

#endif
