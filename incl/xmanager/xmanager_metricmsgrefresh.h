#ifndef _XMANAGER_METRICMSGREFRESH_H_
#define _XMANAGER_METRICMSGREFRESH_H_

/*
 * Handle refresh command
 *  1、Verify if job exists, report error if not
 *  2. Forward refresh message to capture
 *  3. Create async message and mount to xscsci node
 */
bool xmanager_metricmsg_parserefresh(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket);

/*
 * Assemble refresh return message
 */
bool xmanager_metricmsg_assemblerefresh(xmanager_metric* xmetric, netpoolentry* npoolentry, dlist* dlmsgs);

#endif
