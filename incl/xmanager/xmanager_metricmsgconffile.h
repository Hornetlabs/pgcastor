#ifndef _XMANAGER_METRICMSGCONFFILE_H_
#define _XMANAGER_METRICMSGCONFFILE_H_

/*
 * Handle conffile command
 *  1. jobtype must be less than PROCESS
 *  2. Verify if job already exists
 *  3、Overwrite conf file
 */
bool xmanager_metricmsg_parseconffile(xmanager_metric* xmetric,
                                      netpoolentry*    npoolentry,
                                      netpacket*       npacket);

#endif
