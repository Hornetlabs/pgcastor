#ifndef _TRAILPAGE_H_
#define _TRAILPAGE_H_

/* page verify */
bool trailpage_valid(mpage* mp);

/* will page splitas records */
bool trailpage_page2records(loadtrailrecords* ltrailrecords, mpage* mp);

#endif
