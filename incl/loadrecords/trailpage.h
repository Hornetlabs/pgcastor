#ifndef _TRAILPAGE_H_
#define _TRAILPAGE_H_

/* page 校验 */
bool trailpage_valid(mpage* mp);


/* 将 page 拆分为 records */
bool trailpage_page2records(loadtrailrecords* ltrailrecords, mpage* mp);

#endif
