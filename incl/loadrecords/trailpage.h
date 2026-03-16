#ifndef _RIPPLE_TRAILPAGE_H_
#define _RIPPLE_TRAILPAGE_H_

/* page 校验 */
bool ripple_trailpage_valid(mpage* mp);


/* 将 page 拆分为 records */
bool ripple_trailpage_page2records(ripple_loadtrailrecords* ltrailrecords, mpage* mp);

#endif
