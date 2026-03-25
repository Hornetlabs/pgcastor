#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Asia_Kashgar[] = {
    (char*)"Rule	PRC	1986	only	-	May	 4	 2:00	1:00	D",
    (char*)"Rule	PRC	1986	1991	-	Sep	Sun>=11	 2:00	0	S",
    (char*)"Rule	PRC	1987	1991	-	Apr	Sun>=11	 2:00	1:00	D",
    (char*)"Zone	Asia/Urumqi	5:50:20	-	LMT	1928 # or Urumchi",
    (char*)"			6:00	-	URUT	1980 May # Urumqi Time",
    (char*)"			8:00	PRC	C%sT",
    (char*)"Link	Asia/Urumqi		Asia/Kashgar"};
