#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/path/ripple_path.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "xsynch_exbufferdata.h"
#include "xsynch_fe.h"

int main(int argc, char** argv)
{
    char connstr[]= "host=127.0.0.1 port=6543 protocol=TCP";
    xsynchconn* xconn = NULL;

    xconn = XSynchSetParam(NULL);

    XSynchConn(xconn);
    while(1)
    {
        sleep(1);
    }

    return 0;
}
