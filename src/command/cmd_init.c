#include "ripple_app_incl.h"
#include "utils/init/ripple_init.h"
#include "command/ripple_cmd.h"

bool ripple_cmd_init(void *extra_config)
{
    RIPPLE_UNUSED(extra_config);
    return ripple_init();
}
