#ifndef FDS_BLOCK_CONNECTORS_LOG_H_
#define FDS_BLOCK_CONNECTORS_LOG_H_

#include <iostream>
extern "C" {
#include <assert.h>
}

#define LOGTRACE    std::cerr << std::endl
#define LOGDEBUG    std::cerr << std::endl
#define LOGIO       std::cerr << std::endl
#define LOGNORMAL   std::cerr << std::endl
#define LOGWARN     std::cerr << std::endl
#define LOGERROR    std::cerr << std::endl
#define LOGNOTIFY   std::cerr << std::endl
#define LOGCRITICAL std::cerr << std::endl

#define GLOGTRACE       LOGTRACE
#define GLOGDEBUG       LOGDEBUG
#define GLOGIO          LOGIO
#define GLOGNORMAL      LOGNORMAL
#define GLOGWARN        LOGWARN
#define GLOGERROR       LOGERROR
#define GLOGNOTIFY      LOGNOTIFY
#define GLOGCRITICAL    LOGCRITICAL

#endif  // FDS_BLOCK_CONNECTORS_LOG_H_
