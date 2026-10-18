#ifndef APPENDER_APP_HPP
#define APPENDER_APP_HPP

#include "appender_config.hpp"

namespace appender {

// Exit codes: 0 clean shutdown, 1 configuration/startup failure, 2 fatal runtime error
inline constexpr int kExitOk = 0;
inline constexpr int kExitStartup = 1;
inline constexpr int kExitFatal = 2;

// Wires the components together and runs until SIGINT/SIGTERM or a fatal
// error. SIGINT, SIGTERM and SIGUSR1 must already be blocked in the calling
// thread (so every thread inherits the mask) before run() is called.
int runAppender(const AppenderConfig& config);

}  // namespace appender

#endif  // APPENDER_APP_HPP
