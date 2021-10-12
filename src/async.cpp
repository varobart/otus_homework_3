#include "async.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {
class Logger {
public:
  Logger();
  ~Logger();

private:
  static void console_logger();
  static void file_logger();

  static std::atomic<bool> s_bStopWorkers;
  static std::array<std::thread, 3> g_workerThreads;

public:
  static std::condition_variable g_con_cv;
  static std::mutex g_consoleQueueMtx;
  static std::queue<std::string> g_consoleLogQueue;

  static std::condition_variable g_file_cv;
  static std::mutex g_fileQueueMtx;
  static std::queue<std::pair<std::string, std::string>> g_fileLogQueue;
};

std::atomic<bool> Logger::s_bStopWorkers = false;
std::array<std::thread, 3> Logger::g_workerThreads = {
    std::thread(), std::thread(), std::thread()};
std::condition_variable Logger::g_con_cv = {};
std::mutex Logger::g_consoleQueueMtx = {};
std::queue<std::string> Logger::g_consoleLogQueue = {};
std::condition_variable Logger::g_file_cv = {};
std::mutex Logger::g_fileQueueMtx = {};
std::queue<std::pair<std::string, std::string>> Logger::g_fileLogQueue = {};

Logger::Logger() {
  std::array<void (*)(), 3> aLoggerFuncs = {console_logger, file_logger,
                                            file_logger};

  for (int idx = 0; idx < 3; ++idx) {
    std::thread t(aLoggerFuncs[idx]);
    g_workerThreads[idx] = std::move(t);
  }
}

Logger::~Logger() {
  Logger::s_bStopWorkers = true;
  for (auto &thr : g_workerThreads)
    thr.join();
}

// use here producer/consumer pattern
void Logger::console_logger() {
  while (true) {
    std::string logStr;
    {
      std::unique_lock lock(g_consoleQueueMtx);
      g_con_cv.wait(
          lock, []() { return !g_consoleLogQueue.empty() || s_bStopWorkers; });

      if (g_consoleLogQueue.empty() && s_bStopWorkers)
        break;

      logStr = std::move(g_consoleLogQueue.front());
      g_consoleLogQueue.pop();
    }

    std::cout << logStr << std::endl;
  }
}

// use here producer/consumer pattern
void Logger::file_logger() {
  while (true) {
    std::pair<std::string, std::string> nameLogPair;
    {
      std::unique_lock lock(g_fileQueueMtx);
      g_file_cv.wait(
          lock, []() { return !g_fileLogQueue.empty() || s_bStopWorkers; });

      if (g_fileLogQueue.empty() && s_bStopWorkers)
        break;

      nameLogPair = std::move(g_fileLogQueue.front());
      g_fileLogQueue.pop();
    }

    std::ofstream file(nameLogPair.first);
    file << nameLogPair.second << std::endl;
  }
}

Logger g_logger;

class Bulk {
public:
  Bulk(size_t buffer_size);
  ~Bulk() = default;

  void process(const std::string &cmd);
  void flush();

private:
  void log();

  std::stringstream m_cmds;

  size_t m_buffer_size = 0;
  size_t m_nCmds = 0;
  std::time_t m_time;
  int m_nestingLvl = 0;
};

Bulk::Bulk(size_t buffer_size)
    : m_time(std::time(nullptr)), m_buffer_size(buffer_size) {
  m_cmds << "bulk:";
}

void Bulk::log() {
  static std::atomic<unsigned int> postfix_counter = 0;

  if (m_nCmds > 0) {
    std::string timeStr = std::to_string(static_cast<long int>(m_time));
    std::string filename = "bulk" + timeStr + "_" +
                           std::to_string(postfix_counter.load()) + ".log";
    ++postfix_counter;

    {
      std::unique_lock lock(Logger::g_fileQueueMtx);
      Logger::g_fileLogQueue.push(std::make_pair(filename, m_cmds.str()));
    }

    Logger::g_con_cv.notify_one();

    {
      std::unique_lock lock(Logger::g_consoleQueueMtx);
      Logger::g_consoleLogQueue.push(m_cmds.str());
    }

    Logger::g_file_cv.notify_one();
  }

  m_nestingLvl = 0;
  m_nCmds = 0;
  m_time = std::time(nullptr);
  m_cmds = std::stringstream();
  m_cmds << "bulk:";
}

void Bulk::flush() {
  if (m_nestingLvl == 0)

    log();
}

void Bulk::process(const std::string &cmd) {
  if (cmd == "{") {
    if (m_nestingLvl == 0)
      flush();

    ++m_nestingLvl;
  } else if (cmd == "}") {
    --m_nestingLvl;
    if (m_nestingLvl == 0)
      flush();
  } else {
    if (m_nCmds > 0)
      m_cmds << ",";
    else
      m_time = std::time(nullptr);

    m_cmds << " " << cmd;
    ++m_nCmds;

    if (m_nCmds == m_buffer_size && m_nestingLvl == 0)
      flush();
  }
}
} // anonymous namespace

namespace async {
handle_t connect(std::size_t bulk) {
  Bulk *pBulk = new Bulk(bulk);
  return static_cast<void *>(pBulk);
}

void disconnect(handle_t handle) {
  auto pBulk = static_cast<Bulk *>(handle);
  if (!pBulk)
    return;

  pBulk->flush();
  delete pBulk;
  pBulk = nullptr;
}

void receive(handle_t handle, const char *data, std::size_t size) {
  auto pBulk = static_cast<Bulk *>(handle);
  if (!pBulk)
    return;

  std::string str(data, size);
  std::stringstream stream(str);

  std::string cmd;
  while (std::getline(stream, cmd))
    if (!cmd.empty())
      pBulk->process(cmd);
}
} // namespace async
