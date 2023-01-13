// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#ifdef _WIN32
#include <intrin.h>
#endif

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>

#include <azure/core/datetime.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/core/internal/json/json.hpp>
#include <azure/storage/blobs.hpp>

const std::string g_versionString = "0.0.2";

uint64_t randInt(uint64_t offset)
{
#ifdef _WIN32
  uint64_t high_part = 0x12e15e35b500f16e;
  uint64_t low_part = 0x2e714eb2b37916a5;
  return __umulh(low_part, offset) + high_part * offset;
#else
  using uint128_t = __uint128_t;
  constexpr uint128_t mult = static_cast<uint128_t>(0x12e15e35b500f16e) << 64 | 0x2e714eb2b37916a5;
  uint128_t product = offset * mult;
  return product >> 64;
#endif
}

void fillBuffer(uint8_t* buffer, size_t size)
{
  constexpr size_t int_size = sizeof(uint64_t);

  while (size >= int_size)
  {
    *(reinterpret_cast<uint64_t*>(buffer)) = randInt(size);
    size -= int_size;
    buffer += int_size;
  }
  if (size)
  {
    uint64_t r = randInt(size);
    std::memcpy(buffer, &r, size);
  }
}

enum class LogLevel
{
  DEBUG,
  INFO,
  ERROR,
};

constexpr LogLevel ERROR = LogLevel::ERROR;
constexpr LogLevel INFO = LogLevel::INFO;
constexpr LogLevel DEBUG = LogLevel::DEBUG;

LogLevel g_filterLevel = LogLevel::INFO;

void log(LogLevel level, const std::string& content)
{
  if (level < g_filterLevel)
  {
    return;
  }
  std::string levelStr;
  if (level == LogLevel::ERROR)
  {
    levelStr = "ERROR";
  }
  else if (level == LogLevel::INFO)
  {
    levelStr = "INFO";
  }
  else if (level == LogLevel::DEBUG)
  {
    levelStr = "DEBUG";
  }
  auto now = Azure::DateTime(std::chrono::system_clock::now());
  std::string msg = "["
      + now.ToString(
          Azure::DateTime::DateFormat::Rfc3339, Azure::DateTime::TimeFractionFormat::AllDigits)
      + "] [" + levelStr + "] " + content + "\n";
  std::cout << msg << std::flush;
}

namespace Azure { namespace Storage { namespace _detail {
    void DisableReliableStream();
}}} // namespace Azure::Storage::_detail

int main(int argc, char** argv)
{
  using namespace Azure::Storage::Blobs;
  Azure::Storage::_detail::DisableReliableStream();

  if (argc != 2)
  {
    std::cout << (std::string("Usage: ") + argv[0] + " [config.json]") << std::endl;
    return 1;
  }
  Azure::Core::Json::_internal::json config;
  {
    std::ifstream file(argv[1]);
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    config = Azure::Core::Json::_internal::json::parse(content.begin(), content.end());
  }

  const std::string connectionString = config["connection_string"];
  const std::string containerName = config["container_name"];
  const std::string operation = config["operation"];
  const std::string blobName = operation == "upload" ? "b2" : "b1";
  const int concurrency = config["concurrency"];
  const size_t transferSize = config["transfer_size"];
  const size_t readChunkSize = 1 * 1024 * 1024;
  const bool httpKeepAlive = config["http_keep_alive"];
  const int64_t maxRuntimeInSeconds = config["stop_after_seconds"];
  const int64_t maxNumRequests = config["stop_after_number_requests"];
  const std::vector<std::string> terminateKeywords = config["stop_on_keywords"];
  log(INFO, "version: " + g_versionString);
  if (operation != "upload" && operation != "download")
  {
    log(ERROR, "Invalid operation: " + operation);
    return 1;
  }
  else
  {
    log(INFO, "operation: " + operation);
  }
  log(INFO, "Concurrency: " + std::to_string(concurrency));
  log(INFO, "Transfer size for each request: " + std::to_string(transferSize));
  log(INFO, std::string("HTTP keep-alive: ") + (httpKeepAlive ? "true" : "false"));
  if (maxRuntimeInSeconds >= 0)
  {
    log(INFO, "Stop after running for " + std::to_string(maxRuntimeInSeconds) + " seconds");
  }
  if (maxNumRequests >= 0)
  {
    log(INFO, "Stop after sending " + std::to_string(maxNumRequests) + " requests");
  }
  for (const auto& i : terminateKeywords)
  {
    log(INFO, "Stop on keyword " + i);
  }
  const std::string logLevel = config["log_level"];
  if (logLevel == "debug" || logLevel == "DEBUG")
  {
    log(INFO, "Switching log level to DEBUG");
    g_filterLevel = DEBUG;
  }
  else if (logLevel == "info" || logLevel == "INFO")
  {
    log(INFO, "Switching log level to INFO");
    g_filterLevel = INFO;
  }
  else if (logLevel == "error" || logLevel == "ERROR")
  {
    log(INFO, "Switching log level to ERROR");
    g_filterLevel = ERROR;
  }
  else
  {
    log(ERROR, "Invalid log level: " + logLevel);
  }

  auto containerClient
      = BlobContainerClient::CreateFromConnectionString(connectionString, containerName);
  containerClient.CreateIfNotExists();

  BlockBlobClient blobClient = containerClient.GetBlockBlobClient(blobName);
  if (operation == "download")
  {
    bool createBlob = true;
    try
    {
      auto blobSize = blobClient.GetProperties().Value.BlobSize;
      if (blobSize >= static_cast<int64_t>(transferSize))
      {
        createBlob = false;
        log(INFO, "Use existing blob size " + std::to_string(blobSize));
      }
    }
    catch (Azure::Storage::StorageException&)
    {
    }
    if (createBlob)
    {
      std::vector<uint8_t> buffer;
      buffer.resize(transferSize);
      fillBuffer(buffer.data(), buffer.size());
      UploadBlockBlobFromOptions options;
      options.TransferOptions.Concurrency = 32;
      blobClient.UploadFrom(buffer.data(), buffer.size(), options);
      log(INFO, "Create new blob");
    }
  }

  std::atomic<int64_t> requestCounter{0};
  const auto startTime = std::chrono::steady_clock::now();

  BlobClientOptions clientOptions;
  clientOptions.Retry.MaxRetries = 0;
  Azure::Core::Http::CurlTransportOptions curlTransportOptions;
  if (!httpKeepAlive)
  {
    curlTransportOptions.HttpKeepAlive = false;
  }
  clientOptions.Transport.Transport
      = std::make_shared<Azure::Core::Http::CurlTransport>(curlTransportOptions);
  containerClient = BlobContainerClient::CreateFromConnectionString(
      connectionString, containerName, clientOptions);
  blobClient = containerClient.GetBlockBlobClient(blobName);

  std::vector<std::thread> threadHandles;

  auto shouldThrow = [&terminateKeywords](const std::string& message) -> bool {
    for (const auto& i : terminateKeywords)
    {
      if (message.find(i) != std::string::npos)
      {
        return true;
      }
    }
    return false;
  };

  auto shouldStop = [&]() -> bool {
    if (maxNumRequests > 0)
    {
      int64_t numRequests = requestCounter.fetch_add(1, std::memory_order_relaxed) + 1;
      if (numRequests > maxNumRequests)
      {
        return true;
      }
    }
    if (maxRuntimeInSeconds > 0)
    {
      auto numMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - startTime)
                       .count();
      if (numMs > maxRuntimeInSeconds * 1000)
      {
        return true;
      }
    }
    return false;
  };

  std::vector<uint8_t> uploadBuffer;
  if (operation == "upload")
  {
    uploadBuffer.resize(transferSize);
    fillBuffer(&uploadBuffer[0], uploadBuffer.size());
  }

  auto uploadFunc = [&](int id) {
    Azure::Core::IO::MemoryBodyStream bodyStream(uploadBuffer.data(), uploadBuffer.size());
    auto blobClient2 = containerClient.GetBlockBlobClient(blobName + "-" + std::to_string(id));
    try
    {
      auto response = blobClient2.Upload(bodyStream);
      auto requestId = response.RawResponse->GetHeaders().at("x-ms-request-id");
      log(DEBUG, requestId + " successful");
    }
    catch (Azure::Storage::StorageException& e)
    {
      auto requestId = e.RequestId;
      log(ERROR, requestId + " Failed when sending request/getting response, " + e.what());
      if (shouldThrow(e.what()))
      {
        throw;
      }
    }
    catch (std::exception& e)
    {
      log(ERROR, std::string("Failed when sending request/getting response, ") + e.what());
      if (shouldThrow(e.what()))
      {
        throw;
      }
      return;
    }
  };

  auto downloadFunc = [&]() {
    std::vector<uint8_t> buffer;
    buffer.resize(readChunkSize);

    DownloadBlobOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = 0;
    options.Range.Value().Length = transferSize;
    Models::DownloadBlobResult result;
    std::string requestId;
    try
    {
      auto response = blobClient.Download(options);
      requestId = response.RawResponse->GetHeaders().at("x-ms-request-id");
      result = std::move(response.Value);
    }
    catch (Azure::Storage::StorageException& e)
    {
      requestId = e.RequestId;
      log(ERROR, requestId + " Failed when sending request/getting response, " + e.what());
      if (shouldThrow(e.what()))
      {
        throw;
      }
      return;
    }
    catch (std::exception& e)
    {
      log(ERROR, std::string("Failed when sending request/getting response, ") + e.what());
      if (shouldThrow(e.what()))
      {
        throw;
      }
      return;
    }

    while (true)
    {
      try
      {
        size_t actualRead = result.BodyStream->ReadToCount(buffer.data(), readChunkSize);
        if (actualRead == 0)
        {
          log(DEBUG, requestId + " successful");
          break;
        }
      }
      catch (std::exception& e)
      {
        log(ERROR, requestId + " Failed when reading body stream, " + e.what());
        if (shouldThrow(e.what()))
        {
          throw;
        }
        break;
      }
    }
  };

  for (int i = 0; i < concurrency; ++i)
  {
    threadHandles.push_back(std::thread([&shouldStop, &operation, &uploadFunc, &downloadFunc, i]() {
      while (true)
      {
        if (shouldStop())
        {
          break;
        }

        if (operation == "upload")
        {
          uploadFunc(i);
        }
        else if (operation == "download")
        {
          downloadFunc();
        }
      }
    }));
  }

  for (int i = 0; i < concurrency; ++i)
  {
    threadHandles[i].join();
  }

  return 0;
}