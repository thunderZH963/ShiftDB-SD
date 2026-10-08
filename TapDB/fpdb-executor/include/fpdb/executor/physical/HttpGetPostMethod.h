#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HTTPGETPOSTMETHOD_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HTTPGETPOSTMETHOD_H

#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <netdb.h>
#include <sstream>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

class HttpGetPostMethod
{
    public:
        HttpGetPostMethod();
        virtual ~HttpGetPostMethod();
        // Http GET 请求
        int HttpGet(std::string host, std::string path, std::string get_content);
        // Http POST 请求
        int HttpPost(std::string host, std::string path, std::string post_content);
        std::string get_request_return();
        std::string get_main_text();
        int get_return_status_code();

    protected:

    private:
        // 记录请求返回的状态码
        int return_status_code_;
        // 记录请求返回所有数据
        std::string request_return_;
        // 记录请求返回的报文部分
        std::string main_text_;
        // HTTP请求过程中使用到的Socket通信部分
        std::string HttpSocket(std::string host, std::string request_str);
        // 将HTTP请求返回的数据分解
        void AnalyzeReturn(void);
};

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HTTPGETPOSTMETHOD_H