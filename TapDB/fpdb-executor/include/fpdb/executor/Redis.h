//
// Created by czk on 30/01/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_REDIS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_REDIS_H

#include "iostream"
#include "cstring"
#include "string"
#include "cstdio"
#include <hiredis/hiredis.h>

namespace fpdb::executor {
    /*
    Control which ops can be pushdown.
    */
class Redis
{
public:
 
    Redis();

    ~Redis();

	//创建连接
	bool connect(std::string host, int port);

	//get请求
    bool get(std::string key);

	//set请求
	bool set(std::string key, std::string value);

	//密码认证
	bool auth();

	void del(std::string str);
 
private:
    redisContext* _connect;
	redisReply* _reply;		
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_REDIS_H
