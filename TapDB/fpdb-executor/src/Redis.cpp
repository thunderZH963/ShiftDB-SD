//
// Created by czk on 30/01/23.
//

#include <fpdb/executor/Redis.h>
#include <cstdlib>

namespace fpdb::executor {

Redis::Redis() {

}

Redis::~Redis()
{
    if (this->_connect != NULL) {
        redisFree(this->_connect);
    }
}

bool Redis::connect(std::string host, int port)
{
    this->_connect = redisConnect(host.c_str(), port);
    if(this->_connect == NULL || this->_connect->err)
    {
        return false;
    } else {
        return true;
    }
}

bool Redis::get(std::string key)
{
    redisReply *reply = (redisReply*)redisCommand(this->_connect, "get %s", key.c_str());
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR || reply->str == NULL) {
        freeReplyObject(reply);
        return false;
    } else {
        freeReplyObject(reply);
        return true;
    }
}

bool Redis::set(std::string key, std::string value)
{
    redisReply *reply = (redisReply *)redisCommand(this->_connect, "set %s %s", key.c_str(), value.c_str());
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        freeReplyObject(reply);
        return false;
    } else {
        freeReplyObject(reply);
        return true;
    }
}

bool Redis::auth() 
{
    const char *password = std::getenv("FPDB_REDIS_PASSWORD");
    if (password == nullptr || password[0] == '\0') return false;
    redisReply *reply = (redisReply *)redisCommand(this->_connect, "AUTH %s", password);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        freeReplyObject(reply);
        return false;
    } else {
        freeReplyObject(reply);
        return true;
    }
}

void Redis::del(std::string str) {
    redisReply *reply = (redisReply *)redisCommand(this->_connect, "del %s", str.c_str());
    freeReplyObject(reply);
}

}
