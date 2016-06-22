#ifndef _READCFG_H_
#define _READCFG_H_

#include "postgres_fe.h"
#include "c.h"

#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <string>

#include "ini.h"

using std::string;

#define uint64_t uint64

//void _tolower(char* buf);

bool trim(char* out, const char* in, const char* trimed = " \t\r\n");


class DataBuffer {
   public:
    DataBuffer(uint64_t size);
    ~DataBuffer();
    void reset() { length = 0; };

    uint64_t append(const char* buf, uint64_t len);  // ret < len means full
    const char* getdata() { return data; };
    uint64_t len() { return this->length; };
    bool full() { return maxsize == length; };
    bool empty() { return 0 == length; };

   private:
    const uint64_t maxsize;
    uint64_t length;
    // uint64_t offset;
    char* data;
};

class Config {
   public:
    Config(const string& filename);
    ~Config();
    string Get(const string& sec, const string& key,
               const string& defaultvalue);
    bool Scan(const string& sec, const string& key, const char* scanfmt,
              void* dst);
    void* Handle() { return (void*)this->_conf; };

   private:
    ini_t* _conf;
};

bool to_bool(std::string str);

void find_replace(string& str, const string& find, const string& replace);

#endif  // _UTILFUNCTIONS_
