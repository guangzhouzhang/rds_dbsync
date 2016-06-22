
#include "postgres_fe.h"
#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>

#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <time.h>

#include "readcfg.h"
#include <iostream>

using namespace std;

using std::string;


/*
void _tolower(char *buf) {
    do {
        if (*buf >= 'A' && *buf <= 'Z') *buf |= 0x60;
    } while (*buf++);
    return;
}
*/

bool trim(char *out, const char *in, const char *trimed) {
    int targetlen;

    if (!out || !in) {  // invalid string params
        return false;
    }

    targetlen = strlen(in);

    while (targetlen > 0) {
        if (strchr(trimed, in[targetlen - 1]) ==
            NULL)  // can't find stripped char
            break;
        else
            targetlen--;
    }

    while (targetlen > 0) {
        if (strchr(trimed, *in) == NULL)  // normal string
            break;
        else {
            in++;
            targetlen--;
        }
    }

    memcpy(out, in, targetlen);
    out[targetlen] = 0;
    return true;
}

DataBuffer::DataBuffer(uint64_t size) : maxsize(size), length(0) {
    this->data = (char *)malloc(this->maxsize);
}

DataBuffer::~DataBuffer() {
    if (this->data) {
        free(this->data);
    }
}

uint64_t DataBuffer::append(const char *buf, uint64_t len) {
    uint64_t copylen = min(len, maxsize - length);
    if (this->data) {
        memcpy(this->data + length, buf, copylen);
        this->length += copylen;
        return copylen;
    } else {
        return 0;
    }
}

Config::Config(const string &filename) : _conf(NULL) {
    if (filename != "") this->_conf = ini_load(filename.c_str());
    if (this->_conf == NULL) {
        //fprintf(stderr,"Failed to load config file\n");
		cout<<"Failed to load config file\n"<<endl;
    }
}

Config::~Config() {
    if (this->_conf) ini_free(this->_conf);
}

string Config::Get(const string &sec, const string &key,
                   const string &defaultvalue) {
    string ret = defaultvalue;
    if ((key == "") || (sec == "")) return ret;

    if (this->_conf) {
        const char *tmp = ini_get(this->_conf, sec.c_str(), key.c_str());
        if (tmp) ret = tmp;
    }
    return ret;
}

bool Config::Scan(const string &sec, const string &key, const char *scanfmt,
                  void *dst) {
    if ((key == "") || (sec == "")) return false;

    if (this->_conf) {
        return ini_sget(this->_conf, sec.c_str(), key.c_str(), scanfmt, dst);
    }
    return false;
}

bool to_bool(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    if ((str == "yes") || (str == "true") || (str == "y") || (str == "t") ||
        (str == "1")) {
        return true;
    } else {
        return false;
    }
}

void find_replace(string &str, const string &find, const string &replace) {
    if (find.empty()) return;

    size_t pos = 0;

    while ((pos = str.find(find, pos)) != string::npos) {
        str.replace(pos, find.length(), replace);
        pos += replace.length();
    }
}

void *
init_config(char *cfgpath)
{
	Config* s3cfg = NULL;

    s3cfg = new Config(cfgpath);
    if (!s3cfg || !s3cfg->Handle()) 
	{
        if (s3cfg) 
		{
            delete s3cfg;
            s3cfg = NULL;
        }
        return NULL;
    }

	return s3cfg;
}

int
get_config(void *cfg, char *sec, char* key, char **value)
{
	Config* s3cfg = (Config *)cfg;
	string	ssec;
	string	skey;
	string	rc;

	if (sec == NULL || key == NULL)
		return 1;

	ssec = string(sec);
	skey = string(key);
	rc = s3cfg->Get(ssec, skey, "");
	if (rc == "")
		return 1;

	*value = strdup(rc.c_str());

	return 0;
}

