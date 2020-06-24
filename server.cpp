#include "city.h"
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;
#define BUF_SIZE 4096
#define NUM_ENTRIES 800000
#define NUM_ALIASES 10000

pthread_mutex_t lock;
pthread_mutex_t cache_lock;
sem_t sem;

struct thread_info {
  int s = -1;
  pthread_cond_t cv;
};

struct block {
  char http_name[41];
  int block_num = -1;
  char buffer[BUF_SIZE];
  ssize_t readsize = 0;
};

struct key {
  uint8 object_name[20];
  uint32 block_num;
};

struct value {
  uint8 object_name[20];
  uint32 block_num = -1;
  uint32 block_length = -1;
  uint32 pointer = -1;
};

struct alias {
  char name[64] = "";
  char alias[64] = "";
};

list<block> LRU;
map<pair<char *, int>, block> cache;

int kvfd = -1;
int pointer_to_end;

int aliasfd = -1;

int name_resolver(char *v1) {
  int http_name_check = -1;
  int count = 0;
  string httpcheck = v1;
  size_t nothex = httpcheck.find_first_not_of("abcdefABCDEF1234567890");
  if (nothex == string::npos && strlen(v1) == 40) {
    http_name_check = 0;
  }
  while (http_name_check == -1 && count < 8) {
    //printf("%s\n", v1);
    struct alias v2;
    count++;
    int index = CityHash64(v1, strlen(v1))%NUM_ALIASES;
    pread(aliasfd, &v2, 128, index * 128);
    httpcheck = v2.name;
    nothex = httpcheck.find_first_not_of("abcdefABCDEF1234567890");
    if (nothex == string::npos && strlen(v2.name) == 40) {
      http_name_check = 0;
    }
    strcpy(v1, v2.name);
  }
  return http_name_check;
}

ssize_t kvinfo(const uint8_t *object_name, size_t length) {
  struct key k;
  struct value v;
  for (int i = 0; i < 20; i++) {
    k.object_name[i] = object_name[i];
  }
  k.block_num = 0;
  uint32 content_length;
  int index = CityHash32((char *)&k, sizeof(k)) % NUM_ENTRIES;
  pread(kvfd, &v, sizeof(v), index * sizeof(v));
  if (length == (size_t)-1) {
    if (v.pointer == (uint32)-1)
      return -2;
    pread(kvfd, &content_length, sizeof(int), v.pointer);
    return content_length;
  } else {
    if (v.pointer == (uint32)-1) {
      for (int i = 0; i < 20; i++) {
        v.object_name[i] = object_name[i];
      }
      v.block_num = 0;
      v.block_length = sizeof(int);
      v.pointer = pointer_to_end;
      pwrite(kvfd, &v, sizeof(v), index * sizeof(v));
      pointer_to_end += sizeof(int);
    }
    pwrite(kvfd, &length, sizeof(int), v.pointer);
    return length;
  }
}

ssize_t kvwrite(const uint8_t *object_name, size_t length, size_t offset,
                const uint8_t *data) {
  struct key k;
  struct value v;
  for (int i = 0; i < 20; i++) {
    k.object_name[i] = object_name[i];
  }
  k.block_num = (uint32)offset / BUF_SIZE + 1;
  int index = CityHash32((char *)&k, sizeof(k)) % NUM_ENTRIES;
  pread(kvfd, &v, sizeof(v), index * sizeof(v));
  v.block_length = length;
  if (v.pointer == (uint32)-1) {
    for (int i = 0; i < 20; i++) {
      v.object_name[i] = object_name[i];
    }
    v.block_num = k.block_num;
    //v.block_length = length;
    v.pointer = pointer_to_end;
    //pwrite(kvfd, &v, sizeof(v), index * sizeof(v));
    pointer_to_end += length;
  }
  pwrite(kvfd, &v, sizeof(v), index * sizeof(v));
  ssize_t wr = pwrite(kvfd, (char *)data, length,/*
                      NUM_ENTRIES * sizeof(v) + BUF_SIZE +*/ v.pointer);
  return wr;
}

ssize_t kvread(const uint8_t *object_name, size_t offset, const uint8_t *data) {
  if (kvinfo(object_name, -1) == -2)
    return -2;
  if (kvinfo(object_name, -1) < (ssize_t)offset)
    return -1;
  struct key k;
  struct value v;
  for (int i = 0; i < 20; i++) {
    k.object_name[i] = object_name[i];
  }
  k.block_num = (uint32)offset / BUF_SIZE + 1;
  int index = CityHash32((char *)&k, sizeof(k)) % NUM_ENTRIES;
  ssize_t inp = pread(kvfd, &v, sizeof(v), index * sizeof(v));
  inp = pread(kvfd, (char *)data, v.block_length,
              /*NUM_ENTRIES * sizeof(v) + BUF_SIZE + */v.pointer);
  return inp;
}

void write_cache(char *http_name, char *buf, ssize_t inp, int blnr,
                 uint8_t object_name[20]) {
  pthread_mutex_lock(&cache_lock);
  struct block temp = LRU.back();
  LRU.pop_back();
  if (temp.block_num != -1) {
    cache.erase(make_pair(temp.http_name, temp.block_num));
  }
  strcpy(temp.http_name, http_name);
  temp.http_name[40] = '\0';
  temp.block_num = blnr;
  strncpy(temp.buffer, buf, BUF_SIZE);
  temp.readsize = inp;
  cache.insert({make_pair(http_name, blnr), temp});
  LRU.push_front(temp);
  kvwrite(object_name, inp, BUF_SIZE * blnr, (uint8_t *)buf);
  pthread_mutex_unlock(&cache_lock);
}

ssize_t read_cache(char *http_name, char *buf, int blnr,
                   uint8_t object_name[20]) {
  pthread_mutex_lock(&cache_lock);
  struct block temp = LRU.back();
  LRU.pop_back();
  strcpy(temp.http_name, http_name);
  temp.block_num = blnr;
  map<pair<char *, int>, block>::iterator itr =
      cache.find(make_pair(http_name, blnr));
  if (itr != cache.end()) {
    strcpy(temp.buffer, itr->second.buffer);
    strcpy(buf, itr->second.buffer);
  } else {
    temp.readsize =
        kvread(object_name, blnr * BUF_SIZE, (uint8_t *)temp.buffer);
        kvread(object_name, blnr * BUF_SIZE, (uint8_t *)buf);
  }
  //strncpy(buf, temp.buffer, temp.readsize);
  LRU.push_front(temp);
  pthread_mutex_unlock(&cache_lock);
  return temp.readsize;
}

void work_fn(int cl) {
  char buf[BUF_SIZE] = "";
  string response = "HTTP/1.1 ";
  ssize_t inp = recv(cl, buf, BUF_SIZE, 0);
  if (inp < 0) {
    fprintf(stderr, "recv failed: %s\n", strerror(errno));
    close(cl);
    return;
  }
  char *action = (char *)malloc(sizeof(char) * BUF_SIZE);
  char *httpname = (char *)malloc(sizeof(char) * BUF_SIZE);
  char *http_name = (char *)malloc(sizeof(char) * BUF_SIZE);
  sscanf(buf, "%s %s", action, httpname);
  if (httpname[0] == '/') {
    http_name = httpname + 1;
  } else {
    http_name = httpname;
  }
  uint8 object_name[20];
  if (strcmp(action, "PATCH") != 0) {
    int httpcheck = name_resolver(http_name);
    if (httpcheck != 0) {
      response += "400 Bad Request\r\n\r\n";
      inp = send(cl, response.c_str(), response.length(), 0);
      if (inp < 0) {
        fprintf(stderr, "send failed: %s\n", strerror(errno));
        close(cl);
        return;
      }
      close(cl);
      return;
    }
    for (int i = 0; i < 40; i++) {
      if (isalpha(http_name[i]))
        http_name[i] = tolower(http_name[i]);
    }
    string http_name_str = http_name;
    for (int i = 0; i < (int)http_name_str.length(); i += 2) {
      string byte = http_name_str.substr(i, 2);
      object_name[(int)i / 2] = (uint8)(int)strtol(byte.c_str(), NULL, 16);
    }
  }
  // Code for patch
  if (strcmp(action, "PATCH") == 0) {
    char *res = strstr(buf, "ALIAS");
    res += 6;
    char *space = strstr(res, " ");
    int existing_name_length = space - res;
    char existing_name[64];
    strncpy(existing_name, res, existing_name_length);
    existing_name[existing_name_length] = '\0';
    res += existing_name_length + 1;
    space = strstr(res, "\r\n");
    int new_name_length = space - res;
    char new_name[64];
    strncpy(new_name, res, new_name_length);
    struct alias v2;
    strcpy(v2.name, existing_name);
    strcpy(v2.alias, new_name);
    int index = CityHash64(new_name, strlen(new_name))%NUM_ALIASES;
    pthread_mutex_lock(&lock);
    inp = pwrite(aliasfd, &v2, sizeof(v2), index * 128);
    if (inp < 0) {
      fprintf(stderr, "pwrite failed: %s\n", strerror(errno));
      close(cl);
      return;
    }
    response += "201 Created\r\n\r\n";
    inp = send(cl, response.c_str(), response.length(), 0);
    if (inp < 0) {
      fprintf(stderr, "send failed: %s\n", strerror(errno));
      close(cl);
      return;
    }
    pthread_mutex_unlock(&lock);
    memset(buf, 0, BUF_SIZE);
    close(cl);
  }
  // Code for put
  if (strcmp(action, "PUT") == 0) {
    if (kvinfo(object_name, -1) == -2) {
      response += "201 Created\r\n\r\n";
    } else {
      response += "200 OK\r\n\r\n";
    }
    char *res = strstr(buf, ": ");
    res += 2;
    int tempind = res - buf;
    char *res2 = strstr(res, "\r\n\r\n");
    int tempind2 = res2 - buf;
    char contlen[tempind2 - tempind + 1];
    strncpy(contlen, res, tempind2 - tempind);
    contlen[tempind2 - tempind] = '\0';
    uint32 content_length = atoi(contlen);
    int blnr = 0;
    int temp_content_length = kvinfo(object_name, content_length);
    ssize_t total_inp = 0;//(BUF_SIZE - tempind - 4);
    while (total_inp < temp_content_length) {
      inp = recv(cl, buf, BUF_SIZE, 0);
      if (inp < 0) {
        fprintf(stderr, "recv failed: %s\n", strerror(errno));
        close(cl);
        return;
      }
      total_inp += inp;
      write_cache(http_name, buf, inp, blnr, object_name);
      blnr++;
    }
    pthread_mutex_lock(&lock);
    inp = send(cl, response.c_str(), response.length(), 0);
    if (inp < 0) {
      fprintf(stderr, "send failed: %s\n", strerror(errno));
      close(cl);
      return;
    }
    pthread_mutex_unlock(&lock);
    memset(buf, 0, BUF_SIZE);
    close(cl);
  }
  // Code for get
  if (strcmp(action, "GET") == 0) {
    if (kvinfo(object_name, -1) == -2) {
      response += "404 Not Found\r\n\r\n";
      inp = send(cl, response.c_str(), response.length(), 0);
      close(cl);
      return;
    }
    response += "200 OK\r\n";
    int content_length = kvinfo(object_name, -1);
    response += "Content-Length: " + to_string(content_length) + "\r\n\r\n";
    inp = send(cl, response.c_str(), response.length(), 0);
    if (inp < 0) {
      fprintf(stderr, "send failed: %s\n", strerror(errno));
      close(cl);
      return;
    }
    if (response.substr(9, 3).compare("200") != 0) {
      close(cl);
      return;
    }
    int blnr = 0;
    ssize_t bytes_read = 0;
    while (bytes_read < content_length) {
      ssize_t filesize = read_cache(http_name, buf, blnr, object_name);
      if(filesize == -1){
        fprintf(stderr, "read failed: %s\n", strerror(errno));
        close(cl);
        return;
      }
      bytes_read += filesize;
      blnr++;
      pthread_mutex_lock(&lock);
      inp = send(cl, buf, filesize, 0);
      if (inp < 0) {
        fprintf(stderr, "send failed: %s\n", strerror(errno));
        close(cl);
        return;
      }
      pthread_mutex_unlock(&lock);
    }
    memset(buf, 0, BUF_SIZE);
    close(cl);
  }
}

void *start_fn(void *arg) {
  struct thread_info *ti = (thread_info *)arg;
  for (;;) {
    pthread_mutex_lock(&lock);
    if (ti->s == -1) {
      pthread_cond_wait(&ti->cv, &lock);
      pthread_mutex_unlock(&lock);
      continue;
    }
    int cl = ti->s;
    ti->s = -1;
    pthread_mutex_unlock(&lock);
    work_fn(cl);
    sem_post(&sem);
  }
}

int main(int argc, char *argv[]) {
  string address;
  int parse = 0;
  int NUM_THREADS = 4;
  int NUM_BLOCKS = 40;
  while ((parse = getopt(argc, argv, "N:c:f:m:")) != -1) {
    switch (parse) {
    case 'N':
      NUM_THREADS = atoi(optarg);
      break;
    case 'c':
      NUM_BLOCKS = atoi(optarg);
      break;
    case 'f':
      kvfd = open(optarg, O_CREAT | O_RDWR, S_IRWXU);
      break;
    case 'm':
      aliasfd = open(optarg, O_CREAT | O_RDWR, S_IRWXU);
      break;
    }
  }
  if (kvfd < 0) {
    fprintf(stderr, "kvs open failed\n");
    return EXIT_FAILURE;
  }
  if (aliasfd < 0) {
    fprintf(stderr, "alias file open failed\n");
    return EXIT_FAILURE;
  }
  address = argv[argc - 1];
  size_t pos = address.find(":");
  if ((int)pos == -1) {
    fprintf(stderr, "no colon before address\n");
    return EXIT_FAILURE;
  }
  if (pos == 0) {
    fprintf(stderr, "no address name\n");
    return EXIT_FAILURE;
  }
  string SERVER_NAME_STRING = address.substr(0, pos);
  string temp_port = address.substr(pos + 1);
  int PORT_NUMBER = 80;
  if (pos != address.length() - 1) {
    PORT_NUMBER = stoi(temp_port);
  }
  // Start of section code
  struct hostent *hent = gethostbyname(SERVER_NAME_STRING.c_str());
  if (hent == NULL) {
    fprintf(stderr, "gethostbyname failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  struct sockaddr_in addr;
  memcpy(&addr.sin_addr.s_addr, hent->h_addr, hent->h_length);
  addr.sin_port = htons(PORT_NUMBER);
  addr.sin_family = AF_INET;
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    fprintf(stderr, "socket failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  int enable = 1;
  int sockopr =
      setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
  if (sockopr == -1) {
    fprintf(stderr, "setsockopt failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  int bin = bind(sock, (struct sockaddr *)&addr, sizeof(addr));
  if (bin == -1) {
    fprintf(stderr, "bind failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  int liste = listen(sock, 0);
  if (liste == -1) {
    fprintf(stderr, "listen failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  // End of section code
  pthread_mutex_init(&lock, NULL);
  pthread_mutex_init(&cache_lock, NULL);
  pthread_t thread[NUM_THREADS];
  struct thread_info tinfo[NUM_THREADS];
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_cond_init(&tinfo[i].cv, NULL);
    pthread_create(&thread[i], NULL, start_fn, &tinfo[i]);
  }
  sem_init(&sem, 0, NUM_THREADS);
  block blk[NUM_BLOCKS];
  for (int i = 0; i < NUM_BLOCKS; i++) {
    LRU.push_back(blk[i]);
  }
  struct stat st;
  fstat(kvfd, &st);
  if (st.st_size == 0) {
    pointer_to_end = NUM_ENTRIES * sizeof(struct value);
    for (int i = 0; i < NUM_ENTRIES; i++) {
      struct value v;
      pwrite(kvfd, &v, sizeof(v), i * sizeof(v));
    }
  } else
    pointer_to_end = st.st_size;
  for (;;) {
    for (int i = 0; i < NUM_THREADS; i++) {
      if (tinfo[i].s == -1) {
        int cl = accept(sock, NULL, NULL);
        if (cl == -1) {
          fprintf(stderr, "accept failed: %s\n", strerror(errno));
          close(cl);
        }
        pthread_mutex_lock(&lock);
        tinfo[i].s = cl;
        pthread_cond_signal(&tinfo[i].cv);
        pthread_mutex_unlock(&lock);
        sem_wait(&sem);
      }
    }
  }
  close(sock);
}
