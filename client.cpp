#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define BUF_SIZE 4096

int main(int argc, char *argv[]) {
  int i = 2;
  if (argc < 2) {
    fprintf(stderr, "incorrect number of arguments\n");
    return EXIT_FAILURE;
  }
  std::string address = argv[1];
  std::size_t pos = address.find(":");
  if ((int)pos == -1) {
    fprintf(stderr, "no colon before address\n");
    return EXIT_FAILURE;
  }
  if (pos == 0) {
    fprintf(stderr, "no address name\n");
    return EXIT_FAILURE;
  }
  std::string SERVER_NAME_STRING = address.substr(0, pos);
  std::string temp_port = address.substr(pos + 1);
  int PORT_NUMBER = 80;
  if (pos != address.length() - 1) {
    PORT_NUMBER = std::stoi(temp_port);
  }
  // This code is from Daniel Bittman in section
  struct hostent *hent = gethostbyname(SERVER_NAME_STRING.c_str());
  if (hent == NULL) {
    fprintf(stderr, "gethostbyname failed: %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  struct sockaddr_in addr;
  memcpy(&addr.sin_addr.s_addr, hent->h_addr, hent->h_length);
  addr.sin_port = htons(PORT_NUMBER);
  addr.sin_family = AF_INET;
  // End of section code
  while (i < argc) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
      fprintf(stderr, "socket failed: %s\n", strerror(errno));
      return EXIT_FAILURE;
    }
    i++;
    std::string argument = argv[i - 1];
    std::string action = argument.substr(0, 1);
    int colon1 = argument.find(":");
    if (colon1 != 1) {
      fprintf(stderr, "request %d: first colon not in correct location\n",
              i - 2);
      close(sock);
      continue;
    }
    if (argument.length() == 2) {
      close(sock);
      continue;
    }
    int colon2 = argument.find(":", 2);
    if (colon2 == -1) {
      fprintf(stderr, "request %d: missing second colon\n", i - 2);
      close(sock);
      continue;
    }
    char buf[BUF_SIZE + 1] = "";
    int con = connect(sock, (struct sockaddr *)&addr, sizeof(addr));
    if (con == -1) {
      fprintf(stderr, "request %d: connect failed: %s\n", i - 2,
              strerror(errno));
      close(sock);
      continue;
    }
    if (action.compare("r") != 0 && action.compare("s") != 0 &&
        action.compare("p")) {
      fprintf(stderr, "request %d: not valid command\n", i - 2);
      close(sock);
      continue;
    }
    // Code for get
    if (action.compare("r") == 0) {
      std::string http_name = argument.substr(2, colon2 - colon1 - 1);
      std::string file_name =
          argument.substr(colon2 + 1, argument.length() - colon2 - 1);
      if (file_name.length() == 0) {
        fprintf(stderr, "request %d: no local file name\n", i - 2);
        close(sock);
        continue;
      }
      std::string getreq = "GET " + http_name + " HTTP/1.1\r\n\r\n";
      ssize_t inp = send(sock, getreq.c_str(), strlen(getreq.c_str()), 0);
      if ((int)inp < 0) {
        fprintf(stderr, "request %d: send failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }
      inp = recv(sock, buf, BUF_SIZE, 0);
      if ((int)inp < 0) {
        fprintf(stderr, "request %d: recv failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }
      buf[BUF_SIZE] = '\0';
      std::string response = buf;
      int nl = response.find("\r\n\r\n");
      nl += 4;
      std::string code = response.substr(9, 3);
      if (code.compare("200") != 0) {
        std::cerr << response.substr(9, nl - 9);
        close(sock);
        continue;
      }
      int fd = open(file_name.c_str(), O_CREAT | O_RDWR | O_TRUNC, S_IRWXU);
      if (fd < 0) {
        fprintf(stderr, "request %d: open failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }
      std::string content_len = response.substr(33, nl - 37);
      std::string::size_type sz;
      int content_length = std::stoi(content_len ,&sz);
      std::string data = response.substr(nl);
      int wr = write(fd, data.c_str(), data.length());
      int total_inp = data.length();
      //inp = BUF_SIZE - nl;
      while (total_inp < content_length) {

        inp = recv(sock, buf, BUF_SIZE, 0);
        if (inp < 0) {
          fprintf(stderr, "request %d: recv failed: %s\n", i - 2,
                  strerror(errno));
          close(sock);
          continue;
        }
        total_inp += inp;
        wr = write(fd, buf, inp);
        if (wr < 0) {
          fprintf(stderr, "request %d: write failed: %s\n", i - 2,
                  strerror(errno));
          close(sock);
          continue;
        }
      }
      close(fd);
    }
    // Code for put
    if (action.compare("s") == 0) {
      std::string file_name = argument.substr(2, colon2 - colon1 - 1);
      if (file_name.length() == 0) {
        fprintf(stderr, "request %d: no local file name\n", i - 2);
        close(sock);
        continue;
      }
      std::string http_name =
          argument.substr(colon2 + 1, argument.length() - colon2 - 1);
      int fd = open(file_name.c_str(), O_RDWR);
      if (fd < 0) {
        fprintf(stderr, "request %d: open failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }
      struct stat st;
      fstat(fd, &st);
      std::string putreq =
          "PUT " + http_name +
          " HTTP/1.1\r\nContent-Length: " + std::to_string(st.st_size) +
          "\r\n\r\n";
      ssize_t inp = send(sock, putreq.c_str(), strlen(putreq.c_str()), 0);
      if (inp < 0) {
        fprintf(stderr, "request %d: send failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }

      ssize_t filesize = read(fd, buf, BUF_SIZE);
      if (filesize < 0) {
        fprintf(stderr, "request %d: read failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }
      while (filesize != 0) {
        inp = send(sock, buf, filesize, 0);
        if (inp < 0) {
          fprintf(stderr, "request %d: send failed: %s\n", i - 2,
                  strerror(errno));
          close(sock);
          continue;
        }
        filesize = read(fd, buf, BUF_SIZE);
        if (filesize < 0) {
          fprintf(stderr, "request %d: read failed: %s\n", i - 2,
                  strerror(errno));
          close(sock);
          continue;
        }
      }
      inp = send(sock, buf, filesize, 0);
      inp = recv(sock, buf, BUF_SIZE, 0);
      if (inp < 0) {
        fprintf(stderr, "request %d: recv failed: %s\n", i - 2,
                strerror(errno));
        close(sock);
        continue;
      }
      std::string response = buf;
      int nl = response.find("\r\n\r\n");
      nl += 4;
      std::string code = response.substr(9, 3);
      if (code.compare("200") != 0 && code.compare("201") != 0) {
        std::cerr << response.substr(9, nl - 9);
        close(sock);
        continue;
      }
      close(fd);
    }
    // code for patch
    if (action.compare("p") == 0) {
      std::string original_name = argument.substr(2, colon2 - colon1 - 1);
      std::string new_name =
          argument.substr(colon2 + 1, argument.length() - colon2 - 1);
      std::string patch_req = "PATCH 0000000000000000000000000000000000000000 HTTP/1.1\r\n\r\nALIAS " + original_name + " " + new_name + "\r\n";
      send(sock, patch_req.c_str(), patch_req.length(), 0);
      recv(sock, buf, BUF_SIZE, 0);
      std::string response = buf;
      int nl = response.find("\r\n\r\n");
      nl += 4;
      std::string code = response.substr(9, 3);
      if (code.compare("201") != 0) {
        std::cerr << response.substr(9, nl - 9);
        close(sock);
        continue;
      }
    }
    close(sock);
  }
}
