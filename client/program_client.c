#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <termios.h>
#include <pthread.h>
#include <stdbool.h>
#include <fcntl.h>

#define PORT 8080
#define STR_SIZE 1024

int sock = 0, valread;
int status = 0;
pthread_t thread;

static const char *ROOT = "root";
static const char *ERROR = "error";

/*
    function: isRoot 
    checks if user has root level access
    @param void
    @return bool
*/
bool isRoot() {
    return (getuid() == 0);
}

/*
    function: authenticateClientSide 
    authenticates the client
    @param argc
    @param argv
    @return bool
*/
bool authenticateClientSide(int argc, char const *argv[]) {
    char buffer[STR_SIZE] = {0};

    if (isRoot()) {
        send(sock, ROOT, sizeof(ROOT), 0);
    }
    else if (argc != 5) {
        send(sock, ERROR, strlen(ERROR), 0);
        printf("Failed to authenticate.\nRecheck if you ran the program correctly.\n");
        return false;
    }
    else {
        send(sock, argv[2], strlen(argv[2]), 0);
        valread = read(sock, buffer, STR_SIZE);
        send(sock, argv[4], strlen(argv[4]), 0);
    }
    memset(buffer, 0, sizeof(buffer));
    return true;
}

void *messageHandling(void *arg) {
    char buffer[256];
    while(1) {
        read(*(int *)arg, buffer, STR_SIZE);
        if (strlen(buffer)) {
            printf("%s", buffer);
        }
        memset(buffer, 0, sizeof(buffer));
    }
}

int main(int argc, char const *argv[]) {
    // Template socket client
    struct sockaddr_in address, \
                        serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("failed to create socket\n");
        return -1;
    }

    memset(&serv_addr, '0', sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("Invalid address\n");
        return -1;
    }
    if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        printf("Connection failed\n");
        return -1;
    }
    // End of template socket client

    if (!authenticateClientSide(argc, argv)) {
        return 0;
    }

    char buffer[STR_SIZE] = {0};
    char command[STR_SIZE];

    printf("type 'quit' to quit\n");

    pthread_t messageHandler;
    pthread_create(&messageHandler, NULL, &messageHandling, &sock);

    while (true) {
        scanf("%[^\n]s", buffer);
        getchar();
        send(sock, buffer, strlen(buffer), 0);

        if (strcmp(buffer, "quit") == 0) {
            return 0;
        }

        memset(buffer, 0, sizeof(buffer));
    }
    
    return 0;
}