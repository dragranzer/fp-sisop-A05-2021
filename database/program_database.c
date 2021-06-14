#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <pthread.h>
#include <stdbool.h>

#define SIZE 50
#define STR_SIZE 1024
#define PORT 8080
#define N_OF_CONNECTION 5
#define MAX_COMMANDS 128
#define MAX_COMMAND_LENGTH 64

static const char *AUTH_ERROR = "Authentication error.\nClosing connection...\n";
static const char *ROOT = "root";
static const char *ERROR = "error";
static char *DB_PROG_NAME = "databaseku";

/*
    consts for making users and permission table
    should only be used once when the DB is started
*/ 
static char *AUTH_DB = "auth";
static char *USER_TABLE = "users";
static char *PERM_TABLE = "permissions";

/*
    A temporary stored user creds
    Will be soon deprecated
*/
static const char *NAME = "haha";
static const char *PASS = "hihi";

pthread_t tid[3000];

/*
    Start of User "object" functionalities
    Feature:
        - a User "object" storing the user's name and password
        - a User "constructor"
        - function to print a User "object" contents
*/
typedef struct user_t {
    char name[SIZE];
    char pass[SIZE];
} User;

void makeUser(User* user, char *name, char *pass) {
    strcpy(user->name, name);
    strcpy(user->pass, pass);
    return;
}

void printUser(User* user) {
    printf("name: %s\npass: %s\n", user->name, user->pass);
    return;
}

bool isAlphanum(char c) {
    if (c >= 'A' && c <= 'Z') return 1;
    else if(c >= 'a' && c <= 'z') return 1;
    else if(c >= '0' && c <= '9') return 1;
    return 0;
}

void splitCommands(const char *source, char dest[MAX_COMMANDS][MAX_COMMAND_LENGTH], int *command_size) {
    int i = 0, j = 0, k = 0;
    while (i < strlen(source)) {
        if (!isAlphanum(source[i])) {
            if (k) {
                dest[j][k++] = '\0';
                j++;
                k = 0;
            }
        }
        else {
            dest[j][k++] = source[i];
        }
        i++;
    }
    if (k) {
        dest[j][k++] = '\0';
        j++;
        k = 0;
    }
    *command_size = j;
}

void createDatabase(char *name) {
    mkdir(DB_PROG_NAME, 0777);
    char buff[256];
    sprintf(buff, "%s/%s", DB_PROG_NAME, name);
    mkdir(buff, 0777);
}

void createTable(char *db, char *tb, char *attr[64], int size) {
    char buff[256];
    sprintf(buff, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    FILE *fptr = fopen(buff, "w");
    if (!fptr) return;
    for (int i = 0; i < size; i++) {
        fprintf(fptr, "%s,", attr[i]);
    }
    fclose(fptr);
}

void insertToTable(char *db, char *tb, char *attr_data[64], int size) {
    char buff[256];
    sprintf(buff, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    FILE *fptr = fopen(buff, "a");
    if (!fptr) return;
    fprintf(fptr, "\n");
    for (int i = 0; i < size; i++) {
        fprintf(fptr, "%s,", attr_data[i]);
    }
    fclose(fptr);
    printf("[Log] INSERT INTO %s.%s (", db, tb);
    for (int i = 0; i < size; i++) {
        printf("%s", attr_data[i]);
        if (i != size-1) printf(", ");
    }
    printf(")\n");
}

void __createUsersTable() {
    char *attr[64];
    attr[0] = "name";
    attr[1] = "pass";
    createTable(AUTH_DB, USER_TABLE, attr, 2);
}

void __createPermissionsTable() {

}

void prepareUserSchema() {
    createDatabase(AUTH_DB);
    __createUsersTable();
    __createPermissionsTable();
}

// Buggy: Should be replaced soon
bool isRoot = false;

bool authenticateServerSide(User* user) {
    if (strcmp(user->name, ROOT) == 0) {
        isRoot = true;
        return true;
    }
    else if (strcmp(user->name, NAME) == 0 && strcmp(user->pass, PASS) == 0) {
        return true;
    }

    return false;
}

void dbSendMessage(int *new_socket, char *message) {
    send(*new_socket, message, strlen(message), 0);
}

void *client(void *tmp) {
    char buffer[STR_SIZE] = {0};

    int valread;
    int new_socket = *(int *)tmp;

    User current;

    // start of authentication
    valread = read(new_socket, buffer, STR_SIZE);
    if (strcmp(buffer, ROOT) == 0) {
        makeUser(&current, buffer, buffer);
    }
    else if (strcmp(buffer, ERROR) == 0) {
        printf("%s", AUTH_ERROR);
        close(new_socket);
        return 0;
    }
    else {
        char name[SIZE];
        char pass[SIZE];

        strcpy(name, buffer);
        memset(buffer, 0, sizeof(buffer));
        dbSendMessage(&new_socket, "received");
        
        valread = read(new_socket, buffer, STR_SIZE);
        strcpy(pass, buffer);

        makeUser(&current, name, pass);
    }
    memset(buffer, 0, sizeof(buffer));

    if (!authenticateServerSide(&current)) {
        printf("%s", AUTH_ERROR);
        close(new_socket);
        return 0;
    }
    // end of authentication

    char selectedDatabase[128];

    while (true) {
        valread = read(new_socket, buffer, STR_SIZE);

        if (strcmp(buffer, "quit") == 0) {
            close(new_socket);
            return 0;
        }

        char commands[MAX_COMMANDS][MAX_COMMAND_LENGTH];
        int command_size = 0;
        splitCommands(buffer, commands, &command_size);

        if (strcmp(commands[0], "CREATE") == 0) {
            if (strcmp(commands[1], "DATABASE") == 0) {
                createDatabase(commands[2]);
                strcpy(selectedDatabase, commands[2]);
                printf("[Log] Database %s has been created.\n", commands[2]);
                dbSendMessage(&new_socket, "Database created.");
            }
            else if (strcmp(commands[1], "TABLE") == 0) {
                // CREATE TABLE name (name int, name int)
                char *attr[64];
                int attr_i = 0;
                int i = 0;
                for (int i = 3; i < command_size; i += 2) {
                    attr[attr_i++] = commands[i];
                    printf("Selected %s\n", commands[i]);
                }
                createTable(selectedDatabase, commands[2], attr, attr_i);
                printf("[Log] Table %s.%s has been created.\n", selectedDatabase, commands[2]);
                dbSendMessage(&new_socket, "Table created.");
            }
            else if (strcmp(commands[1], "USER") == 0) {
                printf("%s\n%s\n", commands[2], commands[5]);
            }
        }
        else if(strcmp(commands[0], "USE") == 0) {
            strcpy(selectedDatabase, commands[1]);
        }
        else if (strcmp(commands[0], "INSERT") == 0) {
            if (strcmp(commands[1], "INTO") == 0) {
                /*
                    This function is not robust yet.
                    Please add validation for:
                    1. When no data is written
                        Ex: 
                        * INSERT INTO user
                        * INSERT INTO user ()
                    2. When number of data is not the same as number of column that table has
                        Ex:
                        * CREATE TABLE user (name string, password string)
                        * INSERT INTO user ('Sayu', '12346', 'Female')
                            This command is invalid since user table has 2 columns.
                */
                char *attr[64];
                int attr_i = 0;
                for (int i = 3; i < command_size; i++) {
                    attr[attr_i++] = commands[i];
                }
                insertToTable(selectedDatabase, commands[2], attr, attr_i);
            }
        }

        // printf("DEBUG Command:\n");
        // for (int i = 0; i < command_size; i++) {
        //     printf("%d. `%s`\n", i, commands[i]);
        // }

        if (strlen(buffer)) {
            printf("message: %s\n", buffer);
        }

        memset(buffer, 0, sizeof(buffer));
    }
}

/*
    Template for server socket
    Please leave this section alone
*/
int main(int argc, char const *argv[]) {
    int server_fd, \
        new_socket, \
        valread;
    int opt = 1;

    struct sockaddr_in address;
    int addr_len = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(server_fd, SOL_SOCKET, 
        SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0 ) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, N_OF_CONNECTION) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    prepareUserSchema();

    int total = 0;
    while(true) {
        if ((new_socket = accept(server_fd, 
            (struct sockaddr *) &address, (socklen_t*) &addr_len)) < 0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }
        pthread_create(&(tid[total]), NULL, &client, &new_socket);
        total++;
    }

    return 0;
}