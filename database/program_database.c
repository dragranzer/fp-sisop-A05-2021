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
#include <dirent.h>

#define SIZE 50
#define STR_SIZE 1024
#define PORT 8080
#define N_OF_CONNECTION 5
#define MAX_COMMANDS 128
#define MAX_COMMAND_LENGTH 64
#define MAX_COLUMN 128
#define MAX_COLUMN_LEN 64
#define MAX_TABLE_LEN 64

// const for currTime string
static const int TIME_SIZE = 30;
// const for log string size
static const int LOG_SIZE = 1000;

static char *logpath = "database.log";
static char *AUTH_ERROR = "Authentication error.\nClosing connection...\n";
static char *PERM_ERROR = "You have no permission to run that command.\n";
static char *CMMD_ERROR = "Error while parsing command.\nPlease make sure the command syntax.\n";
static char *ROOT = "root";
static char *ERROR = "error";
static char *DB_PROG_NAME = "databaseku";

/*
    consts for making users and permission table
    should only be used once when the DB is started
*/ 
static char *AUTH_DB = "auth";
static char *USER_TABLE = "users";
static char *PERM_TABLE = "permissions";

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
    bool isRoot;
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

bool equalUser(User *a, User *b) {
    return (strcmp(a->name, b->name) == 0 && strcmp(a->pass, b->pass) == 0);
}

bool isAlphanum(char c) {
    if (c >= 'A' && c <= 'Z') return 1;
    else if(c >= 'a' && c <= 'z') return 1;
    else if(c >= '0' && c <= '9') return 1;
    else if (c == '*') return 1;
    else if (c == '=') return 1;
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

bool doesDatabaseExist(const char name[]) {
    char buff[256];
    sprintf(buff, "%s/%s", DB_PROG_NAME, name);
    DIR *dir = opendir(buff);
    if (dir) {
        // Directory exists
        closedir(dir);
        return 1;
    }
    return 0;
}

bool doesTableExist(char *db, char *tb) {
    char filePath[256];
    sprintf(filePath, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    FILE *f = fopen(filePath, "r");
    if (f) {
        // table exists
        fclose(f);
        return 1;
    }
    return 0;
}

void createUser(char *u, char *p) {
    char *attr[64];
    attr[0] = u;
    attr[1] = p;
    insertToTable(AUTH_DB, USER_TABLE, attr, 2);
}

void __createUsersTable() {
    char *attr[64];
    attr[0] = "name";
    attr[1] = "pass";
    if (!doesTableExist(AUTH_DB, USER_TABLE)) {
        createTable(AUTH_DB, USER_TABLE, attr, 2);
    }
}

void __createPermissionsTable() {
    char *attr[64];
    attr[0] = "database";
    attr[1] = "user";
    if (!doesTableExist(AUTH_DB, PERM_TABLE)) {
        createTable(AUTH_DB, PERM_TABLE, attr, 2);
    }
}

void prepareAuthSchema() {
    if (!doesDatabaseExist(AUTH_DB)) {
        createDatabase(AUTH_DB);
    }
    __createUsersTable();
    __createPermissionsTable();
}

void __readUserTable(User *user, char *line) {
    char u[SIZE];
    char p[SIZE];

    int i = 0;
    int j = 0;
    while (line[i] != ',') {
        u[j++] = line[i++];
    }
    u[j++] = '\0';
    i++;
    strcpy(user->name, u);

    j = 0;
    while (line[i] != ',') {
        p[j++] = line[i++];
    }
    p[j++] = '\0';
    strcpy(user->pass, p);
}

bool __authenticateServerSideHelper(User *user) {
    char filePath[256];
    sprintf(filePath, "%s/%s/%s.nya", DB_PROG_NAME, AUTH_DB, USER_TABLE);
    FILE *f = fopen(filePath, "r");

    if (!f) {
        printf("error opening file");
        return false;
    }
    User attempt;
    char temp[256];
    int temp_size = 0;
    char ch;
    while (fscanf(f, "%c", &ch) != EOF) {
        if (ch == '\n') {
            if (temp_size) {
                temp[temp_size++] = '\n';
                temp[temp_size++] = '\0';
                __readUserTable(&attempt, temp);
                if (equalUser(user, &attempt)) {
                    return true;
                }

                temp_size = 0;
            }
        }
        else {
            temp[temp_size++] = ch;
        }
    }
    if (temp_size) {
        temp[temp_size++] = '\n';
        temp[temp_size++] = '\0';
        __readUserTable(&attempt, temp);
        if (equalUser(user, &attempt)) {
            return true;
        }
        temp_size = 0;
    }
    fclose(f);

    return false;
}

bool authenticateServerSide(User* user) {
    if (strcmp(user->name, ROOT) == 0) {
        user->isRoot = true;
        return true;
    }
    else if (__authenticateServerSideHelper(user)) {
        user->isRoot = false;
        return true;
    }

    return false;
}

void dbSendMessage(int *new_socket, char *message) {
    send(*new_socket, message, strlen(message), 0);
}

void selectFromTable(int *sock, const char *db, const char *tb) {
    char buff[256];
    sprintf(buff, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    FILE *fptr = fopen(buff, "r");
    if (!fptr) return;
    char temp[256];
    int temp_size = 0;
    char ch;
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == '\n') {
            if (temp_size) {
                temp[temp_size++] = '\n';
                temp[temp_size++] = '\0';
                dbSendMessage(sock, temp);
                temp_size = 0;
            }
        }
        else {
            temp[temp_size++] = ch;
        }
    }
    if (temp_size) {
        temp[temp_size++] = '\n';
        temp[temp_size++] = '\0';
        dbSendMessage(sock, temp);
        temp_size = 0;
    }
    fclose(fptr);
}

bool isStringInCol(char s1[MAX_COLUMN_LEN], const char arr[MAX_COLUMN][MAX_COLUMN_LEN], int arr_size) {
    for (int i = 0; i < arr_size; i++) {
        if (strcmp(s1, arr[i]) == 0) return 1;
    }
    return 0;
}

void selectFromTable2(int *sock, const char *db, const char *tb, const char col[MAX_COLUMN][MAX_COLUMN_LEN], int col_size) {
    char buff[256];
    sprintf(buff, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    FILE *fptr = fopen(buff, "r");
    if (!fptr) return;
    char printable[STR_SIZE];
    char tb_col[MAX_COLUMN_LEN];
    char tb_col_size = 0;
    bool tb_col_reserved[MAX_COLUMN];
    memset(tb_col_reserved, 0, sizeof(tb_col_reserved));
    int tb_col_number = 0;
    char ch;
    // Reading header
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == ',') {
            tb_col[tb_col_size++] = '\0';
            if (isStringInCol(tb_col, col, col_size)) {
                tb_col_reserved[tb_col_number] = 1;
                sprintf(printable, "%s%16s ", printable, tb_col);
            }
            tb_col[0] = '\0';
            tb_col_size = 0;
            tb_col_number++;
        }
        else if (ch == '\n') {
            // New line is detected
            strcat(printable, "\n");
            dbSendMessage(sock, printable);
            printf("%s", printable);
            break;
        }
        else {
            tb_col[tb_col_size++] = ch;
        }
    }
    tb_col_number = 0;
    memset(printable, 0, sizeof(printable));
    // Reading content
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == ',') {
            tb_col[tb_col_size++] = '\0';
            if (tb_col_reserved[tb_col_number++]) {
                sprintf(printable, "%s%16s ", printable, tb_col);
            }
            tb_col[0] = '\0';
            tb_col_size = 0;
        }
        else if (ch == '\n') {
            // New line
            tb_col_number = 0;
            strcat(printable, "\n");
            dbSendMessage(sock, printable);
            printf("%s", printable);
            memset(printable, 0, sizeof(printable));
        }
        else {
            tb_col[tb_col_size++] = ch;
        }
    }
    strcat(printable, "\n");
    dbSendMessage(sock, printable);
    printf("%s", printable);
    memset(printable, 0, sizeof(printable));
    fclose(fptr);
}

int updateInTable(int *sock, const char *db, const char *tb, char col[MAX_COLUMN_LEN], char val[]) {
    char buff[256];
    sprintf(buff, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    char buff2[256];
    sprintf(buff2, "%s/%s/%s_new.nya", DB_PROG_NAME, db, tb);
    FILE *fptr = fopen(buff, "r");
    if (!fptr) return 0;
    FILE *fptr2 = fopen(buff2, "w");
    if (!fptr2) return 0;
    char tb_col[MAX_COLUMN_LEN];
    int tb_col_size = 0;
    int set_col_id = 0;
    int tb_col_number = 0;
    char ch;
    // Reading header
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == ',') {
            tb_col[tb_col_size++] = '\0';
            if (strcmp(tb_col, col) == 0) {
                set_col_id = tb_col_number;
            }
            fprintf(fptr2, "%s,", tb_col);
            tb_col[0] = '\0';
            tb_col_size = 0;
            tb_col_number++;
        }
        else if (ch == '\n') {
            // New line is detected
            fprintf(fptr2, "\n");
            break;
        }
        else {
            tb_col[tb_col_size++] = ch;
        }
    }
    tb_col_number = 0;
    // Reading content
    int aff_row = 0;
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == ',') {
            tb_col[tb_col_size++] = '\0';
            if ((tb_col_number++) == set_col_id) {
                fprintf(fptr2, "%s,", val);
                aff_row++;
            }
            else {
                fprintf(fptr2, "%s,", tb_col);
            }
            tb_col[0] = '\0';
            tb_col_size = 0;
        }
        else if (ch == '\n') {
            // New line
            tb_col_number = 0;
            fprintf(fptr2, "\n");
        }
        else {
            tb_col[tb_col_size++] = ch;
        }
    }
    fclose(fptr2);
    fclose(fptr);
    remove(buff);
    rename(buff2, buff);
    return aff_row;
}

int deleteFromTable(char *db, char *tb, char col[], char val[]) {
    char buff[256];
    sprintf(buff, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);
    char buff2[256];
    sprintf(buff2, "%s/%s/%s_new.nya", DB_PROG_NAME, db, tb);
    FILE *fptr = fopen(buff, "r");
    if (!fptr) return 0;
    FILE *fptr2 = fopen(buff2, "w");
    if (!fptr2) return 0;
    char tb_col[MAX_COLUMN_LEN];
    int tb_col_size = 0;
    int set_col_id = 0;
    int tb_col_number = 0;
    char ch;
    bool useWhere = true;
    if (strcmp(col, "$") == 0) useWhere = false;
    // Reading header
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == ',') {
            tb_col[tb_col_size++] = '\0';
            if (useWhere && strcmp(tb_col, col) == 0) {
                set_col_id = tb_col_number;
            }
            fprintf(fptr2, "%s,", tb_col);
            tb_col[0] = '\0';
            tb_col_size = 0;
            tb_col_number++;
        }
        else if (ch == '\n') {
            // New line is detected
            fprintf(fptr2, "\n");
            break;
        }
        else {
            tb_col[tb_col_size++] = ch;
        }
    }
    tb_col_number = 0;
    // Reading content
    int aff_row = 0;
    char printable[1024];
    memset(printable, 0, sizeof(printable));
    bool isDeleted = false;
    while (fscanf(fptr, "%c", &ch) != EOF) {
        if (ch == ',') {
            tb_col[tb_col_size++] = '\0';
            if (!useWhere) {
                isDeleted = 1;
                if ((tb_col_number++) == set_col_id)
                    aff_row++;
            }
            else {
                if ((tb_col_number++) == set_col_id && strcmp(tb_col, val) == 0) {
                    isDeleted = 1;
                    aff_row++;
                }
            }
            sprintf(printable, "%s%s,", printable, tb_col);
            tb_col[0] = '\0';
            tb_col_size = 0;
        }
        else if (ch == '\n') {
            // New line
            tb_col_number = 0;
            strcat(printable, "\n");
            if (!isDeleted) {
                fprintf(fptr2, "%s", printable);
            }
            memset(printable, 0, sizeof(printable));
            isDeleted = 0;
        }
        else {
            tb_col[tb_col_size++] = ch;
        }
    }
    strcat(printable, "\n");
    if (!isDeleted) {
        fprintf(fptr2, "%s", printable);
    }
    memset(printable, 0, sizeof(printable));
    isDeleted = 0;
    fclose(fptr2);
    fclose(fptr);
    remove(buff);
    rename(buff2, buff);
    return aff_row;
}

void grantPermission(char *db, char *us) {
    char *attr[64];
    attr[0] = db;
    attr[1] = us;
    insertToTable(AUTH_DB, PERM_TABLE, attr, 2);
}

void __hasPermissionToDBHelper(char *line, char *db_r, char *us_r) {
    char d[SIZE];
    char u[SIZE];

    int i = 0;
    int j = 0;
    while (line[i] != ',') {
        d[j++] = line[i++];
    }
    d[j++] = '\0';
    i++;
    strcpy(db_r, d);

    j = 0;
    while (line[i] != ',') {
        u[j++] = line[i++];
    }
    u[j++] = '\0';
    strcpy(us_r, u);
}

bool hasPermissionToDB(char *name, char *db) {

    if (strcmp(name, ROOT) == 0) {
        return true;
    }

    char filePath[256];
    sprintf(filePath, "%s/%s/%s.nya", DB_PROG_NAME, AUTH_DB, PERM_TABLE);
    FILE *f = fopen(filePath, "r");

    if (!f) {
        printf("error opening file");
        return false;
    }
    User attempt;
    char temp[256];
    int temp_size = 0;
    char ch;

    char db_read[SIZE];
    char us_read[SIZE];

    while (fscanf(f, "%c", &ch) != EOF) {
        if (ch == '\n') {
            if (temp_size) {
                temp[temp_size++] = '\n';
                temp[temp_size++] = '\0';
                __hasPermissionToDBHelper(temp, db_read, us_read);

                if (strcmp(db_read, db) == 0 && strcmp(us_read, name) == 0) {
                    return true;
                }

                temp_size = 0;
            }
        }
        else {
            temp[temp_size++] = ch;
        }
    }
    if (temp_size) {
        temp[temp_size++] = '\n';
        temp[temp_size++] = '\0';
        __hasPermissionToDBHelper(temp, db_read, us_read);

        if (strcmp(db_read, db) == 0 && strcmp(us_read, name) == 0) {
            return true;
        }

        temp_size = 0;
    }
    fclose(f);

    return false;
}

void dropDatabase(char *db) {

}

void dropTable(char *db, char *tb) {
    char filePath[SIZE];
    sprintf(filePath, "%s/%s/%s.nya", DB_PROG_NAME, db, tb);

    if (remove(filePath) == 0) {
        printf("[Log] Table %s.%s has ben dropped.\n", db, tb);
    }
    else {
        printf("failed to drop table.");
    }
}

void logging(User *user, char *command) {
    time_t t = time(NULL);
    struct tm* lt = localtime(&t);

    char currTime[TIME_SIZE];
    strftime(currTime, TIME_SIZE, "%Y-%m-%d %H:%M:%S", lt);

    char log[LOG_SIZE];
    sprintf(log, "%s:%s:%s", currTime, user->name, command);

    FILE *out = fopen(logpath, "a");
    fprintf(out, "%s\n", log);
    fclose(out);
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
    selectedDatabase[0] = '\0';
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
                if (strlen(commands[2])) {
                    if (doesDatabaseExist(commands[2])) {
                        dbSendMessage(&new_socket, "Error, database with that name already exists.\n");
                    }
                    else {
                        createDatabase(commands[2]);
                        grantPermission(commands[2], current.name);
                        dbSendMessage(&new_socket, "Database created.\n");
                        logging(&current, buffer);
                    }
                }
                else {
                    dbSendMessage(&new_socket, "Syntax Error: CREATE DATABASE [database name]\n");
                }
            }
            else if (strcmp(commands[1], "TABLE") == 0) {
                if (selectedDatabase[0] == '\0') dbSendMessage(&new_socket, "No database is selected.\n");
                else {
                    // CREATE TABLE name (name int, name int)
                    char *attr[64];
                    int attr_i = 0;
                    int i = 0;
                    for (int i = 3; i < command_size; i += 2) {
                        attr[attr_i++] = commands[i];
                        printf("Selected %s\n", commands[i]);
                    }

                    if (!doesTableExist(selectedDatabase, commands[2])) {
                        createTable(selectedDatabase, commands[2], attr, attr_i);
                        logging(&current, buffer);
                        dbSendMessage(&new_socket, "Table created.\n");
                    }
                    else {
                        dbSendMessage(&new_socket, "Sorry, a table with that name was already created in this database.\n");
                    }
                }   
            }
            else if (strcmp(commands[1], "USER") == 0) {
                if (!current.isRoot) {
                    dbSendMessage(&new_socket, PERM_ERROR);
                }
                else if (current.isRoot) {
                    if (command_size != 6) {
                        dbSendMessage(&new_socket, CMMD_ERROR);
                    }
                    else {
                        createUser(commands[2], commands[5]);
                        char success[STR_SIZE];
                        sprintf(success, "Successfully created user %s\n", commands[2]);
                        logging(&current, buffer);
                        dbSendMessage(&new_socket, success);
                    }
                }
            }
            else {
                dbSendMessage(&new_socket, "Syntax Error: CREATE [What to create]");
            }
        }
        else if(strcmp(commands[0], "USE") == 0) {
            if (doesDatabaseExist(commands[1]) && command_size == 2) {
                if (hasPermissionToDB(current.name, commands[1])) {
                    strcpy(selectedDatabase, commands[1]);
                    logging(&current, buffer);
                    dbSendMessage(&new_socket, "Database selected.\n");
                }
                else {
                    dbSendMessage(&new_socket, PERM_ERROR);
                }
            }
            else dbSendMessage(&new_socket, "Error, database not found.\n");
        }
        else if (strcmp(commands[0], "INSERT") == 0) {
            if (strcmp(commands[1], "INTO") == 0) {
                if (selectedDatabase[0] == '\0') dbSendMessage(&new_socket, "No database is selected.\n");
                else {
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
                    if (attr_i) {
                        insertToTable(selectedDatabase, commands[2], attr, attr_i);
                        logging(&current, buffer);
                        dbSendMessage(&new_socket, "Data inserted.\n");
                    }
                    else dbSendMessage(&new_socket, "No value is assigned.\n");
                }
            }
            else dbSendMessage(&new_socket, "Usage: INSERT INTO [database name] (value1, value2, ...)\n");
        }
        else if (strcmp(commands[0], "SELECT") == 0) {
            if (selectedDatabase[0] == '\0') dbSendMessage(&new_socket, "No database is selected.\n");
            else {
                if (strcmp(commands[1], "*") == 0) {
                    selectFromTable(&new_socket, selectedDatabase, commands[3]);
                    logging(&current, buffer);
                }
                else {
                    // Stores columns that will be selected
                    char col[MAX_COLUMN][MAX_COLUMN_LEN];
                    int col_size = 0;
                    // Stores table name
                    char tb[MAX_TABLE_LEN];
                    // Incremental value
                    int i = 1;
                    bool isValid = false;
                    bool withWhere = false;
                    while(i < command_size) {
                        if (strcmp(commands[i], "FROM") == 0) {
                            // After string "FROM", there must be followed by table name
                            if (i + 1 < command_size) {
                                strcpy(tb, commands[i+1]);
                                isValid = true;
                            }
                            // Exit while scope
                            i = command_size;
                        }
                        else {
                            strcpy(col[col_size++], commands[i]);
                            i++;
                        }
                    }
                    if (!isValid) {
                        dbSendMessage(&new_socket, "Syntax error: SELECT [col1, col2 | *] FROM [table]\n");
                    }
                    else {
                        printf("col\n");
                        for (int i = 0; i < col_size; i++) printf("%d. `%s`\n", i, col[i]);
                        selectFromTable2(&new_socket, selectedDatabase, tb, col, col_size);
                        logging(&current, buffer);
                    }
                }
            }
        }
        else if (strcmp(commands[0], "UPDATE") == 0) {
            if (doesTableExist(selectedDatabase, commands[1])) {
                if (strcmp(commands[2], "SET") == 0) {
                    if (command_size == 6 && strcmp(commands[4], "=") == 0) {
                        int affected_row = updateInTable(&new_socket, selectedDatabase, commands[1], commands[3], commands[5]);
                        char buff[128];
                        sprintf(buff, "%d rows affected.\n", affected_row);
                        dbSendMessage(&new_socket, buff);
                        logging(&current, buffer);
                    }
                    else dbSendMessage(&new_socket, "Syntax error: UPDATE [table name] SET [col] = [value]\n");
                }
                else dbSendMessage(&new_socket, "Syntax error: UPDATE [table name] SET [col] = [value]\n");
            }
            else dbSendMessage(&new_socket, "Table not found.\n");
        }
        else if (strcmp(commands[0], "DELETE") == 0) {
            if (strcmp(commands[1], "FROM") == 0) {
                if (selectedDatabase[0] == '\0') dbSendMessage(&new_socket, "No database is selected.\n");
                else {
                    if (doesTableExist(selectedDatabase, commands[2])) {
                        if (command_size == 7 && strcmp(commands[3], "WHERE") == 0 && strcmp(commands[5], "=") == 0) {
                            // Delete with WHERE
                            int affected_row = deleteFromTable(selectedDatabase, commands[2], commands[4], commands[6]);
                            char buff[128];
                            sprintf(buff, "%d affected row.\n", affected_row);
                            dbSendMessage(&new_socket, buff);
                        }
                        else {
                            // Delete without where
                            int affected_row = deleteFromTable(selectedDatabase, commands[2], "$", "$");
                            char buff[128];
                            sprintf(buff, "%d affected row.\n", affected_row);
                            dbSendMessage(&new_socket, buff);
                        }
                        logging(&current, buffer);
                    }
                    else {
                        dbSendMessage(&new_socket, "Table not exist.\n");
                    }
                }
            }
            else dbSendMessage(&new_socket, "Syntax error: DELETE FROM [table name]\n");
        }
        else if (strcmp(commands[0], "GRANT") == 0) {
            if (command_size != 5) {
                dbSendMessage(&new_socket, "Syntax Error: GRANT PERMISSION [Database Name] INTO [User name]\n");
            }
            else {
                if (!current.isRoot) {
                    dbSendMessage(&new_socket, PERM_ERROR);
                }
                else if (current.isRoot) {
                    grantPermission(commands[2], commands[4]);
                    char success[STR_SIZE];
                    sprintf(success, "Granted %s permission to database %s\n", commands[4], commands[2]);
                    dbSendMessage(&new_socket, success);
                    logging(&current, buffer);
                }
            }
        }
        else if (strcmp(commands[0], "DROP") == 0) {
            if (command_size > 2) {
                if (strcmp(commands[1], "DATABASE") == 0) {
                    if (doesDatabaseExist(commands[2])) {
                        dropDatabase(commands[2]);
                    }
                    else {
                        dbSendMessage(&new_socket, "Cannot found database\n");
                    }
                }
                else if (strcmp(commands[1], "TABLE") == 0) {
                    if (doesTableExist(selectedDatabase, commands[2])) {
                        dropTable(selectedDatabase, commands[2]);
                        logging(&current, buffer);
                    }
                    else {
                        dbSendMessage(&new_socket, "Cannot found table\n");
                    }
                }
                else if (strcmp(commands[1], "COLUMN") == 0) {
                    if (command_size != 5) {
                        dbSendMessage(&new_socket, "Syntax Error: DROP COLUMN [column_name] FROM [table_name]\n");
                    }
                }
            }
            else {
                dbSendMessage(&new_socket, "Syntax Error on DROP query\n");
            }
        }
        else dbSendMessage(&new_socket, "Command not found.\n");

        // printf("DEBUG Command:\n");
        // for (int i = 0; i < command_size; i++) {
        //     printf("%d. `%s`\n", i, commands[i]);
        // }

        // if (strlen(buffer)) {
        //     printf("message: %s\n", buffer);
        // }

        memset(buffer, 0, sizeof(buffer));
        memset(commands, 0, sizeof(commands));
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

    prepareAuthSchema();

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
