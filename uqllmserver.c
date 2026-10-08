/** @file uqllmserver.c
 * @author s4916521
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @aitool ChatGPT
 *
 * This program provides the uqllmserver networked server.
 */
#include "csse2310a3.h"
#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>

#define INCORRECT_NUMBER_OF_ARGUMENTS 10
#define USER_EOF 0
#define COMMUNICATION_ERROR_WITH_THE_SERVER 17
#define CANNOT_CONNECT_TO_THE_SERVER 11
#define EMPTY_STRING_FOR_PORT 15
#define LOCAL_HOST "localhost"
#define EXIT_INVALID_ARGUMENTS 13
#define EXIT_LISTEN_MAIN_PORT 8
#define EXIT_LISTEN_STATS_PORT 12
#define EXIT_LLM_START_FAILED 5
#define EXIT_LLM_CONNECT_FAILED 9
#define BASE_DECIMAL 10
#define MIN_ARGS 3
#define MAX_ARGS 4
#define MAX_CONNECTIONS 10000
#define DEFAULT_MAX_CONNECTIONS 200000
#define LISTEN_BACKLOG 1024
#define DEFAULT_SUCCESS 10
/** Maximum allowed history size (command-line historysize). */
#define MAX_HISTORY_SIZE 10
#define ARRAY_SIZE 16384
#define MAX_PROMPT_LENGTH 2048
/** Reserved space in buffer for HTTP response headers before body. */
#define HTTP_HEADER_RESERVE 128
#define HOME_PAGE_PATH "/local/courses/csse2310/resources/a3/home.html"
/** Buffer size for reading smollm port from pipe. */
#define SMOLLM_PORT_BUFFER_SIZE 1024
/** Max length of port number string for getaddrinfo. */
#define PORT_NUMBER_STRING_MAX 16
#define LLM_COMPLETION_PATH "/completion"
#define LLM_N_PREDICT_DEFAULT 256
#define IM_END_TOKEN "<|im_end|>"
/** HTTP status code for a successful response. */
#define HTTP_STATUS_OK 200
#define RETRY_COUNT 25
/** Max string length for HTTP Content-Length header value. */
#define HTTP_CONTENT_LEN_STR_MAX 32
/** HttpHeader pointer slots: two headers plus NULL terminator. */
#define HTTP_RESPONSE_HEADER_SLOTS 3
/** Extra bytes for snprintf pattern "\"%s\":\"" plus NUL. */
#define JSON_KEY_PATTERN_EXTRA 5
/** Bytes to skip past a matched "\"key\":\"" prefix. */
#define JSON_KEY_VALUE_PREFIX_LEN 4
/** Bytes to skip past a matched "\"key\":" prefix (numeric values). */
#define JSON_UINT_KEY_PREFIX_LEN 3
/** Initial capacity when parsing a JSON string value. */
#define JSON_STRING_INIT_CAP 256

/**
 * @brief Program configuration: port
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct Params {

    bool portSpecified; /**< True if port was given on the command line. */
    char* port; /**< Port number from argv. */
    int maxconns; /**< Maximum number of connections. */
    int historysize; /**< History size. */
    int portnumber; /**< Port number. */
    bool portnumberSpecified; /**< True if portnumber was given on the command
                                 line. */
} Params;

/**
 * @brief Shared server state protected by a mutex
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
/**
 * @brief Shared smollm2 connection (matches ai.c LlmConn).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct {
    pthread_mutex_t lock; /**< Protects LLM socket access across threads. */
    int sockFd; /**< Socket for writing to smollm2, or -1 */
    FILE* readFp; /**< Persistent read stream for smollm2 responses */
    int llmPort; /**< TCP port smollm2 listens on. */
    pid_t llmPid; /**< Child process ID of smollm2. */
} LlmConn;

/**
 * @brief Shared server state for all client threads.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct ServerData {
    pthread_mutex_t lock; /**< Protects shared server fields. */
    sem_t await; /**< Semaphore used to coordinate server threads. */
    LlmConn llm; /**< Shared LLM connection for all client threads */
} ServerData;

/**
 * @brief Statistics counters reported on SIGHUP
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct OutputData {
    pthread_mutex_t lock; /**< Protects all statistics counters below. */
    uint32_t numClientsConnected; /**< Clients currently connected. */
    uint32_t totalConnectedClients; /**< Total clients ever connected. */
    uint32_t completedSessions; /**< Completed client sessions. */
    uint32_t promptsReceived; /**< Prompts received from clients. */
    uint32_t tokensEvaluated; /**< Tokens evaluated by the LLM. */
} OutputData;

/**
 * @brief One user/assistant turn in per-client chat history.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct Block {
    char* prompt; /**< User prompt text for this turn. */
    char* response; /**< Assistant response text for this turn. */
    struct Block* next; /**< Next (newer) block, or NULL. */
} Block;

/**
 * @brief Per-client thread context passed to client_handling
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct InnerData {
    ServerData* server; /**< Shared server state. */
    int serverFd; /**< Connected client socket (write via this fd). */
    LlmConn* llm; /**< Shared smollm2 connection */
    sem_t* connected; /**< Connection limit semaphore. */
    OutputData* output; /**< Statistics counters to update. */
    int historySize; /**< Max chat history blocks */
    Block* histHead; /**< Oldest block in chat history list. */
    Block* histTail; /**< Newest block in chat history list. */
    int histCount; /**< Number of blocks currently stored. */
} InnerData;

/**
 * @brief Character and its JSON string escape sequence
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct {
    char c; /**< Source character to escape in JSON. */
    const char* rep; /**< Replacement escape sequence (e.g. "\\n"). */
} JsonEscape;

/** JSON escape rules applied when building the LLM prompt body. */
const JsonEscape JSON_ESCAPES[] = {
        {'\\', "\\\\"},
        {'"', "\\\""},
        {'\n', "\\n"},
        {'\r', "\\r"},
        {'\t', "\\t"},
};

#define JSON_ESCAPE_COUNT (sizeof(JSON_ESCAPES) / sizeof(JSON_ESCAPES[0]))

int send_client_invalid_address(int fd);
int send_client_invalid_method(int fd);
int send_client_home_page(int fd);
int send_request_llm(LlmConn* llm, const char* jsonBody, size_t jsonLen);
int handle_llm_prompt(
        int fd, InnerData* innerData, unsigned char* body, unsigned long len);
int try_connect_llm(int smollmPort);
int append_literal(
        char* dest, size_t destSize, size_t* offset, const char* lit);
int append_json_escaped(char* dest, size_t destSize, size_t* offset,
        const char* src, size_t srcLen);
char* build_llm_json(Block* history, int historySize, const char* currentPrompt,
        size_t promptLen);
void history_append(Block** headPtr, Block** tailPtr, int* countPtr,
        int historySize, const char* prompt, const char* response);
void history_free(Block* head);
int deliver_llm_content_to_client(int fd, const char* content);
int send_to_client(int fd, const char* content, unsigned long contentLen);
char* json_parse_quoted_string(const char** pos);
char* json_find_quoted_value(const char* json, const char* key);
void json_find_uint_value(const char* json, const char* key, unsigned int* out);
void exit_llm_connect_failed(LlmConn* llm);
int try_connect_smollm2(LlmConn* llm);
int connect_smollm2(LlmConn* llm);
int ensure_smollm2(LlmConn* llm);
int send_llm_prompt_locked(LlmConn* llm, const char* jsonBody, size_t jsonLen);
int read_llm_response_locked(LlmConn* llm, const char* jsonBody, size_t jsonLen,
        int* llmStatus, char** llmStatusText, HttpHeader*** llmHeaders,
        unsigned char** llmBody, unsigned long* llmBodyLen);
int recv_response_from_server(InnerData* innerData, FILE* clientFp);
int listen_stats_port(const char* port);
int handle_one_stats_request(int fd, FILE* fp, OutputData* output);
void* stats_client_thread(void* arg);
void* stats_listen_thread(void* arg);
void start_stats_server(int statsFd, OutputData* data);
int listen_stats_from_env(int mainFd);
ServerData* alloc_server_data(void);
void start_llm_for_server(ServerData* server);
int send_client_statistics(int fd, OutputData* data);
int send_client_prompt_too_large(int fd, char* prompt);

/**
 * @brief Initialise a Params structure to default values
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param params Pointer to Params to clear and initialise
 */
void init_params(Params* params)
{
    params->portSpecified = false;
    params->port = NULL;
    params->maxconns = 0;
    params->historysize = 0;
    params->portnumber = 0;
    params->portnumberSpecified = false;
}

/**
 * @brief Display usage information and exit
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
void show_usage_and_exit()
{
    fprintf(stderr, "Usage: uqllmserver maxconns historysize [portnumber]\n");
    exit(EXIT_INVALID_ARGUMENTS);
}

/**
 * @brief Parse command-line argv into maxconns, historysize, and portnumber
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param argc Argument count from main
 * @param argv Argument vector from main
 * @param params Output Params populated from argv
 */
void check_arguments(int argc, char* argv[], Params* params)
{
    if (argc < MIN_ARGS || argc > MAX_ARGS) {
        show_usage_and_exit();
    }
    for (int i = 1; i < argc; i++) {
        if (strlen(argv[i]) == 0) {
            show_usage_and_exit();
        }
    }
    char* endptr;
    long maxConnsLong = strtol(argv[1], &endptr, BASE_DECIMAL);
    if (*endptr != '\0' || maxConnsLong < 0 || maxConnsLong > MAX_CONNECTIONS) {
        show_usage_and_exit();
    }
    if (maxConnsLong == 0) {
        params->maxconns = DEFAULT_MAX_CONNECTIONS;
    } else {
        params->maxconns = (int)maxConnsLong;
    }

    /* Parse historysize. */
    unsigned long historysize = strtoul(argv[2], &endptr, BASE_DECIMAL);
    if (*endptr != '\0' || historysize <= 0 || historysize > MAX_HISTORY_SIZE) {
        show_usage_and_exit();
    }
    params->historysize = (int)historysize;
    if (argc == MAX_ARGS) {
        params->portnumber = strtol(argv[MAX_ARGS - 1], &endptr, BASE_DECIMAL);
        if (*endptr != '\0' || params->portnumber < 0) {
            show_usage_and_exit();
        }
        params->portnumberSpecified = true;
        params->port = argv[MAX_ARGS - 1];
    }
}

/**
 * @brief Check port
 * and print error message and exit if port is not valid
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param port Port number
 */
void port_checking(const char* port)
{
    fprintf(stderr, "uqllmserver: unable to listen on given port \"%s\"\n",
            port);
    exit(EXIT_LISTEN_MAIN_PORT);
}

/**
 * @brief Create a listening socket on the given port
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param port Port number string, or NULL to let the OS choose a port
 * @return Listening socket file descriptor (does not return on error)
 */
int check_port(const char* port)
{
    struct addrinfo hints;
    struct addrinfo* ai = NULL;
    /* Resolve local address for passive TCP listen. */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    const char* realPort = (port == NULL) ? "0" : port;
    if (getaddrinfo(NULL, realPort, &hints, &ai) != 0) {
        port_checking(realPort);
    }
    /* Create socket and bind to the resolved address. */
    int serv = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (serv < 0) {
        freeaddrinfo(ai);
        port_checking(realPort);
    }
    if (bind(serv, ai->ai_addr, ai->ai_addrlen) != 0) {
        close(serv);
        freeaddrinfo(ai);
        port_checking(realPort);
    }
    /* Report chosen port on stderr when the OS assigned one. */
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (getsockname(serv, (struct sockaddr*)&addr, &len) == 0) {
        fprintf(stderr, "%d\n", ntohs(addr.sin_port));
        fflush(stderr);
    }
    /* Start listening; release addrinfo before returning the fd. */
    if (listen(serv, LISTEN_BACKLOG) != 0) {
        close(serv);
        freeaddrinfo(ai);
        port_checking(realPort);
    }
    freeaddrinfo(ai);
    return serv;
}

/**
 * @brief Start smollm
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llmPid Process ID of smollm2
 * @return Port number
 */
int start_smollm(int* llmPid)
{
    int pr[2] = {-1, -1};
    pipe(pr);
    int pid = fork();
    if (pid == 0) {
        dup2(pr[1], STDOUT_FILENO);
        close(pr[0]);
        close(pr[1]);
        /* Redirect stderr to /dev/null in the child. */
        dup2(open("/dev/null", O_WRONLY), STDERR_FILENO);
        char* smollmArgv[] = {"smollm2", NULL};
        execvp("smollm2", smollmArgv);
        exit(EXIT_LLM_START_FAILED);
    }
    close(pr[1]);
    /* Read port number from the pipe; EOF or read error means start failed. */
    char buffer[SMOLLM_PORT_BUFFER_SIZE];
    memset(buffer, 0, sizeof(buffer));
    ssize_t bytesRead = read(pr[0], buffer, sizeof(buffer));
    if (bytesRead <= 0) {
        fprintf(stderr, "uqllmserver: could not start the LLM\n");
        exit(EXIT_LLM_START_FAILED);
    }
    close(pr[0]);
    *llmPid = pid;
    return atoi(buffer);
}

/**
 * @brief Print LLM connect error and exit
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
void print_llm_connect_error(void)
{
    fprintf(stderr, "uqllmserver: could not connect to the LLM\n");
    exit(EXIT_LLM_CONNECT_FAILED);
}

/**
 * @brief Connect to the LLM
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param smollmPort Port number on which smollm is listening
 * @return Connected socket file descriptor (does not return on error)
 */
int connect_llm(int smollmPort)
{
    struct addrinfo* ai = 0;
    struct addrinfo hints = {0};
    char portStr[PORT_NUMBER_STRING_MAX];
    snprintf(portStr, sizeof(portStr), "%d", smollmPort);
    /* Build hints for IPv4 TCP and resolve host:port. */
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int err = getaddrinfo(LOCAL_HOST, portStr, &hints, &ai);
    if (err) {
        /* Name lookup failed; free any partial result and abort. */
        if (ai) {
            freeaddrinfo(ai);
        }
        print_llm_connect_error();
    }
    /* Open socket and connect; on failure release resources and abort. */
    int connectedFd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(connectedFd, ai->ai_addr, ai->ai_addrlen) != 0) {
        freeaddrinfo(ai);
        close(connectedFd);
        print_llm_connect_error();
    }
    freeaddrinfo(ai);
    return connectedFd;
}

/**
 * @brief Try a single TCP connection to smollm2 (no process exit on failure).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param smollmPort Port smollm2 is listening on
 * @return Connected socket fd, or -1 on failure
 */
int try_connect_llm(int smollmPort)
{
    /* Resolve localhost and connect one TCP socket to smollm2. */
    struct addrinfo* ai = NULL;
    struct addrinfo hints = {0};
    char portStr[PORT_NUMBER_STRING_MAX];
    snprintf(portStr, sizeof(portStr), "%d", smollmPort);
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(LOCAL_HOST, portStr, &hints, &ai) != 0) {
        if (ai) {
            freeaddrinfo(ai);
        }
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        freeaddrinfo(ai);
        return -1;
    }
    if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
        freeaddrinfo(ai);
        close(fd);
        return -1;
    }
    freeaddrinfo(ai);
    return fd;
}

/**
 * @brief Single TCP connect; set up persistent readFp (matches ai.c).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection to update
 * @return 1 on success, 0 on failure
 */
int try_connect_smollm2(LlmConn* llm)
{
    /* Drop any previous smollm2 socket and read stream. */
    if (llm->readFp) {
        fclose(llm->readFp);
        llm->readFp = NULL;
    }
    if (llm->sockFd >= 0) {
        close(llm->sockFd);
        llm->sockFd = -1;
    }
    /* Open a fresh TCP connection and attach a read FILE*. */
    int fd = try_connect_llm(llm->llmPort);
    if (fd < 0) {
        return 0;
    }
    FILE* fp = fdopen(fd, "r");
    if (!fp) {
        close(fd);
        return 0;
    }
    llm->sockFd = fd;
    llm->readFp = fp;
    return 1;
}

/**
 * @brief Connect to smollm2 with up to RETRY_COUNT attempts (matches ai.c).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection to update
 * @return 1 on success, 0 on failure
 */
int connect_smollm2(LlmConn* llm)
{
    for (int attempt = 0; attempt < RETRY_COUNT; attempt++) {
        if (try_connect_smollm2(llm)) {
            return 1;
        }
    }
    return 0;
}

/**
 * @brief Send client invalid request
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 * @return Bytes written on success, 0 if nothing sent, -1 on write failure
 */
int send_client_invalid_method(int fd)
{
    const char* body = "Invalid HTTP method\n";
    char buffer[ARRAY_SIZE];
    size_t bodyLen = strlen(body);
    int written = snprintf(buffer, sizeof(buffer),
            "HTTP/1.1 405 Method Not Allowed\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: %zu\r\n"
            "\r\n"
            "%s",
            bodyLen, body);
    if (written > 0) {
        ssize_t sendBytes = write(fd, buffer, (size_t)written);
        if (sendBytes <= 0) {
            return -1;
        }
        return sendBytes;
    }
    return 0;
}

/**
 * @brief Send client home page
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 * @return Bytes written on success, -1 on read or write failure
 */
int send_client_home_page(int fd)
{
    char buffer[ARRAY_SIZE];
    char body[ARRAY_SIZE];
    size_t maxBody = sizeof(buffer) - HTTP_HEADER_RESERVE;
    /* Load the  home page from the course resources path. */
    FILE* file = fopen(HOME_PAGE_PATH, "r");
    if (file == NULL) {
        return -1;
    }
    size_t bodyLen = fread(body, 1, maxBody, file);
    fclose(file);
    /* Assemble headers and HTML body in one buffer for a single write. */
    int headerLen = snprintf(buffer, sizeof(buffer),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html\r\n"
            "Content-Length: %zu\r\n"
            "\r\n",
            bodyLen);
    if (headerLen < 0 || (size_t)headerLen + bodyLen >= sizeof(buffer)) {
        return -1;
    }
    memcpy(buffer + headerLen, body, bodyLen);
    ssize_t sendBytes = write(fd, buffer, (size_t)headerLen + bodyLen);
    if (sendBytes <= 0) {
        return -1;
    }
    return sendBytes;
}

/**
 * @brief Parse a quoted JSON string at *pos (handles \\n etc.).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @param pos Input/output pointer into JSON (advanced past closing quote)
 * @return Newly allocated decoded string, or NULL on allocation failure
 */
char* json_parse_quoted_string(const char** pos)
{
    size_t cap = JSON_STRING_INIT_CAP;
    char* result = malloc(cap);
    if (!result) {
        return NULL;
    }
    size_t used = 0;

    /* Decode characters until the closing double quote. */
    while (**pos && **pos != '"') {
        char c;
        if (**pos == '\\' && (*pos)[1]) {
            (*pos)++;
            switch (**pos) {
            case 'n':
                c = '\n';
                break;
            case 'r':
                c = '\r';
                break;
            case 't':
                c = '\t';
                break;
            case '"':
                c = '"';
                break;
            case '\\':
                c = '\\';
                break;
            default:
                c = **pos;
                break;
            }
        } else {
            c = **pos;
        }
        if (used + 1 >= cap) {
            cap *= 2;
            char* tmp = realloc(result, cap);
            if (!tmp) {
                free(result);
                return NULL;
            }
            result = tmp;
        }
        result[used++] = c;
        (*pos)++;
    }
    result[used] = '\0';
    return result;
}

/**
 * @brief Find a quoted JSON string value for key and decode it.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @param json The JSON string to search
 * @param key The key whose quoted value is required
 * @return Newly allocated decoded string, or NULL if not found
 */
char* json_find_quoted_value(const char* json, const char* key)
{
    size_t keyLen = strlen(key);
    char* pattern = malloc(keyLen + JSON_KEY_PATTERN_EXTRA);
    if (!pattern) {
        return NULL;
    }
    snprintf(pattern, keyLen + JSON_KEY_PATTERN_EXTRA, "\"%s\":\"", key);
    const char* pos = strstr(json, pattern);
    free(pattern);
    if (!pos) {
        return NULL;
    }
    pos += keyLen + JSON_KEY_VALUE_PREFIX_LEN;
    return json_parse_quoted_string(&pos);
}

/**
 * @brief Find an unsigned integer JSON value for key.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param json The JSON string to search
 * @param key The key whose numeric value is required
 * @param out Output value (unchanged if key is missing)
 */
void json_find_uint_value(const char* json, const char* key, unsigned int* out)
{
    size_t keyLen = strlen(key);
    char* pattern = malloc(keyLen + JSON_KEY_PATTERN_EXTRA);
    if (!pattern) {
        return;
    }
    snprintf(pattern, keyLen + JSON_KEY_PATTERN_EXTRA, "\"%s\":", key);
    const char* pos = strstr(json, pattern);
    free(pattern);
    if (!pos) {
        return;
    }
    pos += keyLen + JSON_UINT_KEY_PREFIX_LEN;
    /*skip whitespace*/
    while (*pos == ' ') {
        pos++;
    }
    unsigned int val = 0;
    /*parse the integer "123" -> 123*/
    while (isdigit((unsigned char)*pos)) {
        val = val * BASE_DECIMAL + (unsigned int)(*pos - '0');
        pos++;
    }
    *out = val;
}

/**
 * @brief If smollm2 died, restart and reconnect (must hold llm->lock).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection state
 * @return 1 if smollm2 is available, 0 if restart/reconnect failed
 */
int ensure_smollm2(LlmConn* llm)
{
    int status;
    pid_t ret = waitpid(llm->llmPid, &status, WNOHANG);
    if (ret == llm->llmPid) {
        int pid = 0;
        int port = start_smollm(&pid);
        llm->llmPid = pid;
        llm->llmPort = port;
        if (!connect_smollm2(llm)) {
            return 0;
        }
    }
    return 1;
}

/**
 * @brief Send a 200 OK plain-text HTTP response to a connected client.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected client socket
 * @param content Response body bytes
 * @param contentLen Length of content in bytes
 * @return Bytes written on success, -1 on allocation or write failure
 */
int send_to_client(int fd, const char* content, unsigned long contentLen)
{
    unsigned long responseLen = 0;
    int status = -1;
    char contentLenStr[HTTP_CONTENT_LEN_STR_MAX];
    snprintf(contentLenStr, sizeof(contentLenStr), "%lu", contentLen);

    /* Two headers (Content-Type, Content-Length) plus NULL terminator. */
    HttpHeader** headers
            = malloc(HTTP_RESPONSE_HEADER_SLOTS * sizeof(HttpHeader*));
    if (!headers) {
        return -1;
    }
    headers[0] = malloc(sizeof(HttpHeader));
    headers[1] = malloc(sizeof(HttpHeader));
    headers[2] = NULL;
    if (!headers[0] || !headers[1]) {
        free(headers[0]);
        free(headers[1]);
        free(headers);
        return -1;
    }
    headers[0]->name = strdup("Content-Type");
    headers[0]->value = strdup("text/plain");
    headers[1]->name = strdup("Content-Length");
    headers[1]->value = strdup(contentLenStr);

    unsigned char* res = construct_HTTP_response(HTTP_STATUS_OK, "OK", headers,
            (const unsigned char*)content, contentLen, &responseLen);

    free(headers[0]->name);
    free(headers[0]->value);
    free(headers[0]);
    free(headers[1]->name);
    free(headers[1]->value);
    free(headers[1]);
    free(headers);

    if (res) {
        status = write(fd, res, (size_t)responseLen);
        free(res);
    }
    return status;
}

/**
 * @brief Send client statistics
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 * @param data OutputData
 * @return Bytes written on success, 0 if nothing sent, -1 on write failure
 */
int send_client_statistics(int fd, OutputData* data)
{
    char body[ARRAY_SIZE];
    char buffer[ARRAY_SIZE];
    uint32_t connected = 0;
    uint32_t total = 0;
    uint32_t completed = 0;
    uint32_t prompts = 0;
    uint32_t tokens = 0;

    pthread_mutex_lock(&data->lock);
    connected = data->numClientsConnected;
    total = data->totalConnectedClients;
    completed = data->completedSessions;
    prompts = data->promptsReceived;
    tokens = data->tokensEvaluated;
    pthread_mutex_unlock(&data->lock);

    /* Format counters as plain text for the /stats response body. */
    int bodyLen = snprintf(body, sizeof(body),
            "Num clients connected: %u\n"
            "Total connected clients: %u\n"
            "Completed sessions: %u\n"
            "Prompts received: %u\n"
            "Tokens evaluated: %u\n",
            connected, total, completed, prompts, tokens);
    /* Wrap statistics text in a 200 OK HTTP response. */
    int written = snprintf(buffer, sizeof(buffer),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: %d\r\n"
            "\r\n"
            "%s",
            bodyLen, body);
    if (written > 0) {
        ssize_t sendBytes = write(fd, buffer, (size_t)written);
        if (sendBytes <= 0) {
            return -1;
        }
        return sendBytes;
    }
    return 0;
}

/**
 * @brief Send client invalid address
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 * @return Bytes written on success, 0 if nothing sent, -1 on write failure
 */
int send_client_invalid_address(int fd)
{
    const char* body = "Invalid address on request line\n";
    char buffer[ARRAY_SIZE];
    size_t bodyLen = strlen(body);
    int written = snprintf(buffer, sizeof(buffer),
            "HTTP/1.1 404 Not Found\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: %zu\r\n"
            "\r\n"
            "%s",
            bodyLen, body);
    if (written > 0) {
        ssize_t sendBytes = write(fd, buffer, (size_t)written);
        if (sendBytes <= 0) {
            return -1;
        }
        return sendBytes;
    }
    return 0;
}

/**
 * @brief Send client prompt too large
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 * @param prompt Prompt
 * @return Bytes written on success, 0 if nothing sent, -1 on write failure
 */
int send_client_prompt_too_large(int fd, char* prompt)
{
    char body[ARRAY_SIZE];
    char buffer[ARRAY_SIZE];
    int bodyLen = snprintf(body, sizeof(body),
            "Input is too large: %zu bytes\n", strlen(prompt));
    int written = snprintf(buffer, sizeof(buffer),
            "HTTP/1.1 413 Content Too Large\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: %d\r\n"
            "\r\n"
            "%s",
            bodyLen, body);
    if (written > 0) {
        ssize_t sendBytes = write(fd, buffer, (size_t)written);
        if (sendBytes <= 0) {
            return -1;
        }
        return sendBytes;
    }
    return 0;
}

/**
 * @brief Append src to dest, escaping characters for a JSON string value
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param dest Output buffer
 * @param destSize Size of dest
 * @param offset Current write offset in dest (updated on success)
 * @param src Source bytes (may contain newlines, quotes, etc.)
 * @param srcLen Number of bytes to copy from src
 * @return 0 on success, -1 if dest would overflow
 */
int append_json_escaped(char* dest, size_t destSize, size_t* offset,
        const char* src, size_t srcLen)
{
    /* Expand each byte that is special inside a JSON string literal. */
    for (size_t i = 0; i < srcLen; i++) {
        const char* rep = NULL;
        for (size_t j = 0; j < JSON_ESCAPE_COUNT; j++) {
            if (src[i] == JSON_ESCAPES[j].c) {
                rep = JSON_ESCAPES[j].rep;
                break;
            }
        }
        if (rep != NULL) {
            size_t repLen = strlen(rep);
            if (*offset + repLen >= destSize) {
                return -1;
            }
            for (size_t k = 0; k < repLen; k++) {
                dest[*offset + k] = rep[k];
            }
            *offset += repLen;
        } else {
            if (*offset + 1U >= destSize) {
                return -1;
            }
            dest[(*offset)++] = src[i];
        }
    }
    dest[*offset] = '\0';
    return 0;
}

/**
 * @brief Append a literal (already JSON-escaped) fragment to a prompt buffer.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param dest Output buffer being built
 * @param destSize Size of dest in bytes
 * @param offset Current write offset in dest (updated on success)
 * @param lit Literal fragment to append
 * @return 0 on success, -1 if dest would overflow
 */
int append_literal(char* dest, size_t destSize, size_t* offset, const char* lit)
{
    size_t len = strlen(lit);
    if (*offset + len >= destSize) {
        return -1;
    }
    memcpy(dest + *offset, lit, len);
    *offset += len;
    dest[*offset] = '\0';
    return 0;
}

/**
 * @brief Append prior user/assistant turns into the ChatML prompt buffer.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @param promptBuf Prompt buffer
 * @param promptSize Size of prompt buffer
 * @param offset Current write offset in prompt buffer
 * @param history History list
 * @param historySize Maximum history blocks to retain
 * @return 0 on success, -1 on buffer overflow
 */
int append_history_to_prompt(char* promptBuf, size_t promptSize, size_t* offset,
        Block* history, int historySize)
{
    int histCount = 0;
    for (Block* tmp = history; tmp != NULL; tmp = tmp->next) {
        histCount++;
    }
    int includeCount = histCount;
    if (includeCount > historySize - 1) {
        includeCount = historySize - 1;
    }
    int skipCount = histCount - includeCount;
    Block* block = history;
    for (int i = 0; i < skipCount && block != NULL; i++) {
        block = block->next;
    }
    /* Replay each stored turn before the current user message. */
    for (; block != NULL; block = block->next) {
        if (append_literal(promptBuf, promptSize, offset, "<|im_start|>user\\n")
                < 0) {
            return -1;
        }
        if (append_json_escaped(promptBuf, promptSize, offset, block->prompt,
                    strlen(block->prompt))
                < 0) {
            return -1;
        }
        if (append_literal(promptBuf, promptSize, offset,
                    IM_END_TOKEN "\\n<|im_start|>assistant\\n")
                < 0) {
            return -1;
        }
        if (append_json_escaped(promptBuf, promptSize, offset, block->response,
                    strlen(block->response))
                < 0) {
            return -1;
        }
        if (append_literal(promptBuf, promptSize, offset, IM_END_TOKEN "\\n")
                < 0) {
            return -1;
        }
    }
    return 0;
}

/**
 * @brief Wrap a ChatML prompt string in a smollm2 completion JSON body.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param promptBuf Null-terminated inner prompt text
 * @return Newly allocated JSON string (caller frees), or NULL on failure
 */
char* wrap_prompt_in_json(const char* promptBuf)
{
    char* jsonBody = malloc(ARRAY_SIZE);
    if (!jsonBody) {
        return NULL;
    }
    memset(jsonBody, 0, ARRAY_SIZE);
    int written = snprintf(jsonBody, ARRAY_SIZE,
            "{\"prompt\": \"%s\",\"n_predict\": %d,\"cache_prompt\": false}",
            promptBuf, LLM_N_PREDICT_DEFAULT);
    if (written < 0 || (size_t)written >= ARRAY_SIZE) {
        free(jsonBody);
        return NULL;
    }
    return jsonBody;
}

/**
 * @brief Build JSON completion body including chat history and current prompt.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @param history Oldest-first list of prior user/assistant turns
 * @param historySize Maximum history blocks to retain
 * @param currentPrompt Bytes of the new user prompt
 * @param promptLen Length of currentPrompt in bytes
 * @return Newly allocated JSON string (caller frees), or NULL on failure
 */
char* build_llm_json(Block* history, int historySize, const char* currentPrompt,
        size_t promptLen)
{
    char promptBuf[ARRAY_SIZE];
    size_t offset = 0;

    memset(promptBuf, 0, sizeof(promptBuf));
    /* Start with the fixed system message block. */
    if (append_literal(promptBuf, sizeof(promptBuf), &offset,
                "<|im_start|>system\\n"
                "You are a helpful assistant." IM_END_TOKEN "\\n")
            < 0) {
        return NULL;
    }
    if (append_history_to_prompt(
                promptBuf, sizeof(promptBuf), &offset, history, historySize)
            < 0) {
        return NULL;
    }
    /* Append the current user turn and open the assistant slot. */
    if (append_literal(
                promptBuf, sizeof(promptBuf), &offset, "<|im_start|>user\\n")
            < 0) {
        return NULL;
    }
    if (append_json_escaped(
                promptBuf, sizeof(promptBuf), &offset, currentPrompt, promptLen)
            < 0) {
        return NULL;
    }
    if (append_literal(promptBuf, sizeof(promptBuf), &offset,
                IM_END_TOKEN "\\n<|im_start|>assistant\\n")
            < 0) {
        return NULL;
    }
    promptBuf[offset] = '\0';
    return wrap_prompt_in_json(promptBuf);
}

/**
 * @brief Drop the oldest block when history exceeds the allowed size.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param headPtr Pointer to history list head
 * @param tailPtr Pointer to history list tail
 * @param countPtr Pointer to number of stored blocks
 */
void history_trim_oldest(Block** headPtr, Block** tailPtr, int* countPtr)
{
    Block* old = *headPtr;
    *headPtr = old->next;
    if (*headPtr == NULL) {
        *tailPtr = NULL;
    }
    free(old->prompt);
    free(old->response);
    free(old);
    (*countPtr)--;
}

/**
 * @brief Append a prompt/response pair to per-client history.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @param headPtr Pointer to history list head
 * @param tailPtr Pointer to history list tail
 * @param countPtr Pointer to number of stored blocks
 * @param historySize Maximum blocks allowed in the list
 * @param prompt User prompt text for this turn
 * @param response Assistant response text for this turn
 */
void history_append(Block** headPtr, Block** tailPtr, int* countPtr,
        int historySize, const char* prompt, const char* response)
{
    Block* block = malloc(sizeof(Block));
    if (!block) {
        return;
    }
    block->prompt = strdup(prompt);
    block->response = strdup(response);
    block->next = NULL;
    if (*tailPtr) {
        (*tailPtr)->next = block;
    } else {
        *headPtr = block;
    }
    *tailPtr = block;
    (*countPtr)++;
    /* Remove the oldest turn when the list grows too large. */
    if (*countPtr > historySize) {
        history_trim_oldest(headPtr, tailPtr, countPtr);
    }
}

/**
 * @brief Free all blocks in a chat history list.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param head Head of the history list to free
 */
void history_free(Block* head)
{
    while (head) {
        Block* next = head->next;
        free(head->prompt);
        free(head->response);
        free(head);
        head = next;
    }
}

/**
 * @brief Build and send a ChatML JSON completion request to smollm2
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection (write via sockFd)
 * @param jsonBody Pre-built JSON request body
 * @param jsonLen Length of jsonBody in bytes
 * @return 0 on success, -1 on buffer overflow or send failure
 */
int send_request_llm(LlmConn* llm, const char* jsonBody, size_t jsonLen)
{
    char buffer[ARRAY_SIZE];
    int hLen = snprintf(buffer, sizeof(buffer),
            "POST %s HTTP/1.1\r\n"
            "Host: localhost:%d\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: %zu\r\n"
            "Connection: keep-alive\r\n"
            "\r\n",
            LLM_COMPLETION_PATH, llm->llmPort, jsonLen);
    if (hLen < 0 || (size_t)hLen + jsonLen >= sizeof(buffer)) {
        return -1;
    }
    memcpy(buffer + hLen, jsonBody, jsonLen);
    size_t total = (size_t)hLen + jsonLen;
    if (write(llm->sockFd, buffer, total) != (ssize_t)total) {
        return -1;
    }
    return 0;
}

/**
 * @brief Tear down smollm2 (SIGKILL + reap), print fatal connect-error, exit 9.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection (llmPid is used)
 */
void exit_llm_connect_failed(LlmConn* llm)
{
    kill(llm->llmPid, SIGKILL);
    waitpid(llm->llmPid, NULL, 0);
    fprintf(stderr, "uqllmserver: could not connect to the LLM\n");
    exit(EXIT_LLM_CONNECT_FAILED);
}

/**
 * @brief Send prompt to smollm2 with retries (caller must hold llm->lock).
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection
 * @param jsonBody JSON body to POST (resent on retry)
 * @param jsonLen Length of jsonBody in bytes
 * @return 1 if the request was sent, 0 otherwise
 */
int send_llm_prompt_locked(LlmConn* llm, const char* jsonBody, size_t jsonLen)
{
    if (send_request_llm(llm, jsonBody, jsonLen) == 0) {
        return 1;
    }
    /* Retry: reconnect TCP then resend (matches ai.c). */
    for (int attempt = 0; attempt < RETRY_COUNT; attempt++) {
        if (!try_connect_smollm2(llm)) {
            continue;
        }
        if (send_request_llm(llm, jsonBody, jsonLen) == 0) {
            return 1;
        }
    }
    return 0;
}

/**
 * @brief Read smollm2 HTTP response with reconnect/resend retries.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection
 * @param jsonBody JSON body to resend on reconnect
 * @param jsonLen Length of jsonBody in bytes
 * @param llmStatus Output HTTP status code from smollm2
 * @param llmStatusText Output HTTP status text (caller frees)
 * @param llmHeaders Output HTTP headers (caller frees)
 * @param llmBody Output response body (caller frees)
 * @param llmBodyLen Output length of llmBody in bytes
 * @return Non-zero from get_HTTP_response on success, 0 on failure
 */
int read_llm_response_locked(LlmConn* llm, const char* jsonBody, size_t jsonLen,
        int* llmStatus, char** llmStatusText, HttpHeader*** llmHeaders,
        unsigned char** llmBody, unsigned long* llmBodyLen)
{
    int llmResult = get_HTTP_response(llm->readFp, llmStatus, llmStatusText,
            llmHeaders, llmBody, llmBodyLen);
    if (llmResult != 0) {
        return llmResult;
    }
    /* On EOF, reconnect and resend the request before reading again. */
    for (int attempt = 0; attempt < RETRY_COUNT; attempt++) {
        free(*llmStatusText);
        *llmStatusText = NULL;
        free_array_of_headers(*llmHeaders);
        *llmHeaders = NULL;
        free(*llmBody);
        *llmBody = NULL;

        if (!try_connect_smollm2(llm)) {
            continue;
        }
        if (send_request_llm(llm, jsonBody, jsonLen) != 0) {
            continue;
        }
        llmResult = get_HTTP_response(llm->readFp, llmStatus, llmStatusText,
                llmHeaders, llmBody, llmBodyLen);
        if (llmResult != 0) {
            break;
        }
    }
    return llmResult;
}

/**
 * @brief Send decoded LLM text to the client with a trailing newline.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Client socket
 * @param content Assistant reply text (may be empty)
 * @return 0 on success, -1 on failure
 */
int deliver_llm_content_to_client(int fd, const char* content)
{
    const char* text = content ? content : "";
    size_t contentLen = strlen(text);
    int status = 0;
    char* responseBody = malloc(contentLen + 2);
    if (responseBody) {
        memcpy(responseBody, text, contentLen);
        responseBody[contentLen] = '\n';
        responseBody[contentLen + 1] = '\0';
        status = send_to_client(fd, responseBody, contentLen + 1);
        free(responseBody);
    }
    return status;
}

/**
 * @brief Strip trailing newline characters from a copied string.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param text Source string to copy and trim
 * @return Newly allocated string (caller frees), or NULL on allocation failure
 */
char* strip_trailing_newlines(const char* text)
{
    char* copy = strdup(text ? text : "");
    if (!copy) {
        return NULL;
    }
    size_t len = strlen(copy);
    while (len > 0 && (copy[len - 1] == '\n' || copy[len - 1] == '\r')) {
        copy[--len] = '\0';
    }
    return copy;
}

/**
 * @brief Send JSON to smollm2 and return the raw response body.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param llm Shared smollm2 connection
 * @param jsonBody JSON request body to POST
 * @param jsonLen Length of jsonBody in bytes
 * @param llmBodyOut Output response body (caller frees)
 */
void exchange_with_llm(LlmConn* llm, const char* jsonBody, size_t jsonLen,
        unsigned char** llmBodyOut)
{
    int llmStatus = 0;
    char* llmStatusText = NULL;
    HttpHeader** llmHeaders = NULL;
    unsigned char* llmBody = NULL;
    unsigned long llmBodyLen = 0;

    pthread_mutex_lock(&llm->lock);
    if (!ensure_smollm2(llm)) {
        pthread_mutex_unlock(&llm->lock);
        exit_llm_connect_failed(llm);
    }
    if (!send_llm_prompt_locked(llm, jsonBody, jsonLen)) {
        pthread_mutex_unlock(&llm->lock);
        exit_llm_connect_failed(llm);
    }
    /* Read the response from the LLM */
    int llmResult = read_llm_response_locked(llm, jsonBody, jsonLen, &llmStatus,
            &llmStatusText, &llmHeaders, &llmBody, &llmBodyLen);
    pthread_mutex_unlock(&llm->lock);

    free(llmStatusText);
    free_array_of_headers(llmHeaders);

    if (llmResult == 0 || !llmBody) {
        free(llmBody);
        exit_llm_connect_failed(llm);
    }
    *llmBodyOut = llmBody;
}

/**
 * @brief Store a completed prompt/response pair in client history.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param innerData Per-client context
 * @param body Request body to send to LLM
 * @param len Length of request body in bytes
 * @param content Response body from LLM
 */
void record_llm_turn(InnerData* innerData, unsigned char* body,
        unsigned long len, const char* content)
{
    char* storedResponse = strip_trailing_newlines(content);
    char* promptCopy = malloc(len + 1);
    if (promptCopy) {
        memcpy(promptCopy, body, len);
        promptCopy[len] = '\0';
        history_append(&innerData->histHead, &innerData->histTail,
                &innerData->histCount, innerData->historySize, promptCopy,
                storedResponse ? storedResponse : "");
        free(promptCopy);
    }
    free(storedResponse);
}

/**
 * @brief Handle LLM prompt: check connection, send with retries, forward
 * response.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @ai Debugging
 * @aidetails ChatGPT Used AI assistance to debug the code
 * @param fd Client file descriptor
 * @param innerData Per-client context
 * @param body Request body to send to LLM
 * @param len Length of request body in bytes
 * @return 0 on success, -1 on failure
 */
int handle_llm_prompt(
        int fd, InnerData* innerData, unsigned char* body, unsigned long len)
{
    LlmConn* llm = innerData->llm;
    char* jsonBody = build_llm_json(innerData->histHead, innerData->historySize,
            (const char*)body, (size_t)len);
    unsigned char* llmBody = NULL;
    unsigned int tokensEval = 0;
    char* content = NULL;

    if (!jsonBody) {
        return -1;
    }
    exchange_with_llm(llm, jsonBody, strlen(jsonBody), &llmBody);
    free(jsonBody);

    /* Parse assistant content and token count from the LLM JSON body. */
    json_find_uint_value((const char*)llmBody, "tokens_evaluated", &tokensEval);
    content = json_find_quoted_value((const char*)llmBody, "content");
    free(llmBody);
    if (innerData->output != NULL) {
        pthread_mutex_lock(&innerData->output->lock);
        innerData->output->tokensEvaluated += tokensEval;
        pthread_mutex_unlock(&innerData->output->lock);
    }
    if (!content) {
        content = strdup("");
    }

    record_llm_turn(innerData, body, len, content);
    deliver_llm_content_to_client(fd, content);
    free(content);
    return 0;
}

/**
 * @brief Read and handle one HTTP request from a client connection.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param innerData Per-client thread context
 * @param clientFp Buffered read stream for the client socket
 * @return 0 if the request was handled, -1 on read failure or disconnect
 */
int recv_response_from_server(InnerData* innerData, FILE* clientFp)
{
    int fd = innerData->serverFd;
    char *method, *address;
    unsigned char* body;
    unsigned long len;
    HttpHeader** headers;
    int status = -1;

    /* Read one HTTP request from the persistent client connection. */
    if (get_HTTP_request(clientFp, &method, &address, &headers, &body, &len)) {
        /* Route by method and path (/stats home page, or POST / prompt). */
        if (strcmp(method, "POST") != 0 && strcmp(method, "GET") != 0) {
            send_client_invalid_method(fd);
            status = 0;
        } else if (strcmp(address, "/stats") == 0
                && strcmp(method, "GET") != 0) {
            send_client_invalid_method(fd);
            status = 0;
        } else if (strcmp(address, "/stats") != 0
                && strcmp(address, "/") != 0) {
            send_client_invalid_address(fd);
            status = 0;
        } else if (len > MAX_PROMPT_LENGTH) {
            status = send_client_prompt_too_large(fd, (char*)body);
        } else if (strcmp(address, "/stats") == 0) {
            status = send_client_statistics(fd, innerData->output);
        } else if (strcmp(address, "/") == 0 && strcmp(method, "GET") == 0) {
            status = send_client_home_page(fd);
        } else if (strcmp(method, "POST") == 0 && strcmp(address, "/") == 0) {
            pthread_mutex_lock(&innerData->output->lock);
            innerData->output->promptsReceived++;
            pthread_mutex_unlock(&innerData->output->lock);
            status = handle_llm_prompt(fd, innerData, body, len);
        }
        free(method);
        free(address);
        free(body);
        free_array_of_headers(headers);
        return status;
    }
    return status;
}

/**
 * @brief Argument passed to stats_listen_thread.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct {
    int listenFd; /**< Stats listen socket file descriptor. */
    OutputData* output; /**< Shared statistics counters. */
} StatsListenArg;

/**
 * @brief Argument passed to stats_client_thread.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct {
    int clientFd; /**< Connected stats client socket. */
    OutputData* output; /**< Shared statistics counters. */
} StatsClientArg;

/**
 * @brief Create a listening socket on the statistics port.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param port Port number string from A3_STATS_PORT
 * @return Listening socket fd, or -1 on failure
 */
int listen_stats_port(const char* port)
{
    struct addrinfo hints;
    struct addrinfo* ai = NULL;
    /* Resolve local address for passive TCP listen on the stats port. */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    if (getaddrinfo(NULL, port, &hints, &ai) != 0) {
        return -1;
    }
    /* Create socket, bind, then start listening. */
    int serv = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (serv < 0) {
        freeaddrinfo(ai);
        return -1;
    }
    if (bind(serv, ai->ai_addr, ai->ai_addrlen) != 0) {
        close(serv);
        freeaddrinfo(ai);
        return -1;
    }
    freeaddrinfo(ai);
    if (listen(serv, LISTEN_BACKLOG) != 0) {
        close(serv);
        return -1;
    }
    return serv;
}

/**
 * @brief Handle one GET /stats request on the stats port.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected client socket (write responses here)
 * @param fp Buffered read stream for the client socket
 * @param output Shared statistics counters
 * @return 0 if handled, -1 on read failure or disconnect
 */
int handle_one_stats_request(int fd, FILE* fp, OutputData* output)
{
    char* method = NULL;
    char* address = NULL;
    unsigned char* body = NULL;
    unsigned long bodyLen = 0;
    HttpHeader** headers = NULL;
    /* Read one HTTP request from the stats client connection. */
    if (!get_HTTP_request(fp, &method, &address, &headers, &body, &bodyLen)) {
        return -1;
    }
    /* Only GET /stats is valid on the dedicated statistics port. */
    if (!method || strcmp(method, "GET") != 0) {
        send_client_invalid_method(fd);
    } else if (!address || strcmp(address, "/stats") != 0) {
        send_client_invalid_address(fd);
    } else {
        send_client_statistics(fd, output);
    }
    free(method);
    free(address);
    free(body);
    free_array_of_headers(headers);
    return 0;
}

/**
 * @brief Serve one stats client until disconnect.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param arg StatsClientArg (freed by this thread)
 * @return NULL
 */
void* stats_client_thread(void* arg)
{
    StatsClientArg* sca = (StatsClientArg*)arg;
    int clientFd = sca->clientFd;
    OutputData* output = sca->output;
    free(sca);

    FILE* clientFp = fdopen(clientFd, "r");
    if (!clientFp) {
        close(clientFd);
        return NULL;
    }
    while (handle_one_stats_request(clientFd, clientFp, output) == 0) {
    }
    fclose(clientFp);
    return NULL;
}

/**
 * @brief Accept loop for the dedicated statistics listen port.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param arg StatsListenArg (freed by this thread)
 * @return NULL (never returns in normal operation)
 */
void* stats_listen_thread(void* arg)
{
    StatsListenArg* sla = (StatsListenArg*)arg;
    int listenFd = sla->listenFd;
    OutputData* output = sla->output;
    free(sla);

    /* Accept stats clients and spawn one thread per connection. */
    while (true) {
        int clientFd = accept(listenFd, NULL, NULL);
        if (clientFd < 0) {
            continue;
        }
        StatsClientArg* sca = malloc(sizeof(StatsClientArg));
        sca->clientFd = clientFd;
        sca->output = output;
        pthread_t tid;
        pthread_create(&tid, NULL, stats_client_thread, sca);
        pthread_detach(tid);
    }
    return NULL;
}

/**
 * @brief Start the detached stats accept thread.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param statsFd Listening socket on the statistics port
 * @param data Shared statistics counters passed to client threads
 */
void start_stats_server(int statsFd, OutputData* data)
{
    StatsListenArg* sla = malloc(sizeof(StatsListenArg));
    if (!sla) {
        return;
    }
    sla->listenFd = statsFd;
    sla->output = data;
    pthread_t tid;
    pthread_create(&tid, NULL, stats_listen_thread, sla);
    pthread_detach(tid);
}

/**
 * @brief Client handling
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param argv InnerData
 * @return NULL
 */
void* client_handling(void* argv)
{
    InnerData* innerData = (InnerData*)argv;
    int clientFd = innerData->serverFd;

    innerData->histHead = NULL;
    innerData->histTail = NULL;
    innerData->histCount = 0;

    FILE* clientFp = fdopen(clientFd, "r");

    /* One clientFp for the lifetime of this TCP connection (matches spec). */
    while (true) {
        int status = recv_response_from_server(innerData, clientFp);
        if (status < 0) {
            break;
        }
    }

    fclose(clientFp);
    history_free(innerData->histHead);
    pthread_mutex_lock(&innerData->output->lock);
    innerData->output->numClientsConnected--;
    innerData->output->completedSessions++;
    pthread_mutex_unlock(&innerData->output->lock);
    sem_post(innerData->connected);
    return NULL;
}

/**
 * @brief Run the server accept loop and spawn client threads
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Listening socket file descriptor
 * @param server Shared server state (includes LLM connection)
 * @param params Parsed command-line configuration
 * @param data Statistics counters updated by client threads
 */
void run_server(int fd, ServerData* server, Params* params, OutputData* data)
{
    int cfd = -1;
    sem_t connceted;
    sem_init(&connceted, 0, (unsigned int)params->maxconns);
    /* Accept clients until the process is terminated. */
    while (true) {
        sem_wait(&connceted);
        InnerData* innerData = malloc(sizeof(InnerData));
        if (!innerData) {
            sem_post(&connceted);
            continue;
        }
        innerData->server = server;
        cfd = accept(fd, NULL, NULL);
        if (cfd < 0) {
            free(innerData);
            sem_post(&connceted);
            continue;
        }
        innerData->serverFd = cfd;
        innerData->llm = &server->llm;
        innerData->connected = &connceted;
        innerData->output = data;
        innerData->historySize = params->historysize;
        innerData->histHead = NULL;
        innerData->histTail = NULL;
        innerData->histCount = 0;
        pthread_mutex_lock(&data->lock);
        data->numClientsConnected++;
        data->totalConnectedClients++;
        pthread_mutex_unlock(&data->lock);
        pthread_t tid;
        pthread_create(&tid, NULL, client_handling, innerData);
        pthread_detach(tid);
    }
}

/**
 * @brief Handle SIGHUP signal
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param data OutputData
 */
void handle_sighup(OutputData* data)
{
    uint32_t connected = 0;
    uint32_t total = 0;
    uint32_t completed = 0;
    uint32_t prompts = 0;
    uint32_t tokens = 0;

    /* Snapshot counters under lock before printing to stderr. */
    pthread_mutex_lock(&data->lock);
    connected = data->numClientsConnected;
    total = data->totalConnectedClients;
    completed = data->completedSessions;
    prompts = data->promptsReceived;
    tokens = data->tokensEvaluated;
    pthread_mutex_unlock(&data->lock);

    fprintf(stderr, "Num clients connected: %u\n", connected);
    fprintf(stderr, "Total connected clients: %u\n", total);
    fprintf(stderr, "Completed sessions: %u\n", completed);
    fprintf(stderr, "Prompts received: %u\n", prompts);
    fprintf(stderr, "Tokens evaluated: %u\n", tokens);
    fflush(stderr);
}

/**
 * @brief Signal handler
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param arg OutputData
 * @return NULL
 */
void* signal_handler(void* arg)
{
    OutputData* data = (OutputData*)arg;
    (void)arg;
    int sig;
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGHUP);

    while (true) {
        sigwait(&set, &sig);
        if (sig == SIGHUP) {
            handle_sighup(data);
        }
    }
    return NULL;
}

/**
 * @brief Register signal handler
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param data OutputData
 */
void register_signal(OutputData* data)
{
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGHUP);
    sigaddset(&set, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    pthread_t signalThread;
    pthread_create(&signalThread, NULL, signal_handler, data);
    pthread_detach(signalThread);
}

/**
 * @brief Initialize output data
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param data OutputData
 */
void init_output_data(OutputData* data)
{
    pthread_mutex_init(&data->lock, NULL);
    data->numClientsConnected = 0;
    data->totalConnectedClients = 0;
    data->completedSessions = 0;
    data->promptsReceived = 0;
    data->tokensEvaluated = 0;
}

/**
 * @brief Listen on A3_STATS_PORT when set; exit 12 if listen fails.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param mainFd Main server listen fd (closed on stats listen failure)
 * @return Stats listen fd, or -1 if the environment variable is unset
 */
int listen_stats_from_env(int mainFd)
{
    const char* statsPortStr = getenv("A3_STATS_PORT");
    if (statsPortStr == NULL || statsPortStr[0] == '\0') {
        return -1;
    }
    int statsFd = listen_stats_port(statsPortStr);
    if (statsFd < 0) {
        close(mainFd);
        fprintf(stderr,
                "uqllmserver: cannot listen on given statistics port "
                "\"%s\"\n",
                statsPortStr);
        exit(EXIT_LISTEN_STATS_PORT);
    }
    return statsFd;
}

/**
 * @brief Allocate and initialise shared ServerData.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @return New ServerData, or NULL on allocation failure
 */
ServerData* alloc_server_data(void)
{
    ServerData* server = malloc(sizeof(ServerData));
    memset(server, 0, sizeof(*server));
    pthread_mutex_init(&server->lock, NULL);
    sem_init(&server->await, 0, 0);
    server->llm.sockFd = -1;
    server->llm.readFp = NULL;
    pthread_mutex_init(&server->llm.lock, NULL);
    return server;
}

/**
 * @brief Start smollm2 and connect the shared LLM socket.
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param server Server state whose llm field is updated
 */
void start_llm_for_server(ServerData* server)
{
    int llmPid = 0;
    int smollmPort = start_smollm(&llmPid);
    server->llm.llmPid = llmPid;
    server->llm.llmPort = smollmPort;
    if (!connect_smollm2(&server->llm)) {
        kill(server->llm.llmPid, SIGKILL);
        waitpid(server->llm.llmPid, NULL, 0);
        fprintf(stderr, "uqllmserver: could not connect to the LLM\n");
        exit(EXIT_LLM_CONNECT_FAILED);
    }
}

/**
 * @brief Program entry: parse CLI and initialize the server
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param argc Standard argc
 * @param argv Standard argv
 * @return Zero on success (not all paths return; may exit())
 */
int main(int argc, char* argv[])
{
    Params params;
    init_params(&params);
    check_arguments(argc, argv, &params);
    /* Listen on the main port, then optionally on A3_STATS_PORT. */
    int fd = check_port(params.port);
    int statsFd = listen_stats_from_env(fd);

    ServerData* server = alloc_server_data();
    start_llm_for_server(server);

    OutputData* data = malloc(sizeof(OutputData));
    init_output_data(data);
    if (statsFd >= 0) {
        start_stats_server(statsFd, data);
    }
    /* Handle SIGHUP and run the main client accept loop. */
    register_signal(data);
    run_server(fd, server, &params, data);
    close(fd);
    return 0;
}
