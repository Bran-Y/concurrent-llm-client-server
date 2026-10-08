/** @file uqllmclient.c
 * @author s4916521
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @aitool ChatGPT
 *
 * This program provides a client for the uqllmserver.
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

#define INCORRECT_NUMBER_OF_ARGUMENTS 10
#define USER_EOF 0
#define COMMUNICATION_ERROR_WITH_THE_SERVER 17
#define CANNOT_CONNECT_TO_THE_SERVER 11
#define EMPTY_STRING_FOR_PORT 15
#define LOCAL_HOST "localhost"
#define MAX_PROMPT_LENGTH 16384
#define RESPONSE_OK 200

/**
 * @brief Program configuration: port
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
typedef struct Params {

    bool portSpecified; /**< True if port was given on the command line. */
    char* port; /**< Port number from argv. */
} Params;

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
}

/**
 * @brief Display usage information and exit
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
void show_usage_and_exit()
{
    fprintf(stderr, "Usage: uqllmclient port\n");
    exit(INCORRECT_NUMBER_OF_ARGUMENTS);
}

/**
 * @brief Parse command-line argv into port
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param argc Argument count from main
 * @param argv Argument vector from main
 * @param params Output Params populated from argv
 */
void check_arguments(int argc, char* argv[], Params* params)
{
    if (argc != 2) {
        show_usage_and_exit();
    }
    if (strlen(argv[1]) != 0) {
        params->port = argv[1];
        params->portSpecified = true;
    } else {
        fprintf(stderr, "uqllmclient: bad port\n");
        exit(EMPTY_STRING_FOR_PORT);
    }
}

/**
 * @brief Print server connection error and exit
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
void print_port_error(void)
{
    fprintf(stderr, "uqllmclient: unable to connect to the server\n");
    exit(CANNOT_CONNECT_TO_THE_SERVER);
}

/**
 * @brief Check port
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param port Port number
 * @return Connected file descriptor
 */
int check_port(char* port)
{
    struct addrinfo* ai = 0;
    struct addrinfo hints = {0};
    /* Build hints for IPv4 TCP and resolve host:port. */
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int err = getaddrinfo(LOCAL_HOST, port, &hints, &ai);
    if (err) {
        /* Name lookup failed; free any partial result and abort. */
        if (ai) {
            freeaddrinfo(ai);
        }
        print_port_error();
    }
    /* Open socket and connect; on failure release resources and abort. */
    int connectedFd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(connectedFd, ai->ai_addr, sizeof(struct sockaddr))) {
        freeaddrinfo(ai);
        close(connectedFd);
        print_port_error();
    }
    freeaddrinfo(ai);
    return connectedFd;
}

/**
 * @brief Print communication error and exit
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 */
void print_communication_error(void)
{
    fprintf(stderr, "uqllmclient: communication error\n");
    exit(COMMUNICATION_ERROR_WITH_THE_SERVER);
}

/**
 * @brief Send post to server
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 * @param body Body to send
 */
void send_post_to_server(int fd, char* body)
{
    char buffer[MAX_PROMPT_LENGTH];
    size_t bodyLen = strlen(body);
    int written = snprintf(buffer, sizeof(buffer),
            "POST / HTTP/1.1\r\n"
            "Content-Length: %zu\r\n"
            "\r\n"
            "%s",
            bodyLen, body);
    ssize_t sendBytes = send(fd, buffer, (size_t)written, 0);
    if (sendBytes <= 0) {
        print_communication_error();
    }
}

/**
 * @brief Receive response from server
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 */
void recv_response_from_server(int fd)
{
    int status;
    char* statusExplanation;
    unsigned char* body;
    unsigned long len;
    HttpHeader** headers;
    FILE* f = fdopen(fd, "r");
    if (get_HTTP_response(
                f, &status, &statusExplanation, &headers, &body, &len)) {
        if (status == RESPONSE_OK) {
            fprintf(stdout, "Response:\n");
            fprintf(stdout, "%s", body);
            fflush(stdout);
        } else {
            /* Valid HTTP error (e.g. 413): print body to stderr, not a
             * communication error. */
            fprintf(stderr, "Response:\n");
            fprintf(stderr, "%s", body);
            fflush(stderr);
        }
        free(statusExplanation);
        free(body);
        free_array_of_headers(headers);
    } else {
        print_communication_error();
    }
}

/**
 * @brief Run the client behaviour
 * @ai Wrote Comments
 * @aidetails ChatGPT Used AI assistance to refine the function documentation
 * comments
 * @param fd Connected file descriptor
 */
void run_behaviour(int fd)
{
    char prompt[MAX_PROMPT_LENGTH];
    fprintf(stdout, "Welcome to uqllmclient.\n");
    fprintf(stdout, "This program was written by s4916521.\n");
    fflush(stdout);
    while (true) {
        fprintf(stdout, "Enter your prompt:\n");
        fflush(stdout);
        memset(prompt, 0, sizeof(prompt));
        fgets(prompt, sizeof(prompt), stdin);
        if (feof(stdin)) {
            break;
        }
        // prompt[strcspn(prompt, "\n")] = 0;
        if (strlen(prompt) == 0) {
            continue;
        }
        send_post_to_server(fd, prompt);
        recv_response_from_server(fd);
    }
}

/**
 * @brief Program entry: parse CLI and connect to the server
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
    int fd = check_port(params.port);
    run_behaviour(fd);
    close(fd);
    return 0;
}
