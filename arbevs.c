#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <pthread.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MAX_PENDING_CONNECTIONS 32
#define MAX_EVENTS 32
#define MAX_CLIENT_READ_BUFFER_SIZE 0x100000
#define CLIENT_READ_BUFFER_WINDOW 512
#define SERVER_NAME "test"
#define READLINE_INITAL_BUFFER_SIZE 8
#define READLINE_BUFFER_STEP_SIZE 32
#define MAX_CLIENT_WRITE_BUFFER_SIZE 0x4000


struct client_message
{
    struct client_context *client;
    size_t count;
    void *data;
};

struct client_context
{
    int fd;
    socklen_t addr_length;
    struct sockaddr_in addr;
    size_t read_buffer_length;
    char *read_buffer;
    size_t write_buffer_length;
    char *write_buffer;
    pthread_mutex_t write_buffer_mutex;
    void (*message_handler)(struct client_message msg);
};

struct server_context
{
    int fd;
    struct sockaddr_in addr;
};

struct pending_read
{
    struct client_context *client;
    size_t count;
    char data[];
};

static int epoll_fd;
static struct epoll_event ev, events[MAX_EVENTS];
static struct server_context server;

// static pthread_mutex_t pending_reads_mutex = PTHREAD_MUTEX_INITIALIZER;
static size_t pending_reads_size = 0;
static struct pending_read *pending_reads = NULL;

static void append_pending_read(
    struct client_context *client,
    size_t count,
    void *data)
{
    // pthread_mutex_lock(&pending_reads_mutex);
    int total_count = sizeof(struct pending_read) + count;
    pending_reads = realloc(
        pending_reads, 
        pending_reads_size
            + total_count);
    assert(pending_reads);
    struct pending_read *cursor = (struct pending_read *)
        ((char *)pending_reads + pending_reads_size);
    cursor->client = client;
    cursor->count = count;
    memcpy(cursor->data, data, count);
    pending_reads_size += total_count;
    // pthread_mutex_unlock(&pending_reads_mutex);
}

static void scan_pending_reads(void)
{
    // pthread_mutex_lock(&pending_reads_mutex);
    if (pending_reads_size <= sizeof(struct pending_read))
        return;
    for (struct pending_read *cursor = pending_reads;
        cursor < (struct pending_read *)
            ((char *)pending_reads + pending_reads_size);
        cursor = (struct pending_read *)
            ((char *)cursor 
                + sizeof(struct pending_read) 
                + cursor->count))
    { 
        struct client_context *client = cursor->client;
        size_t count = cursor->count;
        char *delim = memchr(cursor->data, '\n', cursor->count);
        if (!delim)
        {
            client->read_buffer = realloc(
                client->read_buffer,
                client->read_buffer_length + count + 1);
            client->read_buffer[client->read_buffer_length + count] = 0;
            assert(client->read_buffer);
            memcpy(client->read_buffer, cursor->data, count);
            client->read_buffer_length += count;
            continue;
        }
        size_t tail_count = (uint64_t)delim - (uint64_t)cursor->data;
        size_t message_count = client->read_buffer_length + tail_count;
        char *message = (char *)calloc(message_count + 1, sizeof(char));
        memcpy(message, client->read_buffer, client->read_buffer_length);
        memcpy(
            message + client->read_buffer_length, 
            cursor->data, 
            tail_count);
        size_t leftover_count = count - tail_count - 1;
        if (leftover_count)
        {
            client->read_buffer = realloc(
                client->read_buffer,
                leftover_count + 1);
            client->read_buffer[leftover_count] = 0;
            assert(client->read_buffer);
            memcpy(
                client->read_buffer, 
                cursor->data + tail_count + 1, 
                leftover_count);
        }
        client->read_buffer_length = leftover_count;
        client->message_handler((struct client_message)
        {
            .client = client,
            .count = message_count,
            .data = (void *)message,
        });
    }
    pending_reads_size = 0;
    // pthread_mutex_unlock(&pending_reads_mutex);
}

static void message_client(
    struct client_context *client,
    size_t length, 
    void *data,
    bool free_data)
{
    assert(!pthread_mutex_lock(&client->write_buffer_mutex));
    client->write_buffer = realloc(
        client->write_buffer,
        client->write_buffer_length + length + 2);
    client->write_buffer[client->write_buffer_length + length] = '\n';
    client->write_buffer[client->write_buffer_length + length + 1] = 0;
    assert(client->write_buffer);
    memcpy(client->write_buffer + client->write_buffer_length,
        data, length);
    client->write_buffer_length += length + 1;
    assert(!pthread_mutex_unlock(&client->write_buffer_mutex));
    struct epoll_event _ev;
    _ev.data.ptr = client;
    _ev.events = EPOLLOUT | EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client->fd, &_ev);
    if (free_data)
        free(data);
}

static void set_non_blocking(int fd)
{
	int opts;
	opts = fcntl(fd, F_GETFL);
	assert(opts >= 0);
	opts = opts | O_NONBLOCK;
	assert(fcntl(fd, F_SETFL, opts) >= 0);
}

static void print_client_message(struct client_message msg)
{
    printf("%p %s\n", msg.client, (char *)msg.data);
}

static struct client_context *new_client_context(
    int fd,
    socklen_t addr_length,
    struct sockaddr_in addr)
{
    struct client_context *g = (struct client_context *)
        calloc(1, sizeof(struct client_context));
    assert(g);
    *g = (struct client_context)
    {
        .fd = fd,
        .addr_length = addr_length,
        .addr = addr,
        .write_buffer_mutex = PTHREAD_MUTEX_INITIALIZER,
        .message_handler = print_client_message,
    };
    assert(!pthread_mutex_init(&g->write_buffer_mutex, NULL));
    return g;
}

static void free_client_context(struct client_context *g, bool close_fd)
{
    assert(g);
    if (close_fd)
        close(g->fd);
    if (g->write_buffer)
        free(g->write_buffer);
    assert(!pthread_mutex_destroy(&g->write_buffer_mutex));
    free(g);
}

static void handle_connection(struct epoll_event event)
{
    struct sockaddr_in addr;
    socklen_t addr_length = sizeof(addr);
    int conn_fd = accept(
        server.fd, 
        (struct sockaddr *)&addr, 
        &addr_length);
    assert(conn_fd >= 0);
    set_non_blocking(conn_fd);
    ev.data.ptr = new_client_context(conn_fd, addr_length, addr);
    ev.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev);
}

static void handle_incoming(struct epoll_event event)
{
    struct client_context *client = 
        (struct client_context *)event.data.ptr;
    int client_fd = client->fd;
    ssize_t count;
    char buf[CLIENT_READ_BUFFER_WINDOW];
    for (;;)
    {
        count = read(client_fd, buf, sizeof(buf));
        if (count < 0)
        {
            if (errno != EAGAIN)
            {
                free_client_context(client, true);
            }
            break;
        }
        if (!count || (count == 1 && *buf == '\n'))
        {
            free_client_context(client, true);
            break;
        }
        // client->read_buffer = realloc(
        //     client->read_buffer,
        //     client->read_buffer_length + count);
        // memcpy(client->read_buffer, buf, count);
        // client->read_buffer_length += count;
        append_pending_read(client, count, buf);
    }
    ev.data.ptr = client;
    ev.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
}

static void handle_outgoing(struct epoll_event event)
{
    struct client_context *client = 
        (struct client_context *)event.data.ptr;
    assert(!pthread_mutex_lock(&client->write_buffer_mutex));
    write(
        client->fd, 
        (const void *)client->write_buffer,
        client->write_buffer_length);
    client->write_buffer_length = 0;
    free(client->write_buffer);
    assert(!pthread_mutex_unlock(&client->write_buffer_mutex));
    ev.data.ptr = client;
    ev.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client->fd, &ev);
}

static void event_switch(struct epoll_event event)
{
    if (event.data.ptr == &server)
    {
        handle_connection(event);
        return;
    }
    if (event.events & EPOLLIN)
    {
        handle_incoming(event);
        // message_client(
        //     (struct client_context *)event.data.ptr, 
        //     5, "test\n", 
        //     false);
        return;
    }
    if (event.events & EPOLLOUT)
    {
        handle_outgoing(event);
        return;
    }
    assert(false);
}

static void _loop(void)
{
    int wait_count = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
    for(int i = 0; i < wait_count; ++i)
    {
        event_switch(events[i]);
    }
    scan_pending_reads();
}

static void loop(void)
{
    for (;;_loop()) 
    { }
}

static void *server_thread(void *_)
{
    epoll_fd = epoll_create1(0);
    server.fd = socket(AF_INET, SOCK_STREAM, 0);
    set_non_blocking(server.fd);

    ev.data.ptr = &server;
    ev.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server.fd, &ev);

    memset(&server.addr, 0, sizeof(server.addr));
    server.addr.sin_family = AF_INET;
    server.addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server.addr.sin_port = htons(49831);
    bind(server.fd, (struct sockaddr *)&server.addr, sizeof(server.addr));
    listen(server.fd, MAX_PENDING_CONNECTIONS);
    loop();
    pthread_exit(NULL);
    return NULL;
}

static size_t readline(FILE *source, char **linebuf)
{
    *linebuf = (char *)calloc(READLINE_INITAL_BUFFER_SIZE, sizeof(char));
    int i = 0;
    for (int c = fgetc(source); 
        c != EOF && c != '\n'; 
        i++, c = fgetc(source))
    {
        (*linebuf)[i] = (char)c;
        if (i == READLINE_INITAL_BUFFER_SIZE - 1)
        {
            *linebuf = (char *)
                realloc(*linebuf, READLINE_BUFFER_STEP_SIZE);
                memset(*linebuf + i + 1, 0, 
                    READLINE_BUFFER_STEP_SIZE - READLINE_INITAL_BUFFER_SIZE);
            continue;
        }
        if (i % READLINE_BUFFER_STEP_SIZE == READLINE_BUFFER_STEP_SIZE - 1)
        {
            *linebuf = (char *)
                realloc(*linebuf, i + READLINE_BUFFER_STEP_SIZE + 1);
                memset(*linebuf + i + 1, 0, READLINE_BUFFER_STEP_SIZE);
            continue;
        }
    }
    return i;
}

int main(int argc, char **argv)
{
    pthread_t server;
    pthread_create(&server, NULL, server_thread, NULL);
    char *line;

    for (;;)
    {
        assert(readline(stdin, &line));
        void *ptr;
        assert(sscanf(line, "%p", &ptr));
        char *str = (char *)memchr(line, ' ', 18);
        assert(str);
        str += 1;
        size_t length = strlen(str);
        // the client ptr is not verified ...
        struct client_context *client = (struct client_context *)ptr;
        message_client(client, length, (void *)str, false);
    }
    
    free(line);
    void *_;
    pthread_join(server, &_);
    pthread_exit(NULL);
    return 0;
}


