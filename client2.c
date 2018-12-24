#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/un.h>
#include <sys/time.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>

#define DEFAULT_CONNECT_INTERVAL 200l
#define DEFAULT_CONNECT_TRIES 5l
#define SOCK_BUFFER_SIZE 512

union address_t {
	struct sockaddr_in ip4;
	struct sockaddr_in6 ip6;
	struct sockaddr_un unixx;
};

void usage(char *self);
void parse_addr(char *self, char *spec, const char *side, union address_t *address, socklen_t *size);
int do_server(char *self, const char *side, union address_t *address, socklen_t size, const char *spec,
		char** argv, int command_start, int command_end, const char *key,
		long connect_interval, long connect_tries);
void start_server(char *self, const char *side, char** argv, int command_start, int command_end, const char *key);
void cross_the_streams(char *self, int left_sock, int right_sock);

/* client2
 *   <left-address> <right-address>
 *   <connect-interval> <connect-tries>
 *   [left-command... [';' [left-key [right-command... [';' [right-key]]]]]]
 */
int main(int argc, char** argv) {
	union address_t left_addr, right_addr;
	socklen_t left_addr_size, right_addr_size;
	long connect_interval, connect_tries;
	char *endptr;
	int left_command_start, left_command_end;
	const char *left_key, *right_key;
	int right_command_start, right_command_end;
	int left_sock, right_sock;
	if(argc < 5)
		usage(*argv);
	parse_addr(*argv, argv[1], "left", &left_addr, &left_addr_size);
	parse_addr(*argv, argv[2], "right", &right_addr, &right_addr_size);
	connect_interval = strtol(argv[3], &endptr, 0);
	if(!*argv[3] || *endptr) {
		fprintf(stderr, "%s: Malformed connect interval: %s\n", basename(*argv), argv[3]);
		return 1;
	}
	if(connect_interval <= 0l)
		connect_interval = DEFAULT_CONNECT_INTERVAL;
	connect_tries = strtol(argv[4], &endptr, 0);
	if(!*argv[4] || *endptr) {
		fprintf(stderr, "%s: Malformed number connect attempts: %s\n", basename(*argv), argv[4]);
		return 1;
	}
	if(connect_tries <= 0l)
		connect_tries = DEFAULT_CONNECT_TRIES;
	for(left_command_start = left_command_end = 5; left_command_end < argc; ++left_command_end) {
		if(!strcmp(argv[left_command_end], ";"))
			break;
	}
	if(left_command_end + 1 >= argc) {
		left_key = NULL;
		right_command_start = right_command_end = argc;
	}
	else {
		left_key = argv[left_command_end + 1];
		right_command_start = right_command_end = left_command_end + 2;
	}
	for(; right_command_end < argc; ++right_command_end) {
		if(!strcmp(argv[right_command_end], ";"))
			break;
	}
	if(right_command_end + 1 >= argc)
		right_key = NULL;
	else if(right_command_end + 2 < argc) {
		fprintf(stderr, "%s: Excess command line arguments after right semaphore key\n", basename(*argv));
		return 1;
	}
	else
		right_key = argv[right_command_end + 1];
	left_sock = do_server(*argv, "left", &left_addr, left_addr_size, argv[1], argv,
			left_command_start, left_command_end, left_key, connect_interval, connect_tries);
	right_sock = do_server(*argv, "right", &right_addr, right_addr_size, argv[2], argv,
			right_command_start, right_command_end, right_key, connect_interval, connect_tries);
	if(daemon(0, 1)) {
		fprintf(stderr, "%s: Failed to daemonize: %s\n", basename(*argv), strerror(errno));
		return 1;
	}
	cross_the_streams(*argv, left_sock, right_sock);
	return 0;
}

void usage(char *self) {
	fprintf(stderr, "Usage: %s "
			"<left-address> <right-address> "
			"<connect-interval> <connect-tries> "
			"[left-command... [';' [left-key [right-command... [';' [right-key]]]]]]\n",
			basename(self));
	exit(1);
}

void parse_addr(char *self, char *spec, const char *side, union address_t *address, socklen_t *size) {
	char *sep, *endptr;
	size_t length;
	long pnum;
	length = strlen(spec);
	if(length >= (size_t)4 && !memcmp(spec, "tcp:", (size_t)4)) {
		spec += 4;
		sep = strchr(spec, '/');
		if(!sep) {
			fprintf(stderr, "%s: TCP4 address for %s server is missing the '/' separator: %s\n",
					basename(self), side, spec);
			exit(1);
		}
		*sep = '\0';
		if(inet_pton(AF_INET, spec, &address->ip4.sin_addr) != 1) {
			fprintf(stderr, "%s: Illegal IPv4 address for %s server: %s\n", basename(self), side, spec);
			exit(1);
		}
		address->ip4.sin_family = AF_INET;
		*size = (socklen_t)sizeof(struct sockaddr_in);
		pnum = strtol(++sep, &endptr, 0);
		if(!*sep || *endptr) {
			fprintf(stderr, "%s: Malformed port number for %s server: %s\n", basename(self), side, sep);
			exit(1);
		}
		if(pnum <= 0l || pnum > 65535l) {
			fprintf(stderr, "%s: Port number for %s sever out of range: %ld\n", basename(self), side, pnum);
			exit(1);
		}
		address->ip4.sin_port = htons((uint16_t)pnum);
	}
	else if(length >= (size_t)5 && !memcmp(spec, "tcp6:", (size_t)5)) {
		spec += 5;
		sep = strchr(spec, '/');
		if(!sep) {
			fprintf(stderr, "%s: TCP6 address for %s server is missing the '/' separator: %s\n",
					basename(self), side, spec);
			exit(1);
		}
		*sep = '\0';
		if(inet_pton(AF_INET6, spec, &address->ip6.sin6_addr) != 1) {
			fprintf(stderr, "%s: Illegal IPv6 address for %s server: %s\n", basename(self), side, spec);
			exit(1);
		}
		address->ip6.sin6_family = AF_INET6;
		*size = (socklen_t)sizeof(struct sockaddr_in6);
		pnum = strtol(++sep, &endptr, 0);
		if(!*sep || *endptr) {
			fprintf(stderr, "%s: Malformed port number for %s server: %s\n", basename(self), side, sep);
			exit(1);
		}
		if(pnum <= 0l || pnum > 65535l) {
			fprintf(stderr, "%s: Port number for %s sever out of range: %ld\n", basename(self), side, pnum);
			exit(1);
		}
		address->ip6.sin6_port = htons((uint16_t)pnum);
	}
	else if(length >= (size_t)5 && !memcmp(spec, "unix:", (size_t)4)) {
		spec += 5;
		if(!*spec) {
			fprintf(stderr, "%s: Empty UNIX address for %s server\n", basename(self), side);
			exit(1);
		}
		if(length - (size_t)4 >= sizeof(address->unixx.sun_path)) {
			fprintf(stderr, "%s: UNIX address for %s server is too long: %s\n", basename(self), side, spec);
			exit(1);
		}
		address->unixx.sun_family = AF_UNIX;
		strcpy(address->unixx.sun_path, spec);
		*size = (socklen_t)sizeof(struct sockaddr_un);
	}
	else {
		fprintf(stderr, "%s: Unrecognized socket address format for %s server: %s\n", basename(self), side, spec);
		exit(1);
	}
}

int do_server(char *self, const char *side, union address_t *address, socklen_t size, const char *spec,
		char** argv, int command_start, int command_end, const char *key,
		long connect_interval, long connect_tries) {
	int sock;
	long attempt;
	if(command_end > command_start)
		start_server(self, side, argv, command_start, command_end, key);
	sock = socket(address->ip4.sin_family, SOCK_STREAM, 0);
	if(sock == -1) {
		fprintf(stderr, "%s: Failed to create socket for %s server: %s\n", basename(self), side, strerror(errno));
		exit(1);
	}
	if(fcntl(sock, F_SETFL, O_NONBLOCK)) {
		fprintf(stderr, "%s: Failed to set O_NONBLOCK on socket for %s server: %s\n",
				basename(self), side, strerror(errno));
	}
	for(attempt = 0l; attempt < connect_tries; ++attempt) {
		usleep(connect_interval * (long)1000);
		if(!connect(sock, (const struct sockaddr*)address, size))
			break;
	}
	if(attempt >= connect_tries) {
		fprintf(stderr, "%s: Failed to connect to %s server on address %s with %ld attempts %ld msec apart: %s\n",
				basename(self), side, spec, connect_tries, connect_interval, strerror(errno));
		exit(1);
	}
	return sock;
}

void start_server(char *self, const char *side, char** argv, int command_start, int command_end, const char *key) {
	sem_t *sem;
	char *sem_name;
	char **child_argv;
	pid_t pid;
	if(!key)
		sem = NULL;
	else {
		sem_name = (char*)malloc((size_t)26 + strlen(key));
		if(!sem_name) {
			fprintf(stderr, "%s: Out of memory to allocate semaphore name\n", basename(self));
			exit(1);
		}
		strcpy(sem_name, "/org.unclesniper.client2.");
		strcpy(sem_name + 25, key);
		sem = sem_open(sem_name, O_CREAT | O_EXCL);
		if(sem == SEM_FAILED) {
			switch(errno) {
				case EEXIST:
					sem = NULL;
					break;
				case ENAMETOOLONG:
					fprintf(stderr, "%s: Semaphore name for %s server is too long\n", basename(self), side);
					exit(1);
				default:
					fprintf(stderr, "%s: Failed to check semaphore for %s server: %s\n",
							basename(self), side, strerror(errno));
					exit(1);
			}
		}
	}
	child_argv = (char**)malloc((size_t)(command_end - command_start + 1) * sizeof(char*));
	if(!child_argv) {
		fprintf(stderr, "%s: Out of memory to allocate %s server argv\n", basename(self), side);
		if(sem)
			sem_unlink(sem_name);
		exit(1);
	}
	memcpy(child_argv, argv + command_start, (size_t)(command_end - command_start) * sizeof(char*));
	child_argv[command_end - command_start] = NULL;
	if(sem) {
		pid = fork();
		if(pid == (pid_t)-1) {
			fprintf(stderr, "%s: Failed to fork(2): %s\n", basename(self), strerror(errno));
			if(sem)
				sem_unlink(sem_name);
			exit(1);
		}
		if(pid) {
			free(child_argv);
			return; /* everything else is the child's dealio */
		}
	}
	pid = fork();
	if(pid == (pid_t)-1) {
		fprintf(stderr, "%s: %sFailed to fork(2): %s\n", basename(self),
				sem ? "Intermediate sem_unlink(3) process: " : "", strerror(errno));
		if(sem)
			sem_unlink(sem_name);
		exit(1);
	}
	if(pid) {
		free(child_argv);
		return; /* again, the parent did everything it could */
	}
	execvp(*child_argv, child_argv);
	fprintf(stderr, "%s: Future %s server process: Failed to execvp(3): %s\n",
			basename(self), side, strerror(errno));
	if(sem)
		sem_unlink(sem_name);
	exit(1);
}

#define LEFT_AT_END  01
#define RIGHT_AT_END 02

void cross_the_streams(char *self, int left_sock, int right_sock) {
	fd_set readfds, writefds;
	int nfds;
	char from_left_buffer[SOCK_BUFFER_SIZE], from_right_buffer[SOCK_BUFFER_SIZE];
	size_t from_left_offset, from_right_offset;
	size_t from_left_size, from_right_size;
	ssize_t count;
	int at_end;
	nfds = (left_sock > right_sock ? left_sock : right_sock) + 1;
	from_left_offset = from_right_offset = (size_t)0;
	from_left_size = from_right_size = (size_t)0;
	at_end = 0;
	while(at_end != (LEFT_AT_END | RIGHT_AT_END)) {
		FD_ZERO(&readfds);
		FD_ZERO(&writefds);
		if(from_left_offset < from_left_size)
			FD_SET(right_sock, &writefds);
		else if(!(at_end & LEFT_AT_END))
			FD_SET(left_sock, &readfds);
		if(from_right_offset < from_right_size)
			FD_SET(left_sock, &writefds);
		else if(!(at_end & RIGHT_AT_END))
			FD_SET(right_sock, &readfds);
		if(select(nfds, &readfds, &writefds, NULL, NULL) == -1) {
			fprintf(stderr, "%s: Failed to select(2) the sockets: %s\n", basename(self), strerror(errno));
			exit(1);
		}
		if(FD_ISSET(left_sock, &readfds)) {
			count = read(left_sock, from_left_buffer, sizeof(from_left_buffer));
			if(count == (ssize_t)-1) {
				if(errno != EAGAIN && errno != EWOULDBLOCK) {
					fprintf(stderr, "%s: Failed to read from left server: %s\n", basename(self), strerror(errno));
					exit(1);
				}
			}
			else if(!count) {
				if(shutdown(left_sock, SHUT_RD)) {
					fprintf(stderr, "%s: Failed to shutdown(2) left server socket: %s\n",
							basename(self), strerror(errno));
					exit(1);
				}
				if(shutdown(right_sock, SHUT_WR)) {
					fprintf(stderr, "%s: Failed to shutdown(2) right server socket: %s\n",
							basename(self), strerror(errno));
					exit(1);
				}
				at_end |= LEFT_AT_END;
			}
			else {
				from_left_offset = (size_t)0;
				from_left_size = (size_t)count;
			}
		}
		if(FD_ISSET(right_sock, &readfds)) {
			count = read(right_sock, from_right_buffer, sizeof(from_right_buffer));
			if(count == (ssize_t)-1) {
				if(errno != EAGAIN && errno != EWOULDBLOCK) {
					fprintf(stderr, "%s: Failed to read from right server: %s\n", basename(self), strerror(errno));
					exit(1);
				}
			}
			else if(!count) {
				if(shutdown(right_sock, SHUT_RD)) {
					fprintf(stderr, "%s: Failed to shutdown(2) right server socket: %s\n",
							basename(self), strerror(errno));
					exit(1);
				}
				if(shutdown(left_sock, SHUT_WR)) {
					fprintf(stderr, "%s: Failed to shutdown(2) left server socket: %s\n",
							basename(self), strerror(errno));
					exit(1);
				}
				at_end |= RIGHT_AT_END;
			}
			else {
				from_right_offset = (size_t)0;
				from_right_size = (size_t)count;
			}
		}
		if(FD_ISSET(left_sock, &writefds)) {
			count = write(left_sock, from_right_buffer + from_right_offset, from_right_size - from_right_offset);
			if(count == (ssize_t)-1) {
				if(errno != EAGAIN && errno != EWOULDBLOCK) {
					fprintf(stderr, "%s: Failed to write to left server: %s\n", basename(self), strerror(errno));
					exit(1);
				}
			}
			else
				from_right_offset += (size_t)count;
		}
		if(FD_ISSET(right_sock, &writefds)) {
			count = write(right_sock, from_left_buffer + from_left_offset, from_left_size - from_left_offset);
			if(count == (ssize_t)-1) {
				if(errno != EAGAIN && errno != EWOULDBLOCK) {
					fprintf(stderr, "%s: Failed to write to right server: %s\n", basename(self), strerror(errno));
					exit(1);
				}
			}
			else
				from_left_offset += (size_t)count;
		}
	}
	if(close(left_sock)) {
		fprintf(stderr, "%s: Failed to close(2) left server socket: %s\n", basename(self), strerror(errno));
		exit(1);
	}
	if(close(right_sock)) {
		fprintf(stderr, "%s: Failed to close(2) right server socket: %s\n", basename(self), strerror(errno));
		exit(1);
	}
}
