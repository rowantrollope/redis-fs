# Redis FS Module - Makefile
# Modeled after Redis vectorsets module build system

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

CC = cc
CFLAGS = -O2 -Wall -Wextra -Wno-unused-parameter -g $(SAN) -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE
LDFLAGS = -lm $(SAN)

ifeq ($(uname_S),Linux)
    SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -std=c11 -O2
    SHOBJ_LDFLAGS ?= -shared
else
    SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c11 -O3
    SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

.SUFFIXES: .c .xo .so

all: fs.so

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

fs.xo: fs.c fs.h path.h redismodule.h
path.xo: path.c path.h

fs.so: fs.xo path.xo
	$(CC) -o $@ $^ $(SHOBJ_LDFLAGS) $(LDFLAGS) -lc

clean:
	rm -f *.xo *.so

# E2E test: start Redis with module, run tests, then shut down
TEST_PORT ?= 6399
TEST_PIDFILE = /tmp/redis-fs-test.pid

test: fs.so
	@echo "Starting Redis on port $(TEST_PORT) with fs.so..."
	@redis-server --port $(TEST_PORT) --loadmodule $(PWD)/fs.so \
		--daemonize yes --pidfile $(TEST_PIDFILE) \
		--loglevel warning --save "" --appendonly no
	@sleep 0.5
	@echo "Running tests..."
	@python3 test.py --port $(TEST_PORT); \
		EXIT_CODE=$$?; \
		echo "Stopping Redis..."; \
		kill `cat $(TEST_PIDFILE)` 2>/dev/null || true; \
		rm -f $(TEST_PIDFILE); \
		exit $$EXIT_CODE

# Install skill to all detected agents (requires Node.js/npx)
install-skill:
	@echo "Installing redis-fs skill to all detected agents..."
	npx skills add . --skill redis-fs -g -y

# Install skill to Claude Code only (no Node.js required)
install-skill-local:
	@mkdir -p ~/.claude/skills/redis-fs
	@ln -sf $(PWD)/skills/redis-fs/SKILL.md ~/.claude/skills/redis-fs/SKILL.md
	@echo "Installed redis-fs skill to ~/.claude/skills/redis-fs/"

# Uninstall skill from Claude Code
uninstall-skill-local:
	@rm -rf ~/.claude/skills/redis-fs
	@echo "Uninstalled redis-fs skill from ~/.claude/skills/"

# Build and start MCP server (HTTP) with Redis
mcp-up:
	docker-compose up -d --build
	@echo ""
	@echo "Redis-FS MCP server running at http://localhost:8089/sse"
	@echo "Health check: http://localhost:8089/health"
	@echo ""
	@echo "To add to Auggie, run: make install-mcp-auggie"

# Stop MCP server and Redis
mcp-down:
	docker-compose down

# View MCP server logs
mcp-logs:
	docker-compose logs -f mcp-server

# Add MCP server to Auggie CLI (HTTP transport)
install-mcp-auggie:
	auggie mcp add-json --replace redis-fs '{"type":"sse","url":"http://localhost:8089/sse"}'
	@echo "Added redis-fs MCP server to Auggie (HTTP/SSE)"

# Remove MCP server from Auggie CLI
uninstall-mcp-auggie:
	auggie mcp remove redis-fs
	@echo "Removed redis-fs MCP server from Auggie"

.PHONY: all clean test install-skill install-skill-local uninstall-skill-local mcp-up mcp-down mcp-logs install-mcp-auggie uninstall-mcp-auggie
