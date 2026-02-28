all: redis-fs-mount

redis-fs-mount:
	go build -o redis-fs-mount ./cmd/redis-fs-mount

clean:
	rm -f redis-fs-mount

.PHONY: all clean redis-fs-mount
