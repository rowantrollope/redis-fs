all: rfs redis-qmd

rfs:
	go build -o ../rfs .

redis-qmd:
	go build -o ../redis-qmd ./cmd/redis-qmd

clean:
	rm -f ../rfs ../redis-qmd

.PHONY: all clean rfs redis-qmd
