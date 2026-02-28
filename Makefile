.PHONY: all module mount cli clean test

all: module mount cli

module:
	$(MAKE) -C module

mount:
	$(MAKE) -C mount

cli:
	$(MAKE) -C cli

clean:
	$(MAKE) -C module clean
	$(MAKE) -C mount clean
	$(MAKE) -C cli clean
	$(RM) fs.so fs.xo path.xo

test: module
	$(MAKE) -C module test
