.PHONY: build clean

BINARY=migxattrs

build:
	CGO_ENABLED=0 go build -o ./bin/$(BINARY)

clean:
	rm -f $(BINARY)
