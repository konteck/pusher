CPP = g++
SRC = src/pusher.cpp
OUT = pusher
CPPFLAGS = # -O2 -Wall
LDFLAGS = -lssl -lcrypto -lzmq -lcurl -lmongoclient -lboost_system-mt -lboost_thread-mt -lboost_filesystem-mt

all: build run

build: $(SRC)
	@echo Compiling $(basename $<)...
	$(CPP) $(CPPFLAGS) $(SRC) -o $(OUT) $(LDFLAGS)

run:
	./$(OUT)

install: build
	sudo cp $(OUT) /usr/bin

uninstall:
	sudo rm -rf /usr/bin/$(OUT)

clean:
	rm -rf *.dSYM
