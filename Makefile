CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g
TARGET = database_service
SOURCES = database_service.c
HEADERS = database_service.h  

$(TARGET): $(SOURCES) $(HEADERS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SOURCES)

clean:
	rm -f $(TARGET)
	
.PHONY: clean