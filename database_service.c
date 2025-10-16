// #include "database_service.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/wait.h> 

void run_extractor(void *shared_memory_addr, off_t start_offset, off_t end_offset, const char *keyword, int pipe_write);
void run_sorter(int pipe_read, const char *output_file);
void run_reporter(const char *output_file);

int main(int argc, char *argv[]) {
    
    // Argument control
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <input_file> <output_file> <num_of_workers> <keyword>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Open input file and error check
    int fd = open(argv[1], O_RDONLY);
    if (fd == -1) {
        perror("Error opening input file");
        exit(EXIT_FAILURE);
    }

    // Get the size of the file (bytewise)
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Error getting the file size w/fstat");
        close(fd);
        exit(EXIT_FAILURE);
    }
    size_t file_size = sb.st_size;
    
    // Map the file to memory
    void *mapped_data = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped_data == MAP_FAILED) {
        perror("Error mapping the file");
        close(fd);
        exit(EXIT_FAILURE);
    }

    // Create a pipe for IPC
    int pipe_fd[2];
    if (pipe(pipe_fd) == -1) {
        perror("Error creating pipe");
        close(fd);
        exit(EXIT_FAILURE);
    }
    
    // Fork the extractor processes
    int num_of_workers = atoi(argv[3]);
    int chunk_size = file_size / num_of_workers;

    pid_t *extractor_pid_collection = malloc(sizeof(pid_t) * num_of_workers); // Array to store extractor PIDs
    if (extractor_pid_collection == NULL) {
        perror("Error allocating memory for extractor PIDs");
        close(fd);
        exit(EXIT_FAILURE);
    }


    for (int i = 0; i < num_of_workers; i++) {

        off_t start_offset = i * chunk_size;
        off_t end_offset = (i == num_of_workers - 1) ? file_size : start_offset + chunk_size;

        if (i != 0) // Adjusting the start offset in order to prevent line interruption 
        {
            while (mapped_data[start_offset] != '\n' && start_offset != file_size) {
                start_offset++;
            }
            start_offset++; // Starting by the "char" after the '\n'
        }
            
        if (i != (num_of_workers - 1))
        {
            while (mapped_data[end_offset] != '\n' && end_offset != file_size){
                end_offset++;
            }
            // Ending by the "char", '\n'
        }

        extractor_pid_collection[i] = fork();

        if (extractor_pid_collection[i] == -1)
        {
            perror("Error forking extractor process");
            close(fd);
            exit(EXIT_FAILURE);
        } else if (extractor_pid_collection[i] == 0) // Child process
        {
            close(pipe_fd[0]); // Close unused read end
            run_extractor(mapped_data, start_offset, end_offset, argv[4], pipe_fd[1]);
            close(pipe_fd[1]); // Close write end after use
            exit(EXIT_SUCCESS);
        }
                
    }
    close(pipe_fd[1]); // Close unused write end in parent


    pid_t sorter_pid = fork();

    if (sorter_pid == -1) {
        perror("Error forking sorter process");
        close(fd);
        exit(EXIT_FAILURE);
    } else if (sorter_pid == 0) // Child process
    {
        close(pipe_fd[1]); // Close unused write end
        run_sorter(pipe_fd[0], argv[2]);
        close(pipe_fd[0]); // Close read end after use
        exit(EXIT_SUCCESS);
    }

    close(pipe_fd[0]); // Close unused read end in parent

    int status;
    // Wait for all extractor processes to finish
    for (int i = 0; i < num_of_workers; i++) {
        waitpid(extractor_pid_collection[i], &status, 0);
    }   

    // Wait for the sorter process to finish
    waitpid(sorter_pid, &status, 0);

    pid_t reporter_pid = fork();
    if (reporter_pid == -1) {
        perror("Error forking reporter process");
        close(fd);
        exit(EXIT_FAILURE);
    } else if (reporter_pid == 0) // Child process
    {
        run_reporter(argv[2]);
        exit(EXIT_SUCCESS);
    }

    // Wait for the reporter process to finish
    waitpid(reporter_pid, &status, 0);

    // Clean up
    free(extractor_pid_collection);
    munmap(mapped_data, file_size);
    close(fd);

}  


void run_extractor(void *shared_memory_addr, off_t start_offset, off_t end_offset, const char *keyword, int pipe_write) {

    //*********** Keyword lowering for case insensivity
    char keyword_lower[64];
    for (size_t i = 0; i < strlen(keyword); i++) {
        keyword_lower[i] = tolower(keyword[i]);
    }

    size_t keyword_len = strlen(keyword);
    //***********


    // Line by line keyword detection
    off_t current_offset = shared_memory_addr + start_offset;

    while (current_offset != end_offset)
    {
        if (current_offset != shared_memory_addr + start_offset)
        {
            current_offset++; // To skip the '\n' char
        }
        
        char *tampon[128];
        char *tampon_unlowered[128];

        size_t size_of_line = 1;
        while (shared_memory_addr[current_offset] != '\n')
        {
            size_of_line++;
            current_offset++;
        }

        for (size_t i = 0; i < size_of_line; i++)
        {
            tampon[i] = tolower(shared_memory_addr[current_offset - size_of_line + i]);
            tampon_unlowered[i] = shared_memory_addr[current_offset - size_of_line + i];
        }
        tampon[size_of_line] = '\0'

        if (strstr(tampon, keyword_lower) != NULL){
            write(pipe_write, tampon_unlowered, size_of_line);
        }
    }
    
}

void run_sorter(int pipe_read, const char *output_file) {
    int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd == -1)
    {
        perror("File cannot be opened.");
        exit(EXIT_FAILURE);
    }

    if (dup2(output_fd, STDOUT_FILENO) == -1)
    {
        perror("Dup2 failed.");
        close(output_fd);
        exit(EXIT_FAILURE);
    }
    close(output_fd); // Not needed after dup2

    if (dup2(pipe_read, STDIN_FILENO) == -1)
    {
        perror("Dup2 failed.");
        exit(EXIT_FAILURE);
    }
    close(pipe_read); // Not needed after dup2
    
    char *sort_args[] = {
        "sort",     
        "-k5,5",     
        "-t ' '",      
        NULL       
    };

    if (execvp("sort", sort_args) == -1)
    {
        perror("Error executing sort");
        exit(EXIT_FAILURE);
    }

    perror("Execvp failed");
    exit(EXIT_FAILURE);
}

void run_reporter(const char *output_file) {
    
    char *wc_args[] = {
        "wc",
        "-l",
        output_file,
        NULL
    };
    execvp("wc", wc_args);
    perror("Error executing wc");
    exit(EXIT_FAILURE);
}
