#include "database_service.h"

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
    off_t file_size = sb.st_size;
    
    // Map the file to memory
    void *mapped_data = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
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

    char *data = (char *)mapped_data;


    for (int i = 0; i < num_of_workers; i++) {

        off_t start_offset = i * chunk_size;
        off_t end_offset = (i == num_of_workers - 1) ? file_size : start_offset + chunk_size;

        if (i != 0) // Adjusting the start offset in order to prevent line interruption 
        {
            while (data[start_offset] != '\n' && start_offset != file_size) {
                start_offset++;
            }
            start_offset++; // Starting by the "char" after the '\n'
        }
            
        if (i != (num_of_workers - 1))
        {
            while (data[end_offset] != '\n' && end_offset != file_size){
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
    char keyword_lower[MAX_KEYWORD_LENGTH];
    for (size_t i = 0; i < strlen(keyword); i++) {
        keyword_lower[i] = tolower(keyword[i]);
    }
    keyword_lower[strlen(keyword)] = '\0';
    //***********


    // Line by line keyword detection
    char *data = (char *)shared_memory_addr;
    char *current_offset_ptr = data + start_offset;
    char *end_offset_ptr = data + end_offset;

    while (current_offset_ptr < end_offset_ptr)
    {
        const char *line_start = current_offset_ptr;
        while (*current_offset_ptr != '\n' && current_offset_ptr < end_offset_ptr)
        {
            current_offset_ptr++;
        }

        const char *line_end = current_offset_ptr; //without '\n'

        size_t line_length = line_end - line_start;

        char lowered_buffer[MAX_LINE_LENGTH];
        char original_buffer[MAX_LINE_LENGTH];
        for (size_t i = 0; i < line_length; i++)
        {
            unsigned char c = (unsigned char)line_start[i];
            lowered_buffer[i] = (char)tolower(c);
            original_buffer[i] = (char)c;
        }

        lowered_buffer[line_length] = '\0';

        if (strstr(lowered_buffer, keyword_lower))
        {
            write(pipe_write, original_buffer, line_length + 1);
        }

        if (*current_offset_ptr == '\n' && current_offset_ptr < end_offset_ptr)
        {
            current_offset_ptr++;
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
        NULL       
    };

    if (execvp("sort", sort_args) == -1)
    {
        perror("Error executing sort");
        exit(EXIT_FAILURE);
    }
}

void run_reporter(const char *output_file) {
    
    char *wc_args[] = {
        "wc",
        "-l",
        (char *)output_file,
        NULL
    };
    execvp("wc", wc_args);
    perror("Error executing wc");
    exit(EXIT_FAILURE);
}
