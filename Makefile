NAME = concurrent-fuzzer
CFLAGS = -Wall -Wextra -Werror -O3 -flto

OBJ_DIR = obj
SRC_DIR = src

SRCS =	main.c
OBJS = $(addprefix $(OBJ_DIR)/, $(SRCS:.c=.o))

all: $(NAME)

$(NAME): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $(NAME)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	mkdir -p $(OBJ_DIR)
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f $(NAME)
	rm -rf $(OBJ_DIR)

re: clean all

.PHONY: all clean re
