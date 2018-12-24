client2: client2.c
	$(CC) -Wall -Wextra -Wno-strict-overflow -O2 $< -o$@ -lpthread
