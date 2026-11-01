#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <time.h>

#define ITERATIONS 1000000

static inline void pin_process_to_cpu()
{
  cpu_set_t set;

  CPU_ZERO(&set);
  CPU_SET(0, &set);

  sched_setaffinity(0, sizeof(cpu_set_t), &set);
}

long long get_elapsed_time(struct timespec start, struct timespec end)
{
  return ((end.tv_sec - start.tv_sec) * 1000000000LL) + (end.tv_nsec - start.tv_nsec);
}

int main(void)
{

  pin_process_to_cpu();

  int pipefds_1[2];
  int pipefds_2[2];

  pipe(pipefds_1);
  pipe(pipefds_2);

  int rd_child = pipefds_1[0];
  int wr_parent = pipefds_1[1];
  int rd_parent = pipefds_2[0];
  int wr_child = pipefds_2[1];

  switch (fork())
  {
  case -1:
    perror("Fork failed");
    exit(1);
  case 0:
    pin_process_to_cpu();
    close(rd_parent);
    close(wr_parent);
    char rd_buff_c[3];

    long long time_elapsed_1 = 0;

    struct timespec start_1, end_1;

    for (int i = 0; i < ITERATIONS; i++)
    {
      clock_gettime(CLOCK_MONOTONIC, &start_1);
      int bytes_1 = read(rd_child, rd_buff_c, 3);
      clock_gettime(CLOCK_MONOTONIC, &end_1);
      time_elapsed_1 += get_elapsed_time(start_1, end_1);

      if (bytes_1 > 0)
      {
        // puts(rd_buff_c);
        write(wr_child, "ho", 3);
      }
    }

    printf("Time elapsed was: %lld\n", time_elapsed_1 / (ITERATIONS * 2));
    exit(0);
  default:
    close(rd_child);
    close(wr_child);
    char rd_buff_p[3];

    write(wr_parent, "hi", 3);

    long long time_elapsed_2 = 0;
    struct timespec start_2, end_2;

    for (int i = 0; i < ITERATIONS; i++)
    {
      clock_gettime(CLOCK_MONOTONIC, &start_2);
      int bytes_2 = read(rd_parent, rd_buff_p, 3);
      clock_gettime(CLOCK_MONOTONIC, &end_2);
      time_elapsed_2 += get_elapsed_time(start_2, end_2);

      if (bytes_2 > 0)
      {
        write(wr_parent, "hi", 3);
        // puts(rd_buff_p);
      }
    }

    wait(NULL);
    printf("Time elapsed was: %lld\n", time_elapsed_2 / (ITERATIONS * 2));
  }
}