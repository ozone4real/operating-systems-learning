# (Probably stupid) demonstration of the Multi-level Feedback Queue (MLFQ) operating system scheduling policy with Ruby.
# The policy is as follows basically:
#   - Processes are scheduled using a set of queues ordered by priority, from lowest to highest. The queues may have time slices proportional to their priority 
#   - If process A's queue priority > Process B's, process A runs
#   - If process A's queue == Process B's, the processes are scheduled in a round-robbin fashion
#   - When a process uses up it's time slice (in a single run for "naive" version or cumulatively for non-naive), it's demoted to the lower queue, else it remains in it's current queue
#   - Periodically (say every 1s), reset by moving all processes to the highest priority queue
#   

NAIVE = ARGV[0]
puts NAIVE

require "securerandom"
require "benchmark"

class PriorityQueue
  def initialize(levels)
    @levels = levels
    @job_queues = Array.new(levels) { JobQueue.new(it) }
    @job_index = {}
  end

  def work_size
    @job_index.length
  end

  def rotate_job
    queue = @job_queues.find(&:any?)
    queue.rotate if queue
  end

  def add_job(job, level = 0)
    @job_queues[level].add(job)
    @job_index[job.id] = level
  end

  def remove_job(id)
    level = @job_index[id]
    raise "Invalid job id" unless level
    @job_queues[level].remove(id)
    @job_index.delete(id)
  end

  def demote_job(id)
    level = @job_index[id]
    raise "Invalid job id" unless level
    job = @job_queues[level].remove(id)
    next_level = [max_level, level + 1].min
    add_job(job, next_level)
  end

  def reset
    first_queue = @job_queues.flat_map(&:jobs)
    @job_queues = [JobQueue.new(0, Set.new(first_queue))] + Array.new(@levels - 1) { JobQueue.new(it) }
    @job_index.transform_values! { 0 }

  end

  def max_level
    @levels - 1
  end
end

class JobQueue
  attr_reader(:level)

  def initialize(level, queue = Set.new)
    @level = level
    @queue = queue
  end

  def jobs
    @queue.to_a
  end

  def any?
    @queue.any?
  end

  def pop
    @queue.first.tap { @queue.delete(it) if it }
  end

  def add(job)
    @queue.add(job)
  end

  def find(id)
    @queue.find {|job| job.id == id }
  end

  def remove(id)
    find(id).tap { @queue.delete(it) if it }
  end

  def rotate
    pop.tap { add(it) }
  end
end

class Job
  attr_reader(:id, :pid)
  attr_accessor(:duration)

  def initialize(work, kernel)
    @id = SecureRandom.hex
    @work = work
    @kernel = kernel
    @status = "pending"
    @duration = 0
  end

  def pause
    Process.kill("STOP", pid)
    @status = "paused"
  end

  def finished?
    @waiter && !@waiter.status
  end

  def status
    return "finished" if finished?
    @status
  end

  def paused?
    @status == "paused"
  end

  def running?
    @status == "running"
  end

  def pending?
    @status == "pending"
  end

  def eql?(job)
    job.is_a?(Job) && id == job.id
  end

  def hash
    id.hash
  end

  def run
    if pending?
      @pid = fork { @work.call(@kernel) }
      @waiter = Process.detach(@pid)
    else
      raise("Job is finished") if finished?
      return if running?

      Process.kill("CONT", pid)
    end
    @status = "running"
  end
end

class MLFQKernel
  TIME_SLICE = 0.05 # 50ms
  BOOST_PRIORITY_EVERY = 1 # 1s


  class SysCalls
    SYSCALL_BYTE = "s"

    def initialize(channel)
      @channel = channel
    end

    def make_sys_call
      @channel.puts(SYSCALL_BYTE)
      sleep(0.0001)
    end
  end

  def initialize
    @priority_queue = PriorityQueue.new(8)
    @mutex = Mutex.new
    @rd, @wr = IO.pipe
    @sys_calls = SysCalls.new(@wr)
    @priority_boost_clock = current_clock_time
    @naive = NAIVE
  end

  def start
    loop do
      job = context_switch
      next sleep(TIME_SLICE) unless job
      read = nil
      time_spent = Benchmark.realtime do
        read = @rd.wait_readable(TIME_SLICE)
      end

      @rd.gets if read

      if job.finished?
        @mutex.synchronize { @priority_queue.remove_job(job.id) }
        next
      end

      next if @priority_queue.work_size <= 1

      job.pause
      # read again from pipe in case syscall was made between previous read and job pause
      @rd.wait_readable(0).then { it.gets if it } 

      time_slice_exhausted = if @naive
        !read
      else
        job.duration += time_spent
        if job.duration >= TIME_SLICE
          job.duration = 0
          true
        else
          false
        end
      end
      puts("Ran job #{job.pid} for #{time_spent}")



      if boost_priority?
        boost_priority
      else
        if !job.finished? && time_slice_exhausted
          @mutex.synchronize { @priority_queue.demote_job(job.id) }
        end
      end
    end
  end

  def add_job(&work)
    @mutex.synchronize { @priority_queue.add_job(Job.new(work, @sys_calls)) }
  end

  private

  def context_switch
    job = @mutex.synchronize { @priority_queue.rotate_job }
    job.run if job
    job
  end

  def boost_priority
    @mutex.synchronize { @priority_queue.reset }
    @priority_boost_clock = current_clock_time
  end

  def current_clock_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def boost_priority?
   (current_clock_time - @priority_boost_clock) >= BOOST_PRIORITY_EVERY
  end
end

# my_kernel = MLFQKernel.new
my_kernel = MLFQKernel.new

my_kernel.add_job do |_kernel|
  loop do
  end
end

my_kernel.add_job do |_kernel|
  loop do
  end
end

# my_kernel.add_job do |kernel|
#   1.upto(10000) do
#     sleep(0.001)
#     kernel.make_sys_call if(rand < 0.1)
#   end
# end

my_kernel.add_job do |kernel|
  timer = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  loop do
    # Try to game the system by making a system call just before the alloted time slice has exhausted
    # With the naive version of the scheduler, this process is always in the highest priority queue.
    # This will make this process unfairly get almost all of the cpu; starving other processses.
    # This won't be the case in the non-naive version.
    if (Process.clock_gettime(Process::CLOCK_MONOTONIC) - timer) > 0.04
      kernel.make_sys_call
      timer =  Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end



my_kernel.start


