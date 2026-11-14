require("securerandom")
require("benchmark")

NICENESS_TO_WEIGHTS = {
  -20 => 88761,
  -19 => 71755,
  -18 => 56483,
  -17 => 46273,
  -16 => 36291,
  -15 => 29154,
  -14 => 23254,
  -13 => 18705,
  -12 => 14949,
  -11 => 11916,
  -10 => 9548,
  -9 => 7620,
  -8 => 6100,
  -7 => 4904,
  -6 => 3906,
  -5 => 3121,
  -4 => 2501,
  -3 => 1991,
  -2 => 1586,
  -1 => 1277,
  0 => 1024,
  1 => 820,
  2 => 655,
  3 => 526,
  4 => 423,
  5 => 335,
  6 => 272,
  7 => 215,
  8 => 172,
  9 => 137,
  10 => 110,
  11 => 87,
  12 => 70,
  13 => 56,
  14 => 45,
  15 => 36,
  16 => 29,
  17 => 23,
  18 => 18,
  19 => 15,
  20 => 12
}

require("securerandom")
require("benchmark")

# Demonstration of basic stride scheduling. 
class Job
  attr_reader(:id, :pid, :niceness, :weight, :vruntime)

  def initialize(work:, niceness:, vruntime:, kernel:)
    @id = SecureRandom.hex
    @work = work
    @kernel = kernel
    @status = "pending"
    @niceness = niceness
    @weight = NICENESS_TO_WEIGHTS[niceness]
    @vruntime = vruntime
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
      return if finished?
      return if running?

      Process.kill("CONT", pid)
    end
    @status = "running"
  end

  def increment_vruntime(by)
    @vruntime += by
  end
end

class LinuxCFSKernel
  SCHED_LATENCY = 0.048 # 48ms
  MIN_GRANULARITY = 0.006 # 6 ms
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
    @mutex = Mutex.new
    @rd, @wr = IO.pipe
    @sys_calls = SysCalls.new(@wr)
    @jobs_list = Set.new
  end

  def start
    loop do
      job = context_switch
      next sleep(MIN_GRANULARITY) unless job
      read = nil
      time_slice = time_slice(job)
      time_spent = Benchmark.realtime do
        read = @rd.wait_readable(time_slice)
      end

      @rd.gets if read

      if job.finished?
        remove_job(job)
        next
      end

      increment_vruntime(job, time_spent)

      next if @jobs_list.size <= 1

      job.pause
      # read again from pipe in case syscall was made between previous read and job pause
      @rd.wait_readable(0).then { it.gets if it } 
      puts("Ran job #{job.pid} for #{time_spent}")
    end
  end

  def add_job(niceness, &work)
    @mutex.synchronize do
      vruntime = @jobs_list.empty? ? 0 : @jobs_list.sort_by(&:vruntime).first.vruntime
      @jobs_list.add(Job.new(work:, niceness:, vruntime:, kernel: @sys_calls))
    end
  end

  private

  def remove_job(job)
    @mutex.synchronize do
      @jobs_list.delete(job)
    end
  end

  def context_switch
    job = @mutex.synchronize { @jobs_list.sort_by(&:vruntime).first}
    return unless job

    job.run
    job
  end

  def increment_vruntime(job, last_runtime)
    runtime = (NICENESS_TO_WEIGHTS[0]/job.weight.to_f) * last_runtime
    job.increment_vruntime(runtime)
  end

  def time_slice(job)
    slice = (job.weight/total_weight.to_f) *  [MIN_GRANULARITY, SCHED_LATENCY.to_f/@jobs_list.size].max
    slice
  end

  def total_weight
    @jobs_list.reject(&:finished?).sum(&:weight)
  end
end

my_kernel = LinuxCFSKernel.new

producer_thread = Thread.new do
  my_kernel.add_job(-5) do |_kernel|
    loop do
    end
  end

  my_kernel.add_job(20) do |_kernel|
    loop do
    end
  end

  my_kernel.add_job(0) do |_kernel|
    loop do
    end
  end

  sleep(50)

  my_kernel.add_job(-10) do |_kernel|
    loop do
    end
  end
end


my_kernel.add_job(-5) do |kernel|
  1.upto(10000) do
    sleep(0.001)
    kernel.make_sys_call if(rand < 0.1)
  end
end

my_kernel.start
producer_thread.join