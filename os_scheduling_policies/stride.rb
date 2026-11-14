require("securerandom")
require("benchmark")

# Demonstration of basic stride scheduling. 
class Job
  attr_reader(:id, :pid, :tickets, :pass)

  def initialize(work:, tickets:, pass:, kernel:)
    @id = SecureRandom.hex
    @work = work
    @kernel = kernel
    @status = "pending"
    @tickets = tickets
    @pass = pass
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
      stride_forward
      @pid = fork { @work.call(@kernel) }
      @waiter = Process.detach(@pid)
    else
      return if finished?
      return if running?

      stride_forward
      Process.kill("CONT", pid)
    end
    @status = "running"
  end

  private

  def stride_forward
    @pass += 1000.to_f/@tickets
  end
end

class StrideKernel
  TIME_SLICE = 0.05 # 50ms
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
      next sleep(TIME_SLICE) unless job
      read = nil
      time_spent = Benchmark.realtime do
        read = @rd.wait_readable(TIME_SLICE)
      end

      @rd.gets if read

      if job.finished?
        remove_job(job)
        next
      end

      next if @jobs_list.size <= 1

      job.pause
      # read again from pipe in case syscall was made between previous read and job pause
      @rd.wait_readable(0).then { it.gets if it } 
      puts("Ran job #{job.pid} for #{time_spent}")
    end
  end

  def add_job(tickets, &work)
    @mutex.synchronize do
      pass = @jobs_list.empty? ? 0 : @jobs_list.sort_by(&:pass).first.pass
      @jobs_list.add(Job.new(work:, tickets:, pass:, kernel: @sys_calls))
    end
  end

  private

  def remove_job(job)
    @mutex.synchronize do
      @jobs_list.delete(job)
    end
  end

  def context_switch
    job = @jobs_list.sort_by(&:pass).first
    return unless job

    job.run
    job
  end

  def current_clock_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end

my_kernel = StrideKernel.new

producer_thread = Thread.new do
  my_kernel.add_job(25) do |_kernel|
    loop do
    end
  end

  my_kernel.add_job(100) do |_kernel|
    loop do
    end
  end

  my_kernel.add_job(25) do |_kernel|
    loop do
    end
  end

  sleep(50)

  my_kernel.add_job(15) do |_kernel|
    loop do
    end
  end
end


# my_kernel.add_job(25) do |kernel|
#   1.upto(10000) do
#     sleep(0.001)
#     kernel.make_sys_call if(rand < 0.1)
#   end
# end

my_kernel.start
producer_thread.join