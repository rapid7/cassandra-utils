class MockSemaphore
  attr_reader :lock_call_count
  attr_reader :renew_call_count
  attr_reader :kill_call_count
  attr_reader :locked_call_count
  attr_reader :try_release_count

  def initialize
    @lock_call_count = 0
    @renew_call_count = 0
    @kill_call_count = 0
    @locked_call_count = 0
    @try_release_count = 0
    @is_locked = false
  end

  def lock
    @is_locked = true
    @lock_call_count += 1
    nil
  end

  def renew
    @renew_call_count += 1
    self
  end

  def locked?
    @locked_call_count += 1
    @is_locked
  end

  def try_release
    @try_release_count += 1
    @is_locked = false
    nil
  end

  def kill
    @kill_call_count += 1
    nil
  end

  def validate!
  end
end
