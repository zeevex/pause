module Pause
  class Action
    attr_accessor :identifier

    def initialize(identifier, options = {})
      @identifier = identifier
      self.class.checks = [] unless self.class.instance_variable_get(:@checks)

      @fail_open =  if options.has_key?(:fail_open)
                      options[:fail_open]
                    elsif self.class.class_variable_defined?(:@@class_fail_open)
                      self.class.class_variable_get(:@@class_fail_open)
                    else
                      false
                    end
    end

    # Action subclasses should define their scope as follows
    #
    #     class MyAction < Pause::Action
    #       scope "my:scope"
    #     end
    #
    def scope
      raise "Should implement scope. (Ex: ipn:follow)"
    end

    def self.scope(scope_identifier = nil)
      class_variable_set(:@@class_scope, scope_identifier)
      define_method(:scope) { scope_identifier }
    end

    def self.fail_open(value = true)
      class_variable_set(:@@class_fail_open, value)
    end

    def self.fail_closed(value = true)
      class_variable_set(:@@class_fail_open, !value)      
    end

    def fail_open?
      @fail_open
    end

    # Action subclasses should define their checks as follows
    #
    #  period_seconds - compare all activity by an identifier within the time period
    #  max_allowed    - if the number of actions by an identifier exceeds max_allowed for the time period marked
    #                   by period_seconds, it is no longer ok.
    #  block_ttl      - time to mark identifier as not ok
    #
    #     class MyAction < Pause::Action
    #       check period_seconds: 60,   max_allowed: 100,  block_ttl: 3600
    #       check period_seconds: 1800, max_allowed: 2000, block_ttl: 3600
    #     end
    #
    def self.check(*args)
      @checks ||= []
      period_seconds, max_allowed, block_ttl =
        if args.first.is_a?(Hash)
          [args.first[:period_seconds], args.first[:max_allowed], args.first[:block_ttl]]
        else
          args
        end
      @checks << Pause::PeriodCheck.new(period_seconds, max_allowed, block_ttl)
    end

    def self.checks
      @checks
    end

    def checks
      self.class.instance_variable_get(:@checks)
    end

    def self.checks=(period_checks)
      @checks = period_checks
    end

    def increment!(count = 1, timestamp = Time.now.to_i, options = {})
      Pause.analyzer.increment(self, timestamp, count)
    rescue ::Redis::CannotConnectError
      options.fetch(:fail_open, fail_open?) ? true : raise
    end

    def rate_limited?
      ! ok?
    end

    def ok?(options = {})
      !Pause.analyzer.adapter.rate_limited?(self.key) && Pause.analyzer.check(self).nil?
    rescue ::Redis::CannotConnectError => e
      $stderr.puts "Error connecting to redis: #{e.inspect}"
      options.fetch(:fail_open, fail_open?) ? true : false
    end

    def analyze(options = {})
      Pause.analyzer.check(self)
    rescue ::Redis::CannotConnectError
      options.fetch(:fail_open, fail_open?) ? nil : raise      
    end

    def self.tracked_identifiers
      Pause.analyzer.tracked_identifiers(self.class_scope)
    rescue ::Redis::CannotConnectError
      fail_open? ? [] : raise
    end

    def self.rate_limited_identifiers
      Pause.analyzer.rate_limited_identifiers(self.class_scope)
    rescue ::Redis::CannotConnectError
      fail_open? ? [] : raise
    end

    def self.unblock_all
      Pause.analyzer.adapter.delete_rate_limited_keys(self.class_scope)
      true
    rescue ::Redis::CannotConnectError
      fail_open? ? true : raise
    end

    def unblock
      Pause.analyzer.adapter.delete_key(self.key)
      true
    rescue ::Redis::CannotConnectError
      fail_open? ? true : raise
    end

    def key
      "#{self.scope}:#{@identifier}"
    end

    # Actions can be globally disabled or re-enabled in a persistent
    # way.
    #
    #   MyAction.disable
    #   MyAction.enabled? => false
    #   MyAction.disabled? => true
    #
    #   MyAction.enable
    #   MyAction.enabled? => true
    #   MyAction.disabled? => false
    #
    def self.enable
      Pause.analyzer.adapter.enable(class_scope)
    end

    def self.disable
      Pause.analyzer.adapter.disable(class_scope)
      true
    rescue ::Redis::CannotConnectError
      fail_open? ? true : raise
    end

    def self.enabled?
      Pause.analyzer.adapter.enabled?(class_scope)
    rescue ::Redis::CannotConnectError
      fail_open? ? true : raise
    end

    def self.disabled?
      ! enabled?
    end

    private

    def self.class_scope
      class_variable_get:@@class_scope if class_variable_defined?(:@@class_scope)
    end
  end
end
