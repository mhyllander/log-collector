module LogCollector

  class Monitor
    include ErrorUtils

    def initialize(config,event_queue)
      @collectors = {}
      @monitors = {}
      @parent_monitors = {}

      config.files.each do |path,fc|
        @collectors[path] = LogCollector::Collector.new(path,fc,event_queue)
      end

      setup_monitors
    end

    def terminate
      $logger.info "terminating collectors"
      @collectors.each {|rp,c| c.terminate}
      cancel_monitors
    end

    def cancel_monitors
      $logger.info "cancel monitors"
      @monitors.each {|p,m| m.stop}
      @monitors.clear
      @parent_monitors.each {|p,m| m.stop}
      @parent_monitors.clear
    end

    def setup_monitors
      $logger.info "setup monitors"
      logdirs = {}
      parentdirs = {}

      @collectors.each do |path,collector|
        pn = Pathname.new(path)

        # get log dirs
        dir, base = pn.split.map {|f| f.to_s}
        logdirs[dir] ||= {}
        logdirs[dir][base] = collector

        # get all parents of log dirs
        pn = pn.parent
        until pn.root?
          dir, base = pn.split.map {|f| f.to_s}
          parentdirs[dir] ||= {}
          parentdirs[dir][base] ||= []
          parentdirs[dir][base] << collector
          pn = pn.parent
        end
      end

      # start monitors on log dirs
      logdirs.each do |dir,files|
        @monitors[dir] = monitor_files(dir,files)
      end

      # start monitors on all parents of log dirs
      parentdirs.each do |dir,parents|
        @parent_monitors[dir] = monitor_parents(dir,parents)
      end
    end

    # inotify events:
    # file created:  created + modified
    # file deleted:  deleted
    # file modified: modified
    # file renamed:  renamed
    def monitor_files(dir,files)
      monitor = JRubyNotify::Notify.new
      monitor.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, file, newfile|
        Thread.current['name'] = 'monitor'
        Thread.current.priority = 3
        begin
          unless file =~ /\/$/
            $logger.debug { %Q[#{path}: detected "#{file}" '#{change}' (#{newfile})] }
            if (collector = files[file])
              collector.notify change
            elsif change==:renamed && (collector = files[newfile])
              collector.notify :replaced
            end
          end
        rescue OutOfMemoryError
          abort "Monitor: exiting because of java.lang.OutOfMemoryError"
        rescue Exception=>e
          on_exception e, false
        end
      end # monitor.watch
      monitor.run
      $logger.info { %Q[watching "#{dir}" for notifications about files #{files.keys}] }
      monitor
    end

    def monitor_parents(dir,parents)
      monitor = JRubyNotify::Notify.new
      monitor.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, file, newfile|
        begin
          $logger.debug { %Q[#{path}: detected "#{file}" '#{change}' (#{newfile})] }
          case change
          when :created
            if (collectors = parents[file])
              # base was re-created
              check_and_restart_monitors collectors
            end
          when :renamed
            # base was renamed, or another dir was renamed to base
            if (collectors = parents[file])       # like a delete
              # if a parent was deleted we just keep on reading the open file
              # and monitor for the re-creation of the parent/directory
            elsif (collectors = parents[newfile]) # like a create
              # base was re-created
              check_and_restart_monitors collectors
            end
          when :deleted
            # base was deleted
            if (collectors = parents[file])
              # if a parent was deleted we just keep on reading the open file
              # and monitor for the re-creation of the parent/directory
            end
          end # case change
        rescue OutOfMemoryError
          abort "Monitor: exiting because of java.lang.OutOfMemoryError"
        rescue Exception=>e
          on_exception e, false
        end
      end # monitor.watch
      monitor.run
      $logger.info { %Q[watching "#{dir}" for notifications about parents #{parents.keys}] }
      monitor
    end

    def check_and_restart_monitors(collectors)
      collectors.each {|c| c.notify :check}
      cancel_monitors
      setup_monitors
    end

  end # class Monitor

end # module LogCollector
