module LogCollector

  class Monitor
    include ErrorUtils

    def initialize(config,event_queue)
      @config = config
      @event_queue = event_queue

      @collectors = {}
      @monitors = {}
      @symlink_monitors = {}

      config.files.each do |path,fc|
        collector = LogCollector::Collector.new(path,fc,@event_queue)
        @collectors[path] = collector
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
      @symlink_monitors.each {|p,m| m.stop}
      @symlink_monitors.clear
    end

    def setup_monitors
      $logger.info "setup monitors"
      symlink_deps = {}

      @collectors.each do |path,collector|
        monitor, symlinks = monitor_file(path,collector)
        @monitors[path] = monitor
        symlinks.each do |pn|
          symlink_deps[pn] ||= []
          symlink_deps[pn] << collector
        end
      end

      symlink_deps.each do |pn,collectors|
        @symlink_monitors[pn.to_s] = monitor_symlink(pn,collectors)
      end
    end

    # inotify events:
    # file created:  created + modified
    # file deleted:  deleted
    # file modified: modified
    # file renamed:  renamed
    def monitor_file(logfile,collector)
      pn = Pathname.new(logfile)
      dir, base = pn.split.map {|f| f.to_s}
      monitor = JRubyNotify::Notify.new
      monitor.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, file, newfile|
        Thread.current['name'] = 'monitor'
        Thread.current.priority = 20
        begin
          unless file =~ /\/$/
            $logger.debug "#{path}: detected '#{change}' #{file} (#{newfile})"
            if base==file
              collector.notification_queue.push change
            elsif change==:renamed && base==newfile
              collector.notification_queue.push :replaced
            end
          end
        rescue Exception=>e
          on_exception e, false
        end
      end # monitor.watch
      monitor.run
      $logger.info %Q[watching "#{dir}" for notifications]

      # check if any parents are symlinks
      symlinks = []
      pn = pn.parent
      until pn.root?
        symlinks << pn if pn.symlink?
        pn = pn.parent
      end

      [monitor, symlinks]
    end

    def monitor_symlink(pn,collectors)
      dir, base = pn.split.map {|f| f.to_s}
      $logger.info %Q[watching "#{dir}" for notifications about "#{base}" symlink]
      monitor = JRubyNotify::Notify.new
      monitor.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, file, newfile|
        begin
          $logger.debug "#{@path}: detected '#{change}' #{file} (#{newfile})"
          case change
          when :created
            if file==base
              # base was re-created, either as a symlink or a directory
              check_and_restart_monitors collectors
            end
          when :renamed
            # symlink base was renamed, or another symlink/dir was renamed to base
            if file==base       # like a delete
              # if a symlink was deleted we just keep on reading the open file
              # and monitor for the re-creation of the symlink/directory
            elsif newfile==base # like a create
              # base was re-created, either as a symlink or a directory
              check_and_restart_monitors collectors
            end
          when :deleted
            # a symlink was deleted
            if file==base
              # if a symlink was deleted we just keep on reading the open file
              # and monitor for the re-creation of the symlink/directory
            end
          end # case change
        rescue Exception=>e
          on_exception e, false
        end
      end # monitor.watch
      monitor.run
      monitor
    end

    def check_and_restart_monitors(collectors)
      collectors.each {|c| c.notification_queue.push :check}
      cancel_monitors
      setup_monitors
    end

  end # class Monitor

end # module LogCollector
