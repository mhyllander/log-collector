module LogCollector

  class Monitor
    include ErrorUtils

    def initialize(config,event_queue,state_mgr)
      @config = config
      @event_queue = event_queue
      @state_mgr = state_mgr

      @collectors = {}
      @old_collectors = []
      @monitors = {}

      @config.files.each do |path,fc|
        set_active(path)
        @collectors[path] = LogCollector::Collector.new(path,fc,@event_queue,self)
      end

      setup_monitors
    end

    def terminate
      cancel_monitors
      $logger.info "terminating collectors"
      @collectors.each {|rp,c| c.terminate}
      @old_collectors.each {|c| c.terminate}
    end

    def cancel_monitors
      $logger.info "cancel monitors"
      @monitors.each {|p,m| m.stop}
      @monitors.clear
    end

    def setup_monitors
      $logger.info "setup monitors"
      logdirs = {}
      ancestordirs = {}

      @collectors.each do |path,collector|
        pn = Pathname.new(path)

        # get log dirs
        dir, fn = pn.split.map {|p| p.to_s}
        logdirs[dir] ||= {}
        logdirs[dir][fn] = collector

        # get all ancestors of log dirs
        pn = pn.parent
        until pn.root?
          dir, subdir = pn.split.map {|p| p.to_s}
          ancestordirs[dir] ||= {}
          ancestordirs[dir][subdir] ||= []
          ancestordirs[dir][subdir] << collector
          pn = pn.parent
        end
      end

      # start monitors
      (logdirs.keys | ancestordirs.keys).uniq.each do |dir|
        @monitors[dir] = start_monitor(dir, logdirs[dir], ancestordirs[dir])
      end
    end

    # inotify events:
    # entry created:  created + modified
    # entry deleted:  deleted
    # entry modified: modified
    # entry renamed:  renamed
    def start_monitor(dir,files,ancestors_in_dir)
      $logger.debug { %Q[start_monitor: #{dir} files=#{files} ancestors_in_dir=#{ancestors_in_dir}] }
      monitor = JRubyNotify::Notify.new
      monitor.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, entry, newentry|
        Thread.current[:name] = 'monitor'
        Thread.current.priority = 3
        begin
          $logger.debug { %Q[#{path}: detected '#{change}' "#{entry}" (#{newentry})] }
          # handle events on logfiles in dir
          if files
            unless entry =~ /\/$/
              case change
              when :modified
                if collector = files[entry]
                  # The log file has been modified. Notify the collector.
                  collector.notify :modified
                end
              when :renamed
                if collector = files[entry]
                  # The log file has been renamed. Forget about the collector and let it continue
                  # until no more data is written to the file. The monitor will wait for the log
                  # file to be created again.
                  collector.notify :renamed
                  forget_collector(path,entry)
                  files[entry] = nil
                elsif collector = files[newentry]
                  # The log file has been replaced by a new file. Forget about the collector and let
                  # it continue until no more data is written to the old file, and start a new
                  # collector on the new file.
                  collector.notify :replaced
                  forget_collector(path,newentry)
                  files[newentry] = start_new_collector(path,newentry)
                end
              when :deleted
                # The log file has been deleted. Forget about the collector and let it continue
                # until no more data is written to the file. The monitor will wait for the log file
                # to be created again.
                if collector = files[entry]
                  collector.notify :deleted
                  forget_collector(path,entry)
                  files[entry] = nil
                end
              when :created
                # A file has been created. Check if the file is being monitored. If no collector
                # exists for the file (it may have been deleted or renamed previously), create a new
                # collector.
                if files.include?(entry)
                  collector = files[entry]
                  if collector
                    collector.notify :created
                  else
                    files[entry] = start_new_collector(path,entry)
                  end
                end
              end
            end
          end
          # handle events on ancestors (of logfiles) in dir
          if ancestors_in_dir
            case change
            when :created
              if collectors = ancestors_in_dir[entry]
                # base was re-created
                check_and_restart_monitors collectors
              end
            when :renamed
              # base was renamed, or another dir was renamed to base
              if collectors = ancestors_in_dir[entry]       # like a delete
                # if an ancestor was deleted we just keep on reading the open file
                # and monitor for the re-creation of the ancestor/directory
              elsif collectors = ancestors_in_dir[newentry] # like a create
                # base was re-created
                check_and_restart_monitors collectors
              end
            when :deleted
              # base was deleted
              if collectors = ancestors_in_dir[entry]
                # if an ancestor was deleted we just keep on reading the open file
                # and monitor for the re-creation of the ancestor/directory
              end
            end # case change
          end
        rescue OutOfMemoryError
          abort "Monitor: exiting because of java.lang.OutOfMemoryError"
        rescue Exception=>e
          on_exception e, false
        end
      end # monitor.watch
      monitor.run
      $logger.info { %Q[watching "#{dir}" for notifications about files #{files ? files.keys : []} ancestors_in_dir=#{ancestors_in_dir ? ancestors_in_dir.keys : []}] }
      monitor
    end

    def forget_collector(dir,fn)
      pn = Pathname.new(dir) + fn
      path = pn.to_s
      if (collector = @collectors[path])
        @old_collectors << collector
        @collectors[path] = nil
      end
    end

    # called by an old collector when it reaches EOF and terminates
    def forget_old_collector(collector)
      @old_collectors.delete collector
    end

    def start_new_collector(dir,fn)
      pn = Pathname.new(dir) + fn
      path = pn.to_s
      $logger.info { %Q[starting new collector on #{path}] }
      # start new collector at beginning of file
      fileconfig = @config.files[path]
      fileconfig['startpos'] = 0
      set_active(path)
      @collectors[path] = LogCollector::Collector.new(path,fileconfig,@event_queue,self)
    end

    def set_active(path)
      fstat = File.stat(path)
      @state_mgr.active_file path, {'dev' => fstat.dev, 'ino' => fstat.ino}
    end

    def check_and_restart_monitors(collectors)
      collectors.each {|c| c.notify :check}
      cancel_monitors
      setup_monitors
    end

  end # class Monitor

end # module LogCollector
