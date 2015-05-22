# encoding: utf-8
require "logstash/outputs/base"
require "logstash/outputs/csv"
require "logstash/namespace"
require "stud/temporary"
require "socket" # for Socket.gethostname
require "thread"
require "tmpdir"
require "fileutils"

require "dropbox_sdk"
require_relative "./dropbox-patch"

# INFORMATION:
#
# This plugin was created for store the logstash's events into a Dropbox folder.
# For use it you needs dropbox token folder.
#
# This plugin is completely based in logstash-output-s3, so if you don't understand something you can also check it out here: https://github.com/logstash-plugins/logstash-output-s3
# Also is inheriting from logstash-output-csv in order to be able to output in csv format, check it out here: https://github.com/logstash-plugins/logstash-output-csv
#
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
# dropbox {
#   temporary_directory => "/Users/javi/Desktop/logstash/tmp/"
#   type => "customers"
#   size_file => 1048576
#   time_file => 2
#   tags => ["customers", "punt fresc", "raw"]
#   csv_format => true
#   csv_options => {"headers" => true}
#   fields => ["name","surname_1", "surname_2"]
#   path => "/loyal_guru_files/customers/"
#   token => "xxxxx"
# }
#}

class LogStash::Outputs::Dropbox < LogStash::Outputs::CSV

  TEMPFILE_EXTENSION = "txt"

  config_name "dropbox"
  default :codec, 'line'

  config :fields, :validate => :array, :required => false

  # Set the size of file in bytes, this means that files on bucket when have dimension > file_size, they are stored in two or more file.
  # If you have tags then it will generate a specific size file for every tags
  ##NOTE: define size of file is the better thing, because generate a local temporary file on disk and then put it in bucket.
  config :size_file, :validate => :number, :required => true

  # Set the time, in minutes, to close the current sub_time_section of bucket.
  # If you define file_size you have a number of files in consideration of the section and the current tag.
  # 0 stay all time on listerner, beware if you specific 0 and size_file 0, because you will not put the file on bucket,
  # for now the only thing this plugin can do is to put the file when logstash restart.
  config :time_file, :validate => :number, :required => true

  ## IMPORTANT: if you use multiple instance of logstash running, you should specify on one of them the "restore=> true" and on the others "restore => false".
  ## This is hack for not destroy the new files after restoring the initial files.
  ## If you do not specify "restore => true" when logstash crashes or is restarted, the files are not sent into the folder,
  ## for example if you have single Instance.
  config :restore, :validate => :boolean, :default => false

  # Set the directory where logstash will store the tmp files before sending it to Dropbox
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # Specify if outputing to csv ( make sure to specify the required fields in logstash-output-csv if you activate this option )
  config :csv_format, :validate => :boolean, :default => false

  # As csv output file do not allow to output headers, that's the option
  config :csv_headers, :validate => :boolean, :required => false, :default => true

  # Specify how many workers to use to upload the files to Dropbox
  config :upload_workers_count, :validate => :number, :default => 1

  # The token of the Dropbox folder you need to access
  config :token, :validate => :string, :required => true


  # Exposed attributes for testing purpose.
  attr_accessor :tempfile
  attr_reader :page_counter
  attr_reader :dropbox

  def dropbox_config

    @logger.info("Registering dropbox output")

    if @token

      @dropbox = DropboxClient.new(@token)
    else
      @logger.error("Token missing, cannot create Dropbox Client to connect Dropbox.")
    end

  end

  public
  def write_on_bucket(file)

    remote_filename = "#{@path}#{File.basename(file)}"

    @logger.debug("Dropbox: ready to write file in folder", :remote_filename => remote_filename, :path => @path)

    File.open(file, 'r+') { |f| f.write(@fields.to_csv(@csv_options)) } if @csv_headers == true

    File.open(file, 'r') do |fileIO|
      begin
        response = @dropbox.put_file(remote_filename, fileIO)
      rescue Dropbox::Errors::Base => error
        @logger.error("Dropbox: Error", :error => error)
        raise LogStash::Error, "Dropbox Configuration Error, #{error}"
      end
    end

    @logger.debug("Dropbox: has written remote file", :remote_filename => remote_filename)

  end

  # This method is used for create new empty temporary files for use. Flag is needed for indicate new subsection time_file.
  public
  def create_temporary_file
    filename = File.join(@temporary_directory, get_temporary_filename(@page_counter))

    @logger.debug("Dropbox: Creating a new temporary file", :filename => filename)

    @file_rotation_lock.synchronize do
      unless @tempfile.nil?
        @tempfile.close
      end

      @tempfile = File.open(filename, "a")
    end
  end

  public
  def register
    require "dropbox_sdk"

    @csv_options = Hash[@csv_options.map{|(k, v)|[k.to_sym, v]}]

    workers_not_supported

    # @dropbox = dropbox_config
    dropbox_config
    @upload_queue = Queue.new
    @file_rotation_lock = Mutex.new

    if !Dir.exist?(@temporary_directory)
      FileUtils.mkdir_p(@temporary_directory)
    end

    restore_from_crashes if @restore == true
    reset_page_counter
    create_temporary_file
    configure_periodic_rotation if time_file != 0
    configure_upload_workers

    @codec.on_event do |event, encoded_event|
      handle_event(encoded_event)
    end
  end


  public
  def restore_from_crashes
    @logger.debug("Dropbox: is attempting to verify previous crashes...")

    Dir[File.join(@temporary_directory, "*.#{TEMPFILE_EXTENSION}")].each do |file|
      name_file = File.basename(file)
      @logger.warn("Dropbox: have found temporary file the upload process crashed, uploading file to Dropbox.", :filename => name_file)
      move_file_to_bucket_async(file)
    end
  end

  public
  def move_file_to_bucket(file)
    if !File.zero?(file)

      write_on_bucket(file)
      @logger.debug("Dropbox: file was put on the upload thread", :filename => File.basename(file), :bucket => @bucket)
    end

    begin
      File.delete(file)
    rescue Errno::ENOENT
      # Something else deleted the file, logging but not raising the issue
      @logger.warn("Dropbox: Cannot delete the temporary file since it doesn't exist on disk", :filename => File.basename(file))
    rescue Errno::EACCES
      @logger.error("Dropbox: Logstash doesnt have the permission to delete the file in the temporary directory.", :filename => File.basename(file), :temporary_directory => @temporary_directory)
    end
  end

  public
  def periodic_interval
    @time_file * 60
  end

  public
  def get_temporary_filename(page_counter = 0)
    current_time = Time.now
    filename = "ls.dropbox.#{Socket.gethostname}.#{current_time.strftime("%Y-%m-%dT%H.%M")}"

    if @tags.size > 0
      return "#{filename}.tag_#{@tags.join('.')}.part#{page_counter}.#{TEMPFILE_EXTENSION}"
    else
      return "#{filename}.part#{page_counter}.#{TEMPFILE_EXTENSION}"
    end
  end

  public
  def receive(event)
    return unless output?(event)

    if @csv_format
      csv_values = @fields.map {|name| get_value_to_csv(name, event)}
      ev = csv_values.to_csv(@csv_options)
    else
      ev = event
    end

    @codec.encode(ev)
  end

  private
  def get_value_to_csv(name, event)
    val = event[name]
    val.is_a?(Hash) ? LogStash::Json.dump(val) : val
  end

  public
  def rotate_events_log?
    @tempfile.size > @size_file
  end

  public
  def write_events_to_multiple_files?
    @size_file > 0
  end

  public
  def write_to_tempfile(event)
    begin
      @logger.debug("Dropbox: put event into tempfile ", :tempfile => File.basename(@tempfile))

      @file_rotation_lock.synchronize do
        @tempfile.syswrite(event)
      end
    rescue Errno::ENOSPC
      @logger.error("Dropbox: No space left in temporary directory", :temporary_directory => @temporary_directory)
      teardown
    end
  end

  public
  def teardown
    shutdown_upload_workers
    @periodic_rotation_thread.stop! if @periodic_rotation_thread

    @tempfile.close
    finished
  end

  private
  def shutdown_upload_workers
    @logger.debug("Dropbox: Gracefully shutdown the upload workers")
    @upload_queue << LogStash::ShutdownEvent
  end

  private
  def handle_event(encoded_event)
    if write_events_to_multiple_files?
      if rotate_events_log?
        @logger.debug("Dropbox: tempfile is too large, let's bucket it and create new file", :tempfile => File.basename(@tempfile))

        move_file_to_bucket_async(@tempfile.path)
        next_page
        create_temporary_file
      else
        @logger.debug("Dropbox: tempfile file size report.", :tempfile_size => @tempfile.size, :size_file => @size_file)
      end
    end

    write_to_tempfile(encoded_event)
  end

  private
  def configure_periodic_rotation
    @periodic_rotation_thread = Stud::Task.new do
      LogStash::Util::set_thread_name("<Dropbox periodic uploader")

      Stud.interval(periodic_interval, :sleep_then_run => true) do
        @logger.debug("Dropbox: time_file triggered, bucketing the file", :filename => @tempfile.path)

        move_file_to_bucket_async(@tempfile.path)
        next_page
        create_temporary_file
      end
    end
  end

  private
  def configure_upload_workers
    @logger.debug("Dropbox: Configure upload workers")

    @upload_workers = @upload_workers_count.times.map do |worker_id|
      Stud::Task.new do
        LogStash::Util::set_thread_name("<Dropbox upload worker #{worker_id}")

        while true do
          @logger.debug("Dropbox: upload worker is waiting for a new file to upload.", :worker_id => worker_id)

          upload_worker
        end
      end
    end
  end

  private
  def upload_worker
    file = @upload_queue.deq

    case file
      when LogStash::ShutdownEvent
        @logger.debug("Dropbox: upload worker is shutting down gracefuly")
        @upload_queue.enq(LogStash::ShutdownEvent)
      else
        @logger.debug("Dropbox: upload working is uploading a new file", :filename => File.basename(file))
        move_file_to_bucket(file)
    end
  end

  private
  def next_page
    @page_counter += 1
  end

  private
  def reset_page_counter
    @page_counter = 0
  end

  # private
  # def delete_on_bucket(filename)

  #   remote_filename = "#{@path}#{File.basename(filename)}"

  #   @logger.debug("Dropbox: delete file from folder", :remote_filename => remote_filename)

  #   begin
  #     @dropbox.file_delete(remote_filename)
  #   rescue Dropbox::Errors::Base => e
  #     @logger.error("Dropbox: error", :error => e)
  #     raise LogStash::ConfigurationError, "Configuration Error"
  #   end
  # end

  private
  def move_file_to_bucket_async(file)
    @logger.debug("Dropbox: Sending the file to the upload queue.", :filename => File.basename(file))
    @upload_queue.enq(file)
  end
end
