# encoding: utf-8
require "logstash/outputs/base"
require "logstash/outputs/csv"
require "logstash/namespace"
# require "logstash/plugin_mixins/aws_config"
require "stud/temporary"
require "socket" # for Socket.gethostname
require "thread"
require "tmpdir"
require "fileutils"

require "dropbox_sdk"
require_relative "./dropbox-patch"

# INFORMATION:
#
# This plugin was created for store the logstash's events into Amazon Simple Storage Service (Amazon S3).
# For use it you needs authentications and an s3 bucket.
# Be careful to have the permission to write file on S3's bucket and run logstash with super user for establish connection.
#
# S3 plugin allows you to do something complex, let's explain:)
#
# S3 outputs create temporary files into "/opt/logstash/S3_temp/". If you want, you can change the path at the start of register method.
# This files have a special name, for example:
#
# ls.s3.ip-10-228-27-95.2013-04-18T10.00.tag_hello.part0.txt
#
# ls.s3 : indicate logstash plugin s3
#
# "ip-10-228-27-95" : indicate you ip machine, if you have more logstash and writing on the same bucket for example.
# "2013-04-18T10.00" : represents the time whenever you specify time_file.
# "tag_hello" : this indicate the event's tag, you can collect events with the same tag.
# "part0" : this means if you indicate size_file then it will generate more parts if you file.size > size_file.
#           When a file is full it will pushed on bucket and will be deleted in temporary directory.
#           If a file is empty is not pushed, but deleted.
#
# This plugin have a system to restore the previous temporary files if something crash.
#
##[Note] :
#
## If you specify size_file and time_file then it will create file for each tag (if specified), when time_file or
## their size > size_file, it will be triggered then they will be pushed on s3's bucket and will delete from local disk.
## If you don't specify size_file, but time_file then it will create only one file for each tag (if specified).
## When time_file it will be triggered then the files will be pushed on s3's bucket and delete from local disk.
#
## If you don't specify time_file, but size_file  then it will create files for each tag (if specified),
## that will be triggered when their size > size_file, then they will be pushed on s3's bucket and will delete from local disk.
#
## If you don't specific size_file and time_file you have a curios mode. It will create only one file for each tag (if specified).
## Then the file will be rest on temporary directory and don't will be pushed on bucket until we will restart logstash.
#
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
# dropbox {
#   temporary_directory => "/Users/javi/Desktop/logstash/tmp/"
#   type => "customers"
#   size_file => 3048576
#   tags => ["customers", "punt fresc", "raw"]
#   prefix => "/loyal_guru_files/customers/"
#   credentials => [ "90xpj25b6k0qv3e","rrfb8l6f824olm7"]
#   token => "36urfzNJ8pAAAAAAAAAABRnDjV981R7vPk7ZYf0cbMZDvxTJiZ5PM2Ex7P-PwPTx"
# }
#
class LogStash::Outputs::Dropbox < LogStash::Outputs::Base


  TEMPFILE_EXTENSION = "txt"
  # S3_INVALID_CHARACTERS = /[\^`><]/

  config_name "dropbox"
  default :codec, 'line'

  # Set the size of file in bytes, this means that files on bucket when have dimension > file_size, they are stored in two or more file.
  # If you have tags then it will generate a specific size file for every tags
  ##NOTE: define size of file is the better thing, because generate a local temporary file on disk and then put it in bucket.
  config :size_file, :validate => :number, :required => true

  # Set the time, in minutes, to close the current sub_time_section of bucket.
  # If you define file_size you have a number of files in consideration of the section and the current tag.
  # 0 stay all time on listerner, beware if you specific 0 and size_file 0, because you will not put the file on bucket,
  # for now the only thing this plugin can do is to put the file when logstash restart.
  config :time_file, :validate => :number, :default => 0

  ## IMPORTANT: if you use multiple instance of s3, you should specify on one of them the "restore=> true" and on the others "restore => false".
  ## This is hack for not destroy the new files after restoring the initial files.
  ## If you do not specify "restore => true" when logstash crashes or is restarted, the files are not sent into the bucket,
  ## for example if you have single Instance.
  config :restore, :validate => :boolean, :default => false


  # Set the directory where logstash will store the tmp files before sending it to S3
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # Specify a prefix to the uploaded filename, this can simulate directories on S3
  config :prefix, :validate => :string, :default => ''

  # Specify how many workers to use to upload the files to S3
  config :upload_workers_count, :validate => :number, :default => 1

  # your dropbox app credentials
  # Credentials can be specified:
  # - As an ["key","secret"] array
  config :credentials, :validate => :array

  # The token of the folder you need to access
  config :token, :validate => :string, :required => true


  # Exposed attributes for testing purpose.
  attr_accessor :tempfile
  attr_reader :page_counter
  attr_reader :dropbox

  ############################
  ##
  ## ==START== DROPBOX SPECIFICS
  ##
  ############################

  def dropbox_config

    @logger.info("Registering dropbox output")

    if @credentials.length == 2
      @access_key_id = @credentials[0]
      @secret_access_key = @credentials[1]
    else
      @logger.error("Credentials missing, at least one of them.")
    end

    if @credentials && @token
      @dropbox = DropboxClient.new(@token)
    end

  end

  public
  def write_on_bucket(file)

    remote_filename = "#{@prefix}#{File.basename(file)}"

    @logger.debug("Dropbox: ready to write file in bucket", :remote_filename => remote_filename, :bucket => @bucket)

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


  ############################
  ##
  ## ==END== DROPBOX SPECIFICS
  ##
  ############################

  # def aws_s3_config
  #   @logger.info("Registering s3 output", :bucket => @bucket, :endpoint_region => @region)
  #   @s3 = AWS::S3.new(aws_options_hash)
  # end

  # def aws_service_endpoint(region)
  #   # Make the deprecated endpoint_region work
  #   # TODO: (ph) Remove this after deprecation.

  #   if @endpoint_region
  #     region_to_use = @endpoint_region
  #   else
  #     region_to_use = @region
  #   end

  #   return {
  #     :s3_endpoint => region_to_use == 'us-east-1' ? 's3.amazonaws.com' : "s3-#{region_to_use}.amazonaws.com"
  #   }
  # end

  # public
  # def write_on_bucket(file)
  #   # find and use the bucket
  #   bucket = @s3.buckets[@bucket]

  #   remote_filename = "#{@prefix}#{File.basename(file)}"

  #   @logger.debug("Dropbox: ready to write file in bucket", :remote_filename => remote_filename, :bucket => @bucket)

  #   File.open(file, 'r') do |fileIO|
  #     begin
  #       # prepare for write the file
  #       object = bucket.objects[remote_filename]
  #       object.write(fileIO, :acl => @canned_acl)
  #     rescue AWS::Errors::Base => error
  #       @logger.error("Dropbox: AWS error", :error => error)
  #       raise LogStash::Error, "AWS Configuration Error, #{error}"
  #     end
  #   end

  #   @logger.debug("Dropbox: has written remote file in bucket with canned ACL", :remote_filename => remote_filename, :bucket  => @bucket, :canned_acl => @canned_acl)
  # end



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
    # required if using ruby version < 2.0
    # http://ruby.awsblog.com/post/Tx16QY1CI5GVBFT/Threading-with-the-AWS-SDK-for-Ruby
    # AWS.eager_autoload!(AWS::S3)

    workers_not_supported

    @dropbox = dropbox_config
    @upload_queue = Queue.new
    @file_rotation_lock = Mutex.new


    if !Dir.exist?(@temporary_directory)
      FileUtils.mkdir_p(@temporary_directory)
    end

    # restore_from_crashes if @restore == true
    reset_page_counter
    create_temporary_file
    configure_periodic_rotation if time_file != 0
    configure_upload_workers

    @codec.on_event do |event, encoded_event|
      handle_event(encoded_event)
    end
  end


  # public
  # def restore_from_crashes
  #   @logger.debug("Dropbox: is attempting to verify previous crashes...")

  #   Dir[File.join(@temporary_directory, "*.#{TEMPFILE_EXTENSION}")].each do |file|
  #     name_file = File.basename(file)
  #     @logger.warn("Dropbox: have found temporary file the upload process crashed, uploading file to S3.", :filename => name_file)
  #     move_file_to_bucket_async(file)
  #   end
  # end

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
    csv_values = @fields.map {|name| get_value_to_csv(name, event)}
    event["message"] = csv_values.to_csv(@csv_options)
    @codec.encode(event["message"])
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

  private
  def delete_on_bucket(filename)
    # bucket = @s3.buckets[@bucket]

    remote_filename = "#{@prefix}#{File.basename(filename)}"

    @logger.debug("Dropbox: delete file from folder", :remote_filename => remote_filename)

    begin
      # prepare for write the file
      # object = bucket.objects[remote_filename]
      # object.delete
      @dropbox.file_delete(remote_filename)
    rescue Dropbox::Errors::Base => e
      @logger.error("Dropbox: error", :error => e)
      raise LogStash::ConfigurationError, "Configuration Error"
    end
  end

  private
  def move_file_to_bucket_async(file)
    @logger.debug("Dropbox: Sending the file to the upload queue.", :filename => File.basename(file))
    @upload_queue.enq(file)
  end
end
