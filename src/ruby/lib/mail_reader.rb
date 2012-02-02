# The purpose of this library is to access emails via IMAP, and to persist them as avros

require 'rubygems'
require 'net/imap'
require 'mail'
require 'json'
require 'avro'
require 'unicode'
$KCODE = 'UTF-8'

class MailReader

  attr_accessor :imap, :email_address, :password, :folder, :message_count, :avros, :directory, :current_filename
  
  def initialize(email_address, password, message_count, directory)
    @email_address = email_address
    @password = password
    @message_count = message_count.to_i
    @folder = '[Gmail]/All Mail'
    @directory = init_directory(directory)
    @file_size = 1048576 #33554432 #32MB max
    trap_signals
  end

  def init_directory(directory)
    if File.exists?(directory)
      puts "Filename #{directory} already exists"
      exit
    end
    Dir.mkdir directory
    directory
  end
  
  def init_avro(count)
    @current_filename = "#{@directory}/part-#{count}"
    file = File.open(@current_filename, 'wb')
    schema = Avro::Schema.parse IO.read(File.dirname(__FILE__) + '/../../avro/email.schema')
    writer = Avro::IO::DatumWriter.new(schema)
    dw = Avro::DataFile::Writer.new(file, writer, schema)
  end
  
  def file_done
    if File.size(@current_filename) > @file_size
      return true
    else
      return false
    end
  end
  
  def trap_signals
    # Trap ctrl-c
    Signal.trap("SIGINT") { @imap.disconnect; @avros.close; break; exit }
  end
  
  def read
    file_counter = 0
    @avros = init_avro(file_counter)
    connect if !@imap or @imap.disconnected?
    message_ids = @imap.search(['ALL']).reverse
    @message_count = message_ids.size if @message_count == 0
    message_ids[0..@message_count].each do |message_id|
      # Fetch the message
      begin
        if file_done
          file_counter += 1
          @avros.close
          @avros = init_avro(file_counter)
        end
        msg_string = @imap.fetch(message_id,'RFC822')[0].attr['RFC822']
        email = Mail.new msg_string
        avroize email
        puts email.subject
      rescue Exception => e
        connect if @imap.disconnected? 
        puts "Exception parsing email: #{e.class} #{e.message} #{e.backtrace}}"
        next
      rescue EOFError, IOError, Error => e
        puts "Error with IMAP connection: #{e.class} #{e.message}"
        connect if @imap.disconnected? 
      end
    end
    @imap.disconnect
    @avros.close
  end
  
  def avroize(email)
    record = {}
    ['message_id', 'to', 'cc', 'bcc', 'reply_to', 'subject'].each do |key|
      record[key] = email.send(key) if email.send(key)
    end
    ['from', 'date'].each do |key|
      record[key] = email.send(key).to_s if email.send(key)
    end
    
    # Must convert to UTF-8, or our Avros won't parse
    record['body'] = email.body.encoded.toutf8
    @avros << record
  end
  
  def connect
    @imap.close if @imap and @imap.respond_to? 'close'
    @imap = Net::IMAP.new('imap.gmail.com', 993, true)
    @imap.login(@email_address, @password)
    @imap.examine(@folder) # examine is read only
  end

end
