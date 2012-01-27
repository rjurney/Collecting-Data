# The purpose of this library is to access emails via IMAP, and to persist them as avros

require 'rubygems'
require 'net/imap'
require 'mail'
require 'json'
require 'avro'

class MailReader

  attr_accessor :imap, :email_address, :password, :folder, :message_count, :avros, :filename
  
  def initialize(email_address, password, message_count, filename)
    @email_address = email_address
    @password = password
    @message_count = message_count.to_i
    @folder = '[Gmail]/All Mail'
    @filename = filename
    @avros = init_avro
    trap_signals
  end

  def init_avro
    file = File.open(@filename, 'wb')
    schema = Avro::Schema.parse IO.read(File.dirname(__FILE__) + '/../../avro/email.schema')
    writer = Avro::IO::DatumWriter.new(schema)
    dw = Avro::DataFile::Writer.new(file, writer, schema)
  end
  
  def trap_signals
    # Trap ctrl-c
    Signal.trap("SIGINT") { @imap.disconnect; @avros.close; exit }
  end
  
  def read
    connect if !@imap or @imap.disconnected?
    message_ids = @imap.search(['ALL']).reverse
    message_ids[0..@message_count].each do |message_id|
      # Fetch the message
      begin
        msg_string = @imap.fetch(message_id,'RFC822')[0].attr['RFC822']
        email = Mail.new msg_string
        avroize email
        puts email.subject
      rescue Exception => e
        puts "Exception parsing email: #{e.class} #{e.message} #{e.backtrace}}"
        next
      rescue EOFError, IOError, Error => e
        puts "Error with IMAP connection: #{e.class} #{e.message}"
        connect if @imap.disconnected? 
      end
    end
    @avros.close
    @imap.disconnect
  end
  
  def avroize(email)
    record = {}
    ['message_id', 'to', 'cc', 'bcc', 'reply_to', 'subject'].each do |key|
      record[key] = email.send(key) if email.send(key)
    end
    ['body', 'from', 'date'].each do |key|
      record[key] = email.send(key).to_s if email.send(key)
    end
    @avros << record
  end
  
  def connect
    @imap.close if @imap and @imap.respond_to? 'close'
    @imap = Net::IMAP.new('imap.gmail.com', 993, true)
    @imap.login(@email_address, @password)
    @imap.examine(@folder) # examine is read only
  end

end
