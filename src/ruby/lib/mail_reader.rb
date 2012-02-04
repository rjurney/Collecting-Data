# The purpose of this library is to access emails via IMAP, and to persist them as avros

require 'rubygems'
require 'net/imap'
require 'json'
require 'avro'
require 'pp'
require 'unicode'
require 'iconv'
$KCODE = 'UTF-8'

class MailReader

  attr_accessor :imap, :email_address, :password, :folder, :message_count, :avros, :directory, :current_filename
  
  def initialize(email_address, password, message_count, directory)
    @email_address = email_address
    @password = password
    @message_count = message_count.to_i
    @folder = '[Gmail]/All Mail'
    @directory = init_directory(directory)
    @file_size = 33554432 #32MB max
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
    @current_filename = "#{@directory}/part-#{count}.avro"
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
        # Net::IMAP::Envelope is small, and fast
        envelope = @imap.fetch(message_id, 'ENVELOPE')[0].attr['ENVELOPE']
        body = fetch_body(message_id)
        avroize(envelope, body)
        puts envelope.subject
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
  
  def fetch_body(message_id)
    body = @imap.fetch(message_id, "BODY")[0].attr["BODY"]
    return nil unless body

    not_utf = false
    begin
      not_utf = body['param']['CHARSET'] == "ISO-8859-1"
      
    rescue
      not_utf = false
    end
    
    if body.multipart?
      count = 1
      text_parts = []
      body.parts.each do |part|
        if part.media_type == 'TEXT'
          if part.subtype == 'PLAIN'
            text_parts << @imap.fetch(message_id, "BODY[#{count}]")[0].attr.first[1]
            count += 1
          end
        end
      end
      body_text = text_parts.join
    else
      body_text = @imap.fetch(message_id, "BODY[1]")[0].attr.first[1]
      body_text = convert_to_utf8(body) if not_utf
      body_text
    end
  end
  
  def avroize(envelope, body)
    record = {}
    
    ['from', 'to', 'cc', 'bcc', 'reply_to', 'in_reply_to'].each do |key|
      if envelope.send(key)
        record[key] = convert_to_utf8 emails_to_strings(envelope.send(key))
      end
    end
    
    ['message_id', 'subject', 'date'].each do |key|
      if envelope.send(key)
        record[key] = convert_to_utf8 envelope.send(key).to_s
      end
    end

    record['body'] = body
    @avros << record
  end
  
  # From Net::IMAP::Address to array of strings
  def emails_to_strings(addresses)
    if addresses && addresses.is_a?(Array)
      addresses.map do |address|
        "#{address.mailbox}@#{address.host}"
      end
    end
  end
  
  def convert_to_utf8(part)
    if part.is_a? String
      iconv part
    elsif part.is_a? Array
      part.map {|p| iconv p}
    end
  end
  
  def iconv(text)
    converted_text = Iconv.conv('iso-8859-15', 'utf-8', text)
  end
  
  def connect
    begin
      @imap.close if @imap and @imap.respond_to? 'close'
    rescue IOError
      puts "."
    ensure
      @imap = Net::IMAP.new('imap.gmail.com', 993, true)
      @imap.login(@email_address, @password)
      @imap.examine(@folder) # examine is read only
    end
  end

end
