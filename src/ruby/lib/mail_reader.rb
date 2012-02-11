# The purpose of this library is to access emails via IMAP, and to persist them as avros

require 'rubygems'
require 'net/imap'
require 'json'
require 'avro'
require 'pp'
require 'date'
#require 'unicode'
require 'iconv'
$KCODE = 'UTF-8'

class MailReader

  attr_accessor :imap, :email_address, :password, :folder, :message_count, :directory, :current_filename, :thread_count
  
  def initialize(email_address, password, thread_count, directory)
    @email_address = email_address
    @password = password
    @message_count = message_count.to_i
    @folder = '[Gmail]/All Mail'
    @directory = init_directory(directory)
    @file_size = 33554432 #32MB max
    @thread_count = thread_count.to_i
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
  
  def init_avro(part_id)
    @current_filename = "#{@directory}/part-#{part_id}.avro"
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
    Signal.trap("SIGINT") { @imap.disconnect; break; exit }
  end
  
  def read
    connect if !@imap or @imap.disconnected?
    all_message_ids = @imap.search(['ALL']).reverse
    
    id_slices = all_message_ids.each_slice(all_message_ids.size/@thread_count).to_a
    puts "Spawning #{id_slices.size} threads to scrape IMAP..."
    
    threads = []
    id_slices.each_with_index do |message_ids, thread_id|
      threads << Thread.new(message_ids, thread_id) do |message_ids, thread_id|
        puts "Worker #{thread_id} booting imap..."
        file_counter = 0
        avros = init_avro("#{thread_id}-#{file_counter}")
        message_ids[0..-1].each do |message_id|
          # Fetch the message
          begin
            if file_done
              file_counter += 1
              avros.close
              avros = init_avro("#{thread_id}-#{file_counter}")
              file_counter += 1
            end
            # Net::IMAP::Envelope is small, and fast
            envelope = @imap.fetch(message_id, 'ENVELOPE')[0].attr['ENVELOPE']
            body = fetch_body(message_id)
            avroize(avros, envelope, body)
            puts "#{thread_id}: #{envelope.subject}"
          rescue Exception => e
            connect if @imap.disconnected? 
            puts "Exception parsing email: #{e.class} #{e.message} #{e.backtrace}}"
            next
          rescue EOFError, IOError, Error => e
            connect if @imap.disconnected? 
            puts "Error with IMAP connection: #{e.class} #{e.message}"
          end
        end
      @imap.disconnect
      avros.close
      end
    end
    threads.each { |a_thread|  a_thread.join }
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
      body_text = []      
      body.parts.each do |part|
        if part.media_type == 'TEXT'
          if part.subtype == 'PLAIN'
            body_text << @imap.fetch(message_id, "BODY[#{count}]")[0].attr.first[1]
            break
          end
        end
      end
      body_text = convert_to_utf8(body_text) if not_utf
    else
      body_text = @imap.fetch(message_id, "BODY[1]")[0].attr.first[1]
      body_text = convert_to_utf8(body) if not_utf
      body_text
    end
  end
  
  def avroize(avros, envelope, body)
    record = {}
    
    ['from', 'to', 'cc', 'bcc', 'reply_to', 'in_reply_to'].each do |key|
      if envelope.send(key)
        record[key] = convert_to_utf8 emails_to_strings(envelope.send(key))
      end
    end
    
    ['message_id', 'subject'].each do |key|
      if envelope.send(key)
        record[key] = convert_to_utf8 envelope.send(key).to_s
      end
    end
    
    # Parse and convert to ISO format
    record['date'] = convert_to_utf8 DateTime.parse(envelope.date).to_s

    record['body'] = body
    avros << record
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
