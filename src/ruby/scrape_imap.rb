# The purpose of this library is to access emails via IMAP, and to build a graph out of them
# 
# Usage: jruby lib/process_imap.sh <username@gmail.com>
#

require 'net/imap'
require 'tmail'
require 'json'
require 'uri'
require 'gmail_xoauth'
require 'aws/s3'

require 'jcode'
$KCODE = 'UTF8'

class ScrapeImap

  attr_accessor :imap, :user_key, :user_email, :folder, :interrupted, :message_count
  PREFIX = "imap:"
  
  def initialize(user_email, message_count, )
    @user_email = user_email
    @user_key = PREFIX + user_email
    @message_count = message_count.to_i
    trap_signals
  end
  
  def trap_signals
    # Trap ctrl-c
    @interrupted = false
    trap("SIGINT") { @interrupted = true }
    trap("SIGALRM") { puts "Caught SIG_ALRM so it doesn't complain"}
  end

  def scan_folder
    count = 0
    skipped_ids = []
    init_imap
 
    messages = imap.search(['ALL'])
    messages = messages.reverse # Most recent first

    messages[resume_id..@message_count].each do |message_id|
      # Trap ctrl-c to persist
      if @interrupted
        
        exit
      end

      # Fetch the message
      begin
        msg = @imap.fetch(message_id,'RFC822.HEADER')[0].attr['RFC822.HEADER']
        mail = TMail::Mail.parse(msg)
        # No from node - skip
        next unless mail.header['from'] and mail.header['from'].respond_to? 'addrs'

        # Get a count of all edges to get a divisor for outgoing edge weights
        recipient_count = self.count_recipients(mail)
        from_addresses = mail.header['from'].addrs
        unless from_addresses
          puts "Skipped email without a from address!"
          skipped_ids << message_id
          next
        end
      rescue Exception => e
        skipped_ids << message_id
        puts e.message + e.backtrace.join("\n")
        next
      end
  
      # Build connection graph from recipients
      begin
        from_addresses.each do |t_from|
          from_address = t_from.address.downcase.gsub /"/, '' #"
          from = @graph.find_or_create_vertex({:type => 'email', :Label => from_address, :network => @user_email}, :Label)
      
          self.build_connections from_address, from, mail, recipient_count, message_id
        end

        # Persist to Voldemort as JSON and /tmp as graphml every 100 emails processed
        if ((count % 100) == 0) and (count > 0)
          print "."
        end
    
      rescue Exception => e
        puts "Exception parsing email: #{e.class} #{e.message} #{e.backtrace}}"
        next
      # IMAP connections die. Ressucitate.
      rescue EOFError, IOError, Error => e
        puts "Error parsing email: #{e.class} #{e.message}"
        init_imap    
      end
      count += 1
    end
  end
  
  def build_connections(from_address, from, mail, recipient_count, message_id)
    for type in ['to', 'cc', 'bcc']
      if mail.header[type] and mail.header[type].respond_to? 'addrs'
        to_addresses = mail.header[type].addrs
        to_addresses.each do |t_to|
          to_address = t_to.address.downcase.gsub /"/, '' #"
          to = @graph.find_or_create_vertex({:type => 'email', :Label => to_address, :network => @user_email}, :Label)
          edge, status = @graph.find_or_create_edge(from, to, 'sent')
          props = edge.properties || {}
          added_weight = 1.0/(recipient_count||1.0)
          to['Weight'] ||= 0
          to['Weight'] += added_weight
          # Ugly as all hell, but JSON won't let you have a numeric key in an object...
          props.merge!({ 'Weight' => ((props['Weight'].to_i || 0) + added_weight).to_s })
          edge.properties = props
          puts "[#{message_id}] #{from_address} --> #{to_address} [#{type}] #{props['Weight']}"
        end
      end
    end
  end
  
  def init_imap
    @folder = '[Gmail]/All Mail'
    @imap.close if @imap and @imap.respond_to? 'close'
    @imap = Net::IMAP.new('imap.gmail.com', 993, usessl = true, certs = nil, verify = false)
    consumer_key = ENV["CONSUMER_KEY"] || ENV["consumer_key"]
    consumer_secret = ENV["CONSUMER_SECRET"] || ENV["consumer_secret"]
    token_json = @redis.get 'access_token:' + @user_email
    token = JSON token_json
    @imap.authenticate('XOAUTH', @user_email,
      :consumer_key => consumer_key,
      :consumer_secret => consumer_secret,
      :token => token['token'],
      :token_secret => token['secret']
    )
    @imap.examine(@folder) # examine is read only
  end
  
  def count_recipients(mail)
    recipient_count = 0
    for to in ['to', 'cc', 'bcc']
      if mail.header and mail.header[to] and mail.header[to].respond_to? 'addrs'
        recipient_count += mail.header[to].addrs.size
      end                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
    end
    recipient_count
  end

end
