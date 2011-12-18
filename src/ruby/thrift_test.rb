# include thrift-generated code
require 'thrift'
$:.push('../thrift/gen-rb')
require 'email_types'

serializer = Thrift::Serializer.new

from = DataSyndrome::EmailAddress.new(:address => 'bob@bob.com', :name => 'Bob Jones')
to = DataSyndrome::EmailAddress.new(:address => 'jim@foo.com')
email1 = DataSyndrome::Email.new(:from => from, :to => [to], :subject => 'Hi Jim!', :body => 'I would like to play chess.')
email2 = DataSyndrome::Email.new(:from => to, :to => [from], :subject => 'Hi Jim!', :body => 'I would like to play chess.')

# Write records to file
file = File.open('/tmp/emails.dat', 'wb')
file << serializer.serialize(email1)
file << serializer.serialize(email2)
file.close

# Read records from the file
file = File.open('/tmp/emails.dat', 'wb')

# 
# 
# AWS::S3::Base.establish_connection!(
#   :access_key_id     => ENV['AWS_ACCESS_KEY_ID'],
#   :secret_access_key => ENV['AWS_SECRET_ACCESS_KEY'])
# 
# bucket = AWS::S3::Bucket.find 'kontexa.email'