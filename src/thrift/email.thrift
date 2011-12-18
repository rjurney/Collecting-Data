namespace java com.datasyndrome.thrift
namespace rb DataSyndrome
namespace py datasyndrome

struct EmailAddress {
  1: string address,
  2: string name,
}

struct Email {
  1: required EmailAddress from,
  2: list<EmailAddress> to,
  3: list<EmailAddress> cc,
  4: list<EmailAddress> bcc,
  5: string reply_to,
  6: string subject,
  7: string date,
  8: string message_id,
  9: string body,
}
