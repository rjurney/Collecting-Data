-- Select a single email as we might view it in raw format.
select m.smtpid as id, 
       m.messagedt as date, 
       s.email as sender,
       (select group_concat(CONCAT(r.reciptype, ':', p.email) SEPARATOR ' ') from recipients r join people p ON r.personid=p.personid where r.messageid = 511) as to_cc_bcc,
       m.subject as subject, 
       SUBSTR(b.body, 1, 200) as body
            from messages m 
            join people s
                on m.senderid=s.personid
            join bodies b 
                on m.messageid=b.messageid 
                    where m.messageid=511;

-- Select an ordered list of what time a person sends emails
select senderid as id, 
       hour(messagedt) as sent_hour, 
       count(*) 
           from messages 
           where senderid=1 
           group by senderid, 
                    m_hour 
           order by senderid, 
                    m_hour;
