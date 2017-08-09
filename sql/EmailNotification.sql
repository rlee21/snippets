CREATE OR REPLACE PROCEDURE FINANCE_STAGING.emailAlert (mess in varchar2, etype in number) 
as
    mailhost    varchar2(64) := 'mailhost.light.ci.seattle.wa.us';
    mailfrom    varchar2(64) := 'finance_staging@EPMSTAGP';
    mailto      varchar2(64);
    mailconn    utl_smtp.connection;
    maildate    varchar2(20);
    vreply      utl_smtp.reply;
    vreplies    utl_smtp.replies;
    emailadd    notify_emails.email%type;
    emailType   notify_emails.etype%type;
begin

  -- Sending email

      vreply := utl_smtp.open_connection(mailHOST, 25, mailCONN);

      vreplies := utl_smtp.ehlo(mailCONN, mailHOST);

      vreply := utl_smtp.mail(mailCONN, mailFROM);

      emailType := etype;
      
      for mail_to in (select email as emailAdd,etype from notify_emails)
      loop
        if mail_to.etype = 1 then
          vreply := utl_smtp.rcpt(mailCONN, mail_to.emailAdd);
        end if;
        if mail_to.etype = 2 and emailType = 2 then
          vreply := utl_smtp.rcpt(mailCONN, mail_to.emailAdd);
        end if;
      end loop;

      vreply := utl_smtp.open_data(mailCONN);

      utl_smtp.write_data(mailCONN, 'Subject: '||mess         || utl_tcp.CRLF);
      utl_smtp.write_data(mailCONN, 'From:    '||mailFROM     || utl_tcp.CRLF);
      utl_smtp.write_data(mailCONN, 'Date:    '||mailDATE     || utl_tcp.CRLF);
      utl_smtp.write_data(mailCONN, 'To:      '||mailTO       || utl_tcp.CRLF);

      vreply := utl_smtp.close_data(mailCONN);

      vreply := utl_smtp.quit(mailCONN);

END emailAlert;
/
