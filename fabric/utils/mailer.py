import logging
import functools
from contextlib import contextmanager
import traceback
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send_email(subject, body, to):
    s = smtplib.SMTP('localhost')
    mime = MIMEText(body)
    mime['Subject'] = subject
    mime['To'] = to
    s.sendmail('lab', to, mime.as_string())


@contextmanager
def warn_email(subject, address, do_send=True):
    try:
        yield
    except Exception as e:
        logger.info("Exception triggered. Emailing {}".format(address))
        if do_send:
            send_email(
                subject=subject, body=traceback.format_exc(), to=address
            )
        raise (e)
    finally:
        pass


# class ExceptionEmail:
#     """
#     A decorator that sends a warning email when catching
#     exceptions. The caught exception will be re-raised.
#     """
#     def __init__(self, subject, address, do_send=True):
#         """
#         Args:
#             subject: title of the email; some identifying metadata
#             address: mail recipient address
#         """
#         self.subject = subject
#         self.address = address
#         self.do_send = do_send  # used to ensure only main process send email

#     def __call__(self, f):
#         @functools.wraps(f)
#         def ret(*args, **kwargs):
#             try:
#                 f(*args, **kwargs)
#             # except KeyboardInterrupt:
#             #     pass  # do not send email on sigint
#             except Exception as e:
#                 logger.info("Exception triggered. Emailing {}".format(self.address))
#                 if self.do_send:
#                     send_email(
#                         subject=self.subject,
#                         body=traceback.format_exc(),
#                         to=self.address
#                     )
#                 raise(e)
#         return ret


# def _test():
#     target = 'whc@ttic.edu'
#     send_email('ttic.mailer sending test', 'this is a test', target)

#     @ExceptionEmail('ttic.mailer decorator test', target)
#     def bad_func():
#         return 1 / 0

#     bad_func()


# if __name__ == '__main__':
#     _test()
