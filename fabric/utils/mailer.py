import os
import logging
from contextlib import contextmanager
import traceback
import smtplib
from email.message import EmailMessage

logger = logging.getLogger(__name__)


def send_email(
    subject, body, to,
    sender="warning@whc.is",
):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = to

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


@contextmanager
def warn_email(to="whc@ttic.edu", subject=None, do_send=True):
    """note that contextmanager can be used as a decorator as well"""
    if subject is None:
        subject = os.environ.get('SLURM_JOB_NAME', 'shell')
    try:
        yield
    except Exception as e:
        logger.info(f"Exception triggered. Emailing {to}")
        if do_send:
            send_email(subject=subject, body=traceback.format_exc(), to=to)
        raise (e)
    finally:
        pass
