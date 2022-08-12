import os
from pathlib import Path
import json
from contextlib import contextmanager

import traceback
import socket

import telebot


class ChatBot():
    def __init__(self):
        with Path("~/telebot_secrets.json").expanduser().open("r") as f:
            secrets = json.load(f)
            token = secrets['bot_token']
            chat_id = secrets['my_chat_id']

        self.bot = telebot.TeleBot(token, parse_mode=None)
        self.chat_id = chat_id

    def msg(self, payload):
        self.bot.send_message(self.chat_id, payload)


@contextmanager
def warn_bot(enabled=None):
    if enabled is None:
        is_remote = bool(os.environ.get("IS_REMOTE", False))
        enabled = is_remote

    if not enabled:
        yield
    else:
        hostname = socket.gethostname()
        bot = ChatBot()
        try:
            yield
        except Exception as e:
            bot.msg(f"{hostname} err msg:\n\n {traceback.format_exc()}")
            raise (e)


@warn_bot(True)
def test_func():
    assert (1 == 2)


def main():
    # bot = NotificationBot()
    # bot.msg("hello there!")
    test_func()


if __name__ == "__main__":
    main()
