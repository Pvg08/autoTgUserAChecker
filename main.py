import asyncio
import random

from tg_client import InteractiveTelegramClient


def main():
    random.seed()
    loop = asyncio.get_event_loop()
    client = InteractiveTelegramClient('config.ini', loop)
    loop.run_until_complete(client.run())


if __name__ == '__main__':
    main()
