"""
Entry point to start the Telegram bot listener.

Usage:
  python scripts/run_bot.py

This runs continuously — use tmux or systemd to keep it alive:
  tmux new -s bot
  source .venv/bin/activate
  python scripts/run_bot.py
  # Ctrl+B, D to detach

The bot runs independently from the daily pipeline cron job.
The pipeline sends messages directly. The bot listens for commands.
"""

import logging
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.common.config import load_config
from src.common.config import load_env
from src.common.logger import setup_root_logging
from src.notifier.bot import start_bot


def main() -> None:
    """
    Load configuration, set up logging, and start the Telegram bot.

    Calls start_bot() which runs indefinitely in long-polling mode.
    Handles KeyboardInterrupt (Ctrl+C) for clean shutdown.
    """
    load_env()
    setup_root_logging()

    logger = logging.getLogger(__name__)
    logger.info("phase=run_bot starting Telegram bot listener")

    notifier_config = load_config("notifier")

    try:
        start_bot(notifier_config)
    except KeyboardInterrupt:
        logger.info("phase=run_bot bot stopped by user (KeyboardInterrupt)")
        sys.exit(0)
    except Exception as exc:
        logger.error("phase=run_bot fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
