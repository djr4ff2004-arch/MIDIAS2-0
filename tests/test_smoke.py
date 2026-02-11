import unittest
import asyncio

try:
    import telegram  # type: ignore
    from telegram.error import RetryAfter  # type: ignore
    from telegram.ext import Application  # type: ignore
    TELEGRAM_AVAILABLE = True
except Exception:
    TELEGRAM_AVAILABLE = False
    RetryAfter = None  # type: ignore
    Application = None  # type: ignore

if TELEGRAM_AVAILABLE:
    from bot.main import VIPBot
else:
    VIPBot = None  # type: ignore


@unittest.skipUnless(TELEGRAM_AVAILABLE, "python-telegram-bot not available in this environment")
class SmokeTests(unittest.TestCase):
    class DummyPTBBot:
        def __init__(self, exc):
            self._exc = exc
            self.calls = 0

        async def send_message(self, *args, **kwargs):
            self.calls += 1
            raise self._exc

        async def edit_message_text(self, *args, **kwargs):
            self.calls += 1
            raise self._exc

    def test_setup_handlers_exists(self):
        bot = VIPBot()  # type: ignore
        self.assertTrue(hasattr(bot, "setup_handlers"))

    def test_retryafter_wrapper_does_not_crash(self):
        exc = RetryAfter(5)  # type: ignore
        bot = VIPBot()  # type: ignore
        dummy = self.DummyPTBBot(exc)

        async def run():
            ok = await bot._safe_send_message(dummy, chat_id=1, text="x")
            self.assertFalse(ok)

        asyncio.run(run())

    def test_post_init_no_jobqueue_required(self):
        bot = VIPBot()  # type: ignore
        app = Application.builder().token("123456:ABCDEF").build()  # type: ignore

        async def run():
            await bot.post_init(app)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
