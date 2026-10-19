"""A local stand-in for SES inbound: accepts SMTP on port 2525 and stores every message, as raw
MIME, under raw/inbox/<recipient>/ in the raw zone (SES receipt rules write to S3 the same way).

Synthetic mail only. It accepts any recipient; the email fetcher decides what counts.
"""
import asyncio
import hashlib
from datetime import datetime, timezone

from aiosmtpd.controller import Controller

from loader import settings
from loader.fetch import inbox_prefix
from loader.raw import RawZone


class StoreInRawZone:
    def __init__(self):
        self.zone = RawZone(settings.load())

    async def handle_DATA(self, server, session, envelope):
        data = envelope.original_content or envelope.content
        stamp = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}-{hashlib.sha256(data).hexdigest()[:12]}"
        for recipient in envelope.rcpt_tos:
            self.zone.put(f"{inbox_prefix(recipient.lower())}{stamp}.eml", data)
        return "250 Message accepted"


if __name__ == "__main__":
    controller = Controller(StoreInRawZone(), hostname="0.0.0.0", port=2525)
    controller.start()
    asyncio.get_event_loop().run_forever()
