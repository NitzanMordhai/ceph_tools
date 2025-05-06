import logging
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Dict
import re

logger = logging.getLogger(__name__)

class EmailSender:
    """
    Sends formatted reports via SMTP.
    """
    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.server = cfg.smtp_server
        self.port = cfg.smtp_port
        self.username = cfg.smtp_username
        self.password = cfg.smtp_password
        self.sender = cfg.email_sender

    def send(self, subject: str, body: str, image_cids: Dict[str, str]) -> None:
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = self.sender

        emails = self.cfg.email

        if isinstance(emails, (list, tuple)):
            raw = " ".join(emails)
        else:
            raw = emails or ""

        tokens = re.split(r"[,\s]+", raw.strip())
        recipients = [t for t in tokens if t]
        msg['To'] = ", ".join(recipients)

        msg.set_content(body)
        # Attach inline images if provided
        for cid, img_file in image_cids.items():
            path = Path(self.cfg.output_dir) / img_file
            if path.exists():
                data = path.read_bytes()
                maintype, subtype = 'image', path.suffix.lstrip('.')
                msg.add_attachment(
                    data,
                    maintype=maintype,
                    subtype=subtype,
                    filename=path.name,
                    cid=cid,
                )

        logger.debug("Connecting to SMTP %s:%s", self.server, self.port)
        with smtplib.SMTP(self.server, self.port) as smtp:
            smtp.starttls()
            if self.username and self.password:
                smtp.login(self.username, self.password)
            smtp.send_message(msg, to_addrs=recipients)
            logger.info("Email sent to %s", recipients)