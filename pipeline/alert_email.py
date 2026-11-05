#!/usr/bin/env python3
"""Send one alert email. The estate's single alert path (2026-08-19).

Alerts used to go to an ntfy topic. Publishing worked and the server returned a
message id every time, but not one message ever reached the owner's phone -- the
iOS app showed them when opened and never pushed. A channel whose delivery cannot
be observed from either end is not a channel, so the estate moved to Gmail SMTP,
the one path here with proven delivery. See Development/LESSONS.md L015.

Env: GMAIL_APP_PASSWORD (required to actually send), ALERT_EMAIL (recipient),
ALERT_FROM (sender and SMTP login).

    python pipeline/alert_email.py --title "..." --body "..."

Always prints the alert, exits 0 even when unconfigured or when SMTP fails: an
alert step must never be the reason a green pipeline reports red, and the run log
keeps the text either way.
"""
import argparse
import os
import smtplib
import sys
from email.message import EmailMessage


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", required=True)
    ap.add_argument("--body", required=True)
    args = ap.parse_args()

    print(f"{args.title}\n\n{args.body}")

    pw = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    to = os.environ.get("ALERT_EMAIL", "").strip()
    frm = os.environ.get("ALERT_FROM", "").strip()
    if not (pw and to and frm):
        print("(GMAIL_APP_PASSWORD / ALERT_EMAIL / ALERT_FROM not set — printed only, no email sent)")
        return 0

    msg = EmailMessage()
    msg["From"] = frm
    msg["To"] = to
    msg["Subject"] = args.title
    msg.set_content(args.body)
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as s:
            s.starttls()
            s.login(frm, pw)
            s.send_message(msg)
        print(f"Alert emailed to {to}")
    except Exception as e:
        # Print the reason in full. A swallowed send error is how this estate ran
        # a month on an alert channel that delivered nothing.
        print(f"Alert email FAILED: {e!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
