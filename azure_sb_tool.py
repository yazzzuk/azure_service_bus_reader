#!/usr/bin/env python3
"""
azure_sb_peek.py — Peek messages from an Azure Service Bus queue (active + DLQ).

Usage:
  python azure_sb_peek.py "<connection-string>" [--max 50] [--queue NAME] [--active-only | --dlq-only]

Install:
  pip install azure-servicebus==7.12.3
"""

import argparse
import base64
import json
import sys
from datetime import datetime
from typing import Any, Optional, Tuple

from azure.servicebus import ServiceBusClient, ServiceBusSubQueue


def parse_connection_string(cs: str) -> Tuple[str, Optional[str]]:
    """Return (namespace_cs_without_entity, entity_path or None)."""
    parts = {}
    for kv in [p for p in cs.strip().split(';') if p]:
        if '=' not in kv:
            continue
        k, v = kv.split('=', 1)
        parts[k.strip()] = v.strip()
    required = ['Endpoint', 'SharedAccessKeyName', 'SharedAccessKey']
    for r in required:
        if r not in parts:
            raise ValueError(f"Connection string missing required part: {r}")
    entity = parts.get('EntityPath')
    namespace_cs = f"Endpoint={parts['Endpoint']};SharedAccessKeyName={parts['SharedAccessKeyName']};SharedAccessKey={parts['SharedAccessKey']}"
    return namespace_cs, entity


def fmt_dt(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    return str(dt)


def _bytes_to_text_or_b64(b: bytes) -> str:
    """Try UTF-8; if not decodable, return base64 with a marker."""
    try:
        return b.decode('utf-8')
    except Exception:
        return "base64:" + base64.b64encode(b).decode('ascii')


def to_jsonable(obj: Any):
    """Recursively convert Azure SB message fields into JSON-serialisable types."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return fmt_dt(obj)
    if isinstance(obj, bytes):
        return _bytes_to_text_or_b64(obj)
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            # Ensure keys are strings; convert bytes keys too
            if isinstance(k, bytes):
                key = _bytes_to_text_or_b64(k)
            else:
                key = str(k)
            out[key] = to_jsonable(v)
        return out
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(x) for x in obj]
    # Fallback to string for anything odd (UUID, proprietary types, etc.)
    return str(obj)


def safe_body_to_str(sb_message) -> str:
    """Render body nicely: try UTF-8 text; if JSON, pretty-print; else base64 as last resort."""
    try:
        chunks = b''.join(b for b in sb_message.body)
    except Exception as e:
        return f"<error reading body: {e}>"

    # Try text
    try:
        s = chunks.decode('utf-8')
        # Try JSON within the text
        try:
            obj = json.loads(s)
            return json.dumps(obj, indent=2, ensure_ascii=False)
        except Exception:
            return s
    except Exception:
        # Not text → return base64 with size hint
        return "base64:" + base64.b64encode(chunks).decode('ascii')


def print_message(prefix: str, m, idx: int):
    props = {
        "sequence_number": getattr(m, "sequence_number", None),
        "enqueued_time_utc": getattr(m, "enqueued_time_utc", None),
        "expires_at_utc": getattr(m, "expires_at_utc", None),
        "locked_until_utc": getattr(m, "locked_until_utc", None),
        "delivery_count": getattr(m, "delivery_count", None),
        "content_type": getattr(m, "content_type", None),
        "subject/label": getattr(m, "subject", None) or getattr(m, "label", None),
        "message_id": getattr(m, "message_id", None),
        "correlation_id": getattr(m, "correlation_id", None),
        "to": getattr(m, "to", None),
        "reply_to": getattr(m, "reply_to", None),
        "session_id": getattr(m, "session_id", None),
        "partition_key": getattr(m, "partition_key", None),
        "dead_letter_reason": getattr(m, "dead_letter_reason", None),
        "dead_letter_error_description": getattr(m, "dead_letter_error_description", None),
        "application_properties": getattr(m, "application_properties", None),
    }
    # Make JSON-safe
    props_json = to_jsonable(props)

    print(f"\n=== {prefix} MESSAGE #{idx} ===")
    print(json.dumps({k: v for k, v in props_json.items() if v is not None}, indent=2, ensure_ascii=False))
    print("-- body --")
    print(safe_body_to_str(m))


def peek_from_queue(client: ServiceBusClient, queue_name: str, max_messages: int, sub_queue: Optional[ServiceBusSubQueue] = None) -> int:
    """Peek and print up to max_messages from (sub_)queue. Returns count."""
    kwargs = dict(queue_name=queue_name)
    if sub_queue:
        kwargs['sub_queue'] = sub_queue
    count = 0
    with client.get_queue_receiver(**kwargs) as receiver:
        messages = receiver.peek_messages(max_message_count=max_messages)
        for i, m in enumerate(messages, 1):
            tag = "DLQ" if sub_queue == ServiceBusSubQueue.DEAD_LETTER else "ACTIVE"
            print_message(tag, m, i)
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description="Peek Azure Service Bus queue and DLQ.")
    parser.add_argument("connection_string", help="Service Bus connection string (can include EntityPath)")
    parser.add_argument("--queue", help="Queue name (if not present in connection string)")
    parser.add_argument("--max", type=int, default=50, help="Max messages to peek per queue (default: 50)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--active-only", action="store_true", help="Only peek active queue")
    group.add_argument("--dlq-only", action="store_true", help="Only peek dead-letter queue")
    args = parser.parse_args()

    try:
        namespace_cs, entity = parse_connection_string(args.connection_string)
    except Exception as e:
        print(f"[error] Invalid connection string: {e}", file=sys.stderr)
        sys.exit(2)

    queue_name = args.queue or entity
    if not queue_name:
        print("[error] No queue name provided. Pass --queue or include EntityPath in the connection string.", file=sys.stderr)
        sys.exit(2)

    try:
        with ServiceBusClient.from_connection_string(namespace_cs) as client:
            total = 0
            if not args.dlq_only:
                print(f"\n>>> Peeking ACTIVE queue: {queue_name} (up to {args.max})")
                c = peek_from_queue(client, queue_name, args.max, sub_queue=None)
                if c == 0:
                    print("[info] No active messages available to peek.")
                total += c

            if not args.active_only:
                print(f"\n>>> Peeking DEAD-LETTER queue: {queue_name} (up to {args.max})")
                c = peek_from_queue(client, queue_name, args.max, sub_queue=ServiceBusSubQueue.DEAD_LETTER)
                if c == 0:
                    print("[info] No dead-letter messages available to peek.")
                total += c

            print(f"\nDone. Total messages peeked: {total}")
    except KeyboardInterrupt:
        print("Interrupted.")
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
