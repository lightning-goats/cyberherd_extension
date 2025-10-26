#!/usr/bin/env python3
"""Stand-alone payment diagnostic for CyberHerd zap recovery.

Usage: run locally or on prod host, pass one or more payment JSON files and the herd_wallet and tracked_note id(s).

Example:
  python tools/payment_diag.py --herd-wallet ab00773393ed4e5eb3b41bf8c4f87cc8 --tracked-note f901... payment1.json payment2.json

This script performs the same checks as ZapMonitor.diagnose_missed_payment_zaps but is read-only and requires no app context.
"""

import argparse
import json
import sys
from datetime import datetime, timezone


def load_payment(path):
    # Support reading from stdin when path is '-'
    if path == '-':
        try:
            return json.load(sys.stdin)
        except Exception as e:
            raise
    with open(path, 'r') as f:
        return json.load(f)


def diag_payment(payment, herd_wallet, tracked_notes, tracked_tags=None, require_author_match=False):
    entry = {
        'wallet_id': payment.get('wallet_id'),
        'checking_id': payment.get('checking_id'),
        'amount': payment.get('amount'),
        'processed': False,
        'reason': None,
        'created_member': None,
    }

    try:
        # Accept either boolean 'success' or string 'status' == 'success'
        payment_success = bool(payment.get('success', False))
        if not payment_success:
            if isinstance(payment.get('status'), str) and payment.get('status').lower() == 'success':
                payment_success = True

        if not payment_success:
            entry['reason'] = 'payment not successful'
            return entry

        if not herd_wallet or payment.get('wallet_id') != herd_wallet:
            entry['reason'] = 'herd_wallet mismatch or missing'
            return entry

        extras = payment.get('extra') or {}
        zap_data_str = None
        if extras.get('comment'):
            zap_data_str = extras.get('comment')
        elif extras.get('nostr'):
            zap_data_str = extras.get('nostr')
        elif extras.get('zap'):
            zap_data_str = extras.get('zap')
        elif isinstance(extras.get('description'), str) and 'zap' in extras.get('description').lower():
            desc = extras.get('description')
            if desc.strip().startswith('{') and desc.strip().endswith('}'):
                zap_data_str = desc

        if not zap_data_str:
            entry['reason'] = 'no zap JSON in payment extras'
            return entry

        try:
            zap_req = json.loads(zap_data_str)
        except Exception:
            entry['reason'] = 'zap JSON parse error'
            return entry

        is_legacy = isinstance(zap_req, dict) and 'kind' not in zap_req and 'pubkey' in zap_req and 'e' in zap_req
        if not (isinstance(zap_req, dict) and (zap_req.get('kind') in (9734, 9735) or is_legacy)):
            entry['reason'] = 'not a recognized zap format'
            return entry

        if is_legacy:
            note_id = zap_req.get('e')
            zapper = zap_req.get('pubkey')
        else:
            tags = zap_req.get('tags', []) or []
            tag_map = {}
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2:
                    tag_map.setdefault(tag[0], []).append(tag[1])
            note_id = tag_map.get('e', [None])[0]
            zapper = None
            if 'description' in tag_map:
                for desc in tag_map['description']:
                    try:
                        desc_obj = json.loads(desc)
                        if isinstance(desc_obj, dict) and 'pubkey' in desc_obj:
                            zapper = desc_obj.get('pubkey')
                            break
                    except Exception:
                        pass
            if not zapper:
                zapper = zap_req.get('pubkey')

        if not note_id:
            entry['reason'] = 'no note id found in zap json'
            return entry

        # Check tag-based tracking: if tracked_notes provided, match those; else, cannot verify tags/author without relays
        if tracked_notes:
            if note_id not in tracked_notes:
                entry['reason'] = 'note id not in tracked notes for this settings'
                return entry
        else:
            # If no tracked_notes, we expected tracked_tags; but we can't query relays here
            entry['reason'] = 'no tracked_note provided for local diagnostic'
            return entry

        if not zapper:
            entry['reason'] = 'zapper pubkey missing'
            return entry

        # Synthetic id check omitted in offline script
        entry['processed'] = True
        entry['reason'] = 'would be processed'
        entry['created_member'] = zapper
        return entry
    except Exception as e:
        entry['reason'] = f'diagnostic error: {e}'
        return entry


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--herd-wallet', required=True)
    p.add_argument('--tracked-note', action='append', default=[], help='Tracked note id (can pass multiple)')
    p.add_argument('payments', nargs='+', help='Paths to payment JSON files')
    args = p.parse_args()

    results = []
    for path in args.payments:
        try:
            pay = load_payment(path)
        except FileNotFoundError:
            print(f"Error: file not found: {path}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Error loading {path}: {e}", file=sys.stderr)
            continue

        # If the file contains a JSON array of payments, process each element.
        if isinstance(pay, list):
            for item in pay:
                r = diag_payment(item, args.herd_wallet, args.tracked_note)
                results.append(r)
        else:
            r = diag_payment(pay, args.herd_wallet, args.tracked_note)
            results.append(r)

    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
