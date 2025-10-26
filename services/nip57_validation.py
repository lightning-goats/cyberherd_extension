"""NIP-57 Zap Receipt Validation.

This module provides validation for NIP-57 zap receipts according to the specification:
https://github.com/nostr-protocol/nips/blob/master/57.md

NIP-57 requires zap receipts (kind 9735) to contain:
- bolt11 tag: Lightning invoice that was paid
- description tag: Original zap request (kind 9734)
- preimage tag: Payment preimage (optional but recommended)
- p tag: Recipient pubkey
- e tag: Zapped event (optional)

This validation ensures only properly formatted zaps trigger headbutt processing.
"""

from __future__ import annotations

from typing import Any
from loguru import logger


def validate_nip57_zap_receipt(event: dict[str, Any], *, require_preimage: bool = False) -> tuple[bool, str]:
    """Validate a NIP-57 zap receipt event.
    
    Per NIP-57, zap receipts (kind 9735) must contain:
    - bolt11 tag with Lightning invoice
    - description tag with zap request (kind 9734)
    - p tag with recipient pubkey
    - Optional: e tag with zapped event
    - Optional: preimage tag with payment proof
    
    Args:
        event: Nostr event dict to validate
        require_preimage: If True, reject zaps without preimage tag
    
    Returns:
        Tuple of (is_valid, error_message)
        - (True, "") if valid
        - (False, "reason") if invalid
    """
    # Check event kind
    if not isinstance(event, dict):
        return False, "Event is not a dictionary"
    
    kind = event.get('kind')
    if kind != 9735:
        return False, f"Invalid kind: {kind} (expected 9735 for zap receipt)"
    
    # Check tags exist
    tags = event.get('tags', [])
    if not isinstance(tags, list):
        return False, "Tags field is not a list"
    
    # Convert tags to dict for easier lookup
    tags_dict: dict[str, list[str]] = {}
    for tag in tags:
        if isinstance(tag, list) and len(tag) >= 2:
            tag_name = tag[0]
            tag_value = tag[1]
            if tag_name not in tags_dict:
                tags_dict[tag_name] = []
            tags_dict[tag_name].append(tag_value)
    
    # Validate required tags
    required_tags = ['bolt11', 'description', 'p']
    missing_tags = []
    
    for required_tag in required_tags:
        if required_tag not in tags_dict:
            missing_tags.append(required_tag)
    
    if missing_tags:
        return False, f"Missing required tags: {', '.join(missing_tags)}"
    
    # Validate bolt11 tag (Lightning invoice)
    bolt11_values = tags_dict.get('bolt11', [])
    if not bolt11_values or not bolt11_values[0]:
        return False, "bolt11 tag is empty"
    
    bolt11 = bolt11_values[0]
    # Basic bolt11 format check (should start with ln)
    if not isinstance(bolt11, str) or not bolt11.lower().startswith('ln'):
        return False, f"Invalid bolt11 format: {bolt11[:20]}..."
    
    # Validate description tag (zap request)
    description_values = tags_dict.get('description', [])
    if not description_values or not description_values[0]:
        return False, "description tag is empty"
    
    description = description_values[0]
    # Description should be JSON containing kind 9734 zap request
    if not isinstance(description, str):
        return False, "description tag is not a string"
    
    # Try to parse description as JSON
    import json
    try:
        zap_request = json.loads(description)
    except json.JSONDecodeError as e:
        return False, f"description tag is not valid JSON: {e}"
    
    # Validate zap request structure
    if not isinstance(zap_request, dict):
        return False, "Zap request in description is not a dict"
    
    if zap_request.get('kind') != 9734:
        return False, f"Invalid zap request kind: {zap_request.get('kind')} (expected 9734)"
    
    # Validate p tag (recipient pubkey)
    p_values = tags_dict.get('p', [])
    if not p_values or not p_values[0]:
        return False, "p tag (recipient pubkey) is empty"
    
    recipient_pubkey = p_values[0]
    if not isinstance(recipient_pubkey, str) or len(recipient_pubkey) != 64:
        return False, f"Invalid recipient pubkey length: {len(recipient_pubkey) if isinstance(recipient_pubkey, str) else 'not a string'} (expected 64 hex chars)"
    
    # Validate preimage if required or present
    preimage_values = tags_dict.get('preimage', [])
    if require_preimage:
        if not preimage_values or not preimage_values[0]:
            return False, "preimage tag is required but missing"
        
        preimage = preimage_values[0]
        if not isinstance(preimage, str) or len(preimage) != 64:
            return False, f"Invalid preimage length: {len(preimage) if isinstance(preimage, str) else 'not a string'} (expected 64 hex chars)"
    elif preimage_values and preimage_values[0]:
        # If preimage is present but not required, still validate its format
        preimage = preimage_values[0]
        if not isinstance(preimage, str) or len(preimage) != 64:
            return False, f"Invalid preimage format: {len(preimage) if isinstance(preimage, str) else 'not a string'} chars (expected 64 hex chars)"
    
    # All validations passed
    return True, ""


def validate_nip57_payment_zap(payment_data: dict[str, Any], *, require_bolt11: bool = True) -> tuple[bool, str]:
    """Validate zap data extracted from invoice payment.
    
    This validates zap data that comes from invoice webhooks rather than
    Nostr relay events. The data structure is slightly different.
    
    Args:
        payment_data: Dict with 'nostr' field containing zap receipt data
        require_bolt11: If True, require bolt11 field in tags
    
    Returns:
        Tuple of (is_valid, error_message)
        - (True, "") if valid
        - (False, "reason") if invalid
    """
    if not isinstance(payment_data, dict):
        return False, "Payment data is not a dictionary"
    
    # Extract nostr data
    nostr_data = payment_data.get('nostr')
    if not nostr_data:
        return False, "Missing 'nostr' field in payment data"
    
    # Parse if string
    if isinstance(nostr_data, str):
        import json
        try:
            nostr_data = json.loads(nostr_data)
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON in nostr field: {e}"
    
    if not isinstance(nostr_data, dict):
        return False, "nostr field is not a dictionary"
    
    # Check kind (can be 9735 for receipt or 9734 for request)
    kind = nostr_data.get('kind')
    if kind not in [9734, 9735]:
        return False, f"Invalid kind: {kind} (expected 9734 or 9735)"
    
    # Validate tags
    tags = nostr_data.get('tags', [])
    if not isinstance(tags, list):
        return False, "Tags field is not a list"
    
    # Convert tags to dict
    tags_dict: dict[str, list[str]] = {}
    for tag in tags:
        if isinstance(tag, list) and len(tag) >= 2:
            tag_name = tag[0]
            tag_value = tag[1]
            if tag_name not in tags_dict:
                tags_dict[tag_name] = []
            tags_dict[tag_name].append(tag_value)
    
    # For payment-derived zaps, require at least p tag (recipient)
    if 'p' not in tags_dict and 'e' not in tags_dict:
        return False, "Missing both p tag (recipient) and e tag (zapped event)"
    
    # Validate bolt11 if required
    # For kind 9734 (zap request), bolt11 is not required
    # For kind 9735 (zap receipt), bolt11 is required
    if kind == 9734:
        # Zap request doesn't have bolt11 yet
        # Only validate if present
        if 'bolt11' in tags_dict:
            bolt11_values = tags_dict.get('bolt11', [])
            if bolt11_values and bolt11_values[0]:
                bolt11 = bolt11_values[0]
                if not isinstance(bolt11, str) or not bolt11.lower().startswith('ln'):
                    return False, f"Invalid bolt11 format: {bolt11[:20] if isinstance(bolt11, str) else 'not a string'}..."
    elif kind == 9735:
        # Zap receipt requires bolt11
        if require_bolt11:
            if 'bolt11' not in tags_dict:
                return False, "Missing bolt11 tag (required for zap receipts)"
            
            bolt11_values = tags_dict.get('bolt11', [])
            if not bolt11_values or not bolt11_values[0]:
                return False, "bolt11 tag is empty"
            
            bolt11 = bolt11_values[0]
            if not isinstance(bolt11, str) or not bolt11.lower().startswith('ln'):
                return False, f"Invalid bolt11 format: {bolt11[:20] if isinstance(bolt11, str) else 'not a string'}..."

            return False, f"Invalid bolt11 format: {bolt11[:20] if isinstance(bolt11, str) else 'not a string'}..."
    
    # Validate description if present (should contain kind 9734)
    if 'description' in tags_dict:
        description_values = tags_dict.get('description', [])
        if description_values and description_values[0]:
            description = description_values[0]
            
            import json
            try:
                zap_request = json.loads(description)
                if isinstance(zap_request, dict):
                    if zap_request.get('kind') != 9734:
                        return False, f"Invalid zap request kind in description: {zap_request.get('kind')}"
            except json.JSONDecodeError:
                # Description might not be JSON for payment-derived zaps
                pass
    
    # All validations passed
    return True, ""


def extract_zap_details(event: dict[str, Any]) -> dict[str, Any]:
    """Extract zap details from a validated zap receipt.
    
    Args:
        event: Validated NIP-57 zap receipt event
    
    Returns:
        Dict with extracted details:
        - bolt11: Lightning invoice
        - recipient_pubkey: Recipient pubkey from p tag
        - zapped_event_id: Zapped event from e tag (if present)
        - preimage: Payment preimage (if present)
        - description: Zap request JSON
        - amount_sats: Amount from bolt11 or zap request
    """
    tags = event.get('tags', [])
    
    details: dict[str, Any] = {
        'bolt11': None,
        'recipient_pubkey': None,
        'zapped_event_id': None,
        'preimage': None,
        'description': None,
        'amount_sats': 0,
    }
    
    for tag in tags:
        if not isinstance(tag, list) or len(tag) < 2:
            continue
        
        tag_name = tag[0]
        tag_value = tag[1]
        
        if tag_name == 'bolt11':
            details['bolt11'] = tag_value
        elif tag_name == 'p':
            details['recipient_pubkey'] = tag_value
        elif tag_name == 'e':
            details['zapped_event_id'] = tag_value
        elif tag_name == 'preimage':
            details['preimage'] = tag_value
        elif tag_name == 'description':
            details['description'] = tag_value
            
            # Try to extract amount from zap request
            import json
            try:
                zap_request = json.loads(tag_value)
                if isinstance(zap_request, dict):
                    # Look for amount in zap request tags
                    zap_tags = zap_request.get('tags', [])
                    for zap_tag in zap_tags:
                        if isinstance(zap_tag, list) and len(zap_tag) >= 2 and zap_tag[0] == 'amount':
                            try:
                                amount_msats = int(zap_tag[1])
                                details['amount_sats'] = amount_msats // 1000
                            except (ValueError, TypeError):
                                pass
            except json.JSONDecodeError:
                pass
    
    return details
