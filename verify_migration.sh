#!/bin/bash
# Verification script for nostr_helpers migration
# Run this to confirm all code paths use nostr_helpers

echo "=================================="
echo "Nostr Helpers Migration Verification"
echo "=================================="
echo ""

CYBERHERD_DIR="lnbits/extensions/cyberherd"

echo "1. Checking for _query_events_via_manager usage..."
echo "   (Should only appear in documentation files)"
QUERY_MANAGER=$(grep -r "_query_events_via_manager" --include="*.py" "$CYBERHERD_DIR" \
    --exclude="nostr_helpers_migration.py" \
    --exclude="*MIGRATION*.md" \
    --exclude="*INTEGRATION*.md" \
    --exclude="*SUMMARY*.md" | wc -l)

if [ "$QUERY_MANAGER" -eq 0 ]; then
    echo "   ✅ PASS: No _query_events_via_manager calls found"
else
    echo "   ❌ FAIL: Found $QUERY_MANAGER instances"
    grep -r "_query_events_via_manager" --include="*.py" "$CYBERHERD_DIR" \
        --exclude="nostr_helpers_migration.py"
fi
echo ""

echo "2. Checking for direct nostr_client.relay_manager access..."
echo "   (Should only appear in nostr_helpers.py)"
RELAY_MANAGER=$(grep -r "nostr_client\.relay_manager" --include="*.py" "$CYBERHERD_DIR" \
    --exclude="nostr_helpers.py" \
    --exclude="nostr_helpers_migration.py" | wc -l)

if [ "$RELAY_MANAGER" -eq 0 ]; then
    echo "   ✅ PASS: No direct relay_manager access found"
else
    echo "   ❌ FAIL: Found $RELAY_MANAGER instances"
    grep -r "nostr_client\.relay_manager" --include="*.py" "$CYBERHERD_DIR" \
        --exclude="nostr_helpers.py" \
        --exclude="nostr_helpers_migration.py"
fi
echo ""

echo "3. Checking for direct message_pool access..."
echo "   (Should only appear in nostr_helpers.py)"
MESSAGE_POOL=$(grep -r "message_pool\.events\.put\|message_pool\.has_events\|message_pool\.get_event" \
    --include="*.py" "$CYBERHERD_DIR" \
    --exclude="nostr_helpers.py" \
    --exclude="nostr_helpers_migration.py" | wc -l)

if [ "$MESSAGE_POOL" -eq 0 ]; then
    echo "   ✅ PASS: No direct message_pool access found"
else
    echo "   ❌ FAIL: Found $MESSAGE_POOL instances"
    grep -r "message_pool\.events\.put\|message_pool\.has_events\|message_pool\.get_event" \
        --include="*.py" "$CYBERHERD_DIR" \
        --exclude="nostr_helpers.py" \
        --exclude="nostr_helpers_migration.py"
fi
echo ""

echo "4. Checking for nostr_helpers imports..."
echo "   (Should be present in migrated files)"
HELPERS_IMPORTS=$(grep -r "from.*nostr_helpers\|import.*nostr_helpers" \
    --include="*.py" "$CYBERHERD_DIR" \
    --exclude="nostr_helpers.py" \
    --exclude="nostr_helpers_migration.py" | wc -l)

if [ "$HELPERS_IMPORTS" -ge 5 ]; then
    echo "   ✅ PASS: Found $HELPERS_IMPORTS nostr_helpers imports"
else
    echo "   ⚠️  WARNING: Only found $HELPERS_IMPORTS imports (expected at least 5)"
    grep -r "from.*nostr_helpers\|import.*nostr_helpers" \
        --include="*.py" "$CYBERHERD_DIR" \
        --exclude="nostr_helpers.py"
fi
echo ""

echo "5. Listing migrated files using nostr_helpers..."
grep -l "import nostr_helpers\|from.*nostr_helpers" \
    --include="*.py" "$CYBERHERD_DIR" \
    --exclude="nostr_helpers.py" \
    --exclude="nostr_helpers_migration.py" | while read -r file; do
    echo "   ✅ $file"
done
echo ""

echo "6. Checking Python syntax..."
SYNTAX_ERRORS=0
for file in "$CYBERHERD_DIR/services/headbutt.py" \
            "$CYBERHERD_DIR/services/nostr_event_monitor.py" \
            "$CYBERHERD_DIR/views_api.py" \
            "$CYBERHERD_DIR/crud.py" \
            "$CYBERHERD_DIR/__init__.py"; do
    if [ -f "$file" ]; then
        if python3 -m py_compile "$file" 2>/dev/null; then
            echo "   ✅ $(basename "$file") - Valid syntax"
        else
            echo "   ❌ $(basename "$file") - Syntax error"
            SYNTAX_ERRORS=$((SYNTAX_ERRORS + 1))
        fi
    fi
done
echo ""

echo "=================================="
echo "Migration Summary"
echo "=================================="
if [ "$QUERY_MANAGER" -eq 0 ] && [ "$RELAY_MANAGER" -eq 0 ] && [ "$MESSAGE_POOL" -eq 0 ] && [ "$SYNTAX_ERRORS" -eq 0 ]; then
    echo "✅ ALL CHECKS PASSED"
    echo ""
    echo "Migration is complete. All code paths now use nostr_helpers."
    exit 0
else
    echo "❌ SOME CHECKS FAILED"
    echo ""
    echo "Please review the output above and fix any issues."
    exit 1
fi
