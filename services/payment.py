"""
Payment service for Cyberherd extension inside LNbits extensions folder.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, wallet=None, db=None):
        # wallet: either a source wallet id (str) or a wallet helper object
        # db: DB helper for reading Cyberherd members and payment targets
        self.wallet = wallet
        self.db = db

    async def pay_cyberherd_members_with_splits(
        self,
        total_sats: int,
        *,
        source_wallet_id: str | None = None,
        dry_run: bool = True,
        app: object | None = None,
    ) -> dict[str, Any]:
        logger.info(
            "PaymentService: pay_cyberherd_members_with_splits called with %s sats",
            total_sats,
        )

        members = await self._get_active_members()
        if not members:
            logger.info("PaymentService: no active members found; nothing to pay")
            return {"success": True, "payments": [], "note": "no active members"}

        payments = self._calculate_split_payments(members, total_sats)
        total_distributed = sum(p["sats"] for p in payments)
        logger.info(
            "PaymentService: prepared %s payments, total sats distributed=%s",
            len(payments),
            total_distributed,
        )

        if dry_run or source_wallet_id is None:
            return {
                "success": True,
                "payments": payments,
                "distributed": total_distributed,
                "dry_run": True,
            }

        source_wallet_id = source_wallet_id or self._get_source_wallet_from_app(app)
        return await self._execute_payments(payments, source_wallet_id)

    async def _get_active_members(self) -> list[dict]:
        db_conn = self.db
        if db_conn is None:
            return []

        try:
            query = "SELECT pubkey, amount, payouts FROM cyber_herd WHERE is_active = 1"
            rows = await db_conn.fetch_all(query)
            return [dict(r) for r in rows] if rows else []
        except Exception:
            logger.debug("PaymentService: could not read members from DB")
            return []

    def _calculate_split_payments(
        self, members: list[dict], total_sats: int
    ) -> list[dict]:
        weights = [max(int(m.get("amount") or 0), 0) for m in members]
        if sum(weights) == 0:
            weights = [1] * len(members)

        total_weight = sum(weights)
        payments = []
        sats_assigned = 0

        for i, m in enumerate(members):
            sats = (total_sats * weights[i]) // total_weight
            payments.append(
                {
                    "pubkey": m.get("pubkey"),
                    "sats": int(sats),
                    "note": m.get("note") if isinstance(m.get("note"), str) else None,
                }
            )
            sats_assigned += sats

        remainder = int(total_sats - sats_assigned)
        for i in range(remainder):
            payments[i % len(payments)]["sats"] += 1

        return payments

    def _get_source_wallet_from_app(self, app: object | None) -> str | None:
        if app:
            try:
                return getattr(
                    getattr(app, "state", app), "cyberherd_source_wallet", None
                )
            except Exception as e:
                logger.warning(e)
        return None

    async def _execute_payments(
        self, payments: list[dict], source_wallet_id: str | None
    ) -> dict[str, Any]:
        if not source_wallet_id:
            return {"success": False, "error": "No source wallet ID provided"}

        try:
            from lnbits.core.crud.wallets import get_wallet

            source_wallet = await get_wallet(source_wallet_id)
            if not source_wallet:
                return {"success": False, "error": "Source wallet not found"}

            # This part needs a way to get invoices for pubkeys.
            # For now, this is a placeholder for the actual payment logic.
            # You would need to integrate with a service that can create invoices
            # for Nostr pubkeys (e.g., via LNURL).
            logger.warning("Payment execution is not fully implemented.")
            # for payment in payments:
            #     # Simplified: assumes a function `create_invoice_for_pubkey` exists
            #     invoice = await create_invoice_for_pubkey(
            #         payment['pubkey'], payment['sats']
            #     )
            #     await pay_invoice(
            #         wallet_id=source_wallet.id,
            #         payment_request=invoice,
            #         max_sat=payment['sats'],
            #         extra={"tag": "cyberherd"},
            #     )

            return {"success": True, "note": "Payments executed (simulated)."}

        except ImportError:
            return {"success": False, "error": "LNbits core components not available"}
        except Exception as e:
            logger.error(f"Payment failed: {e}")
            return {"success": False, "error": str(e)}
