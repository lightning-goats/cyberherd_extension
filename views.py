import os
import logging

from fastapi import APIRouter, Depends, Request
from starlette.responses import HTMLResponse, FileResponse

from lnbits.core.models import User
from lnbits.decorators import check_user_exists
from lnbits.helpers import template_renderer

router = APIRouter()

logger = logging.getLogger(__name__)


def cyberherd_renderer():
    return template_renderer(["cyberherd/templates"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, user: User = Depends(check_user_exists)):
    """Serve the CyberHerd admin UI using the standard LNbits base layout.

    This mirrors patterns from other extensions (e.g., tpos, splitpayments):
    - Use template_renderer so macros/base assets load.
    - Pass serialized user for wallet list and auth keys (window_vars macro).
    """
    try:
        return cyberherd_renderer().TemplateResponse(
            "cyberherd/index.html", {"request": request, "user": user.json()}
        )
    except Exception as e:  # Fallback minimal error page (should rarely trigger)
        logger.error(f"Cyberherd index render failed: {e}")
        return HTMLResponse(
            """
            <div style='padding:1rem;font-family:monospace;'>
              <h3>CyberHerd UI Error</h3>
              <p>Failed to render template. Check server logs.</p>
            </div>
            """,
            status_code=500,
        )
