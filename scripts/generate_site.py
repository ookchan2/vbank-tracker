import json, logging
from pathlib import Path
from datetime import datetime
import database

logger = logging.getLogger(__name__)

def generate_site():
    database.init_db()
    all_promos = database.get_all()
    active  = [p for p in all_promos if p["active"]]
    expired = [p for p in all_promos if not p["active"]]
    banks   = list({p["bank"] for p in all_promos})

    data = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "stats": {
            "total": len(all_promos),
            "active": len(active),
            "expired": len(expired),
            "banks": len(banks),
        },
        "promotions": all_promos,
    }

    docs = Path("docs")
    docs.mkdir(exist_ok=True)
    out = docs / "data.json"
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"✅ docs/data.json generated — {len(all_promos)} total promotions")

if __name__ == "__main__":
    import sys, logging
    logging.basicConfig(level=logging.INFO)
    generate_site()