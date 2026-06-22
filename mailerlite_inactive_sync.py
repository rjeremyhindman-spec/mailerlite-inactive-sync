#!/usr/bin/env python3
"""
Keeps purchase-based "inactive" trigger GROUPS in sync in MailerLite.

MailerLite can dynamically match "purchased within the last N days" (the
"is in last interval" filter) but has no auto-updating "more than N days ago."
So we let MailerLite compute the recent-purchaser SEGMENTS and this job does the
subtraction it can't, then keeps the trigger GROUPS in sync by adding/removing
only the delta (so each subscriber enters the day they cross a threshold and the
"joins group" automation fires once).

Bands:
    Inactive 365 (purchased 365+ days ago)    = (ever)  minus (<=365)
    Inactive 180 (purchased 180-365 days ago) = (<=365) minus (<=180)
    Inactive 90  (purchased 90-180 days ago)  = (<=180) minus (<=90)

ROLLOUT: start with just the 365 tier, add the others later.
  - Set ACTIVE_TIERS = ["180"] now.
  - Each tier you switch on needs ONE new segment + its group:
        365 needs:  "ever purchased"  and  "<=365"
        180 adds:   "<=180"           (reuses "<=365")
        90  adds:   "<=90"            (reuses "<=180")
  Only the segments/groups for ACTIVE_TIERS have to exist; the rest are ignored.
"""

import os
import sys
import time
import requests

API = "https://connect.mailerlite.com/api"
TOKEN = os.environ.get("MAILERLITE_API_TOKEN")
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
MIN_INTERVAL = 0.6  # seconds between API calls (stays under MailerLite's 120/min)

# ====== TURN TIERS ON HERE. Start with just 365; add "180", then "90", later. ======
ACTIVE_TIERS = ["180"]            # e.g. later: ["365", "180"]  then  ["365", "180", "90"]

# Each tier = minuend segment minus subtrahend segment -> group.
TIER_SPECS = {
    "365": {"minuend": "ever",  "subtrahend": "le365", "group": "g365"},
    "180": {"minuend": "le365", "subtrahend": "le180", "group": "g180"},
    "90":  {"minuend": "le180", "subtrahend": "le90",  "group": "g90"},
}

# Name everything EXACTLY as you create it in MailerLite (case-sensitive).
BRANDS = [
    {
        "label": "Peekaboo",
        "segments": {
            "le90":  "[auto] PB purchased <=90",
            "le180": "[auto] PB purchased <=180",
            "le365": "[auto] PB purchased <=365",
            "ever":  "[auto] PB ever purchased",
        },
        "groups": {
            "g90":  "Inactive 90 Days Peekaboo (auto)",
            "g180": "Inactive 180 Days Peekaboo (auto)",
            "g365": "Inactive 365 Days Peekaboo (auto)",
        },
    },
    {
        "label": "KnitFabric",
        "segments": {
            "le90":  "[auto] KF purchased <=90",
            "le180": "[auto] KF purchased <=180",
            "le365": "[auto] KF purchased <=365",
            "ever":  "[auto] KF ever purchased",
        },
        "groups": {
            "g90":  "Inactive 90 Days KnitFabric (auto)",
            "g180": "Inactive 180 Days KnitFabric (auto)",
            "g365": "Inactive 365 Days KnitFabric (auto)",
        },
    },
]

if not TOKEN:
    sys.exit("Set MAILERLITE_API_TOKEN")

S = requests.Session()
S.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
})

_last_call = [0.0]


def _throttle():
    dt = time.time() - _last_call[0]
    if dt < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - dt)
    _last_call[0] = time.time()


def _req(method, url):
    for attempt in range(6):
        _throttle()
        r = S.request(method, url)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", "5")) + 1)
            continue
        if r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        return r
    return r


def list_all(path):
    """Page-based listing for /segments and /groups."""
    items, page = [], 1
    while True:
        r = _req("GET", f"{API}{path}?page={page}&limit=100")
        r.raise_for_status()
        j = r.json()
        items.extend(j.get("data", []))
        meta = j.get("meta") or {}
        if page >= meta.get("last_page", page):
            break
        page += 1
    return items


def cursor_member_ids(path, active_only):
    """Cursor pagination for /segments/{id}/subscribers and /groups/{id}/subscribers."""
    url = f"{API}{path}?limit=100"
    ids = set()
    while url:
        r = _req("GET", url)
        r.raise_for_status()
        j = r.json()
        for s in j.get("data", []):
            if (not active_only) or s.get("status") == "active":
                ids.add(s["id"])
        url = (j.get("links") or {}).get("next")
    return ids


def assign(sub_id, grp_id):
    if DRY_RUN:
        return
    r = _req("POST", f"{API}/subscribers/{sub_id}/groups/{grp_id}")
    if r.status_code not in (200, 201):
        print(f"    ! assign {sub_id} -> {grp_id}: {r.status_code} {r.text[:160]}")


def unassign(sub_id, grp_id):
    if DRY_RUN:
        return
    r = _req("DELETE", f"{API}/subscribers/{sub_id}/groups/{grp_id}")
    if r.status_code not in (200, 204):
        print(f"    ! unassign {sub_id} -> {grp_id}: {r.status_code} {r.text[:160]}")


def sync_group(label, grp_id, target_ids):
    current = cursor_member_ids(f"/groups/{grp_id}/subscribers", active_only=False)
    to_add = target_ids - current
    to_remove = current - target_ids
    print(f"  {label}: target={len(target_ids)} current={len(current)} "
          f"add={len(to_add)} remove={len(to_remove)}")
    for sid in to_add:
        assign(sid, grp_id)
    for sid in to_remove:
        unassign(sid, grp_id)


def main():
    segs = {s["name"]: s["id"] for s in list_all("/segments")}
    grps = {g["name"]: g["id"] for g in list_all("/groups")}

    def seg_id(brand, key):
        name = brand["segments"][key]
        if name not in segs:
            sys.exit(f"Missing segment (check exact name): {name!r}")
        return segs[name]

    def grp_id(brand, key):
        name = brand["groups"][key]
        if name not in grps:
            sys.exit(f"Missing group (check exact name): {name!r}")
        return grps[name]

    print(f"== MailerLite inactive sync == tiers={ACTIVE_TIERS}"
          f"{'  [DRY RUN]' if DRY_RUN else ''}")
    member_cache = {}

    def members(brand, key):
        ck = (brand["label"], key)
        if ck not in member_cache:
            member_cache[ck] = cursor_member_ids(f"/segments/{seg_id(brand, key)}/subscribers", True)
        return member_cache[ck]

    for b in BRANDS:
        print(f"-- {b['label']} --")
        member_cache.clear()
        for tier in ACTIVE_TIERS:
            spec = TIER_SPECS[tier]
            band = members(b, spec["minuend"]) - members(b, spec["subtrahend"])
            sync_group(f"Inactive {tier}", grp_id(b, spec["group"]), band)

    print("Done." + ("  No changes written (DRY_RUN)." if DRY_RUN else ""))


if __name__ == "__main__":
    main()
