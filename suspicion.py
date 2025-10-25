def start_suspect(table, nid, now, suspectHoldTime):
    m = table.get(nid)
    if not m or m.status in ("failed", "left"):
        return False
    if m.status != "suspect":
        m.status = "suspect"
        m.suspect_deadline = now + suspectHoldTime
        return True
    return False

def clear_suspect(table, nid):
    m = table.get(nid)
    if m and m.status == "suspect":
        m.status = "alive"
        m.suspect_deadline = None

def delete_member(table, nid, reason="timeout"):
    m = table.get(nid)
    if m:
        m.status = "failed"
        m.suspect_deadline = None