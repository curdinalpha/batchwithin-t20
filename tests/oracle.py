"""Independent reference model. Integer time only, stepped one tick at a
time (no event queue, no deadline catch-up, no state enum). Shares no code
with batchwithin/. Same-tick order: deadline, requests, offers, completion."""


def oracle(K, D, R, arrivals, requests, complete_at=None):
    n = len(arrivals)
    horizon = max([0] + list(arrivals) + list(requests) +
                  ([complete_at] if complete_at is not None else [])) + (n + 2) * (D + 1) + 2
    active, t0, closed, ready, out, adm = [], None, None, [], [], []
    demand, nxt, completed, completed_at = 0, 0, False, None
    req = sorted(requests)
    ri = 0

    def deliver(t):
        nonlocal demand, closed
        while demand and ready:
            items, w0, st, why = ready.pop(0)
            demand -= 1
            out.append((tuple(items), w0, st, max(st, t), why))
            if closed is not None:
                ready.append(closed)
                closed = None

    def seal(t, why):
        nonlocal active, t0, closed
        c = (active, t0, t, why)
        active, t0 = [], None
        if len(ready) < R:
            ready.append(c)
            deliver(t)
        else:
            closed = c

    for t in range(horizon + 1):
        if active and closed is None and t == t0 + D:
            seal(t, "time")
        while ri < len(req) and req[ri] == t:
            demand += 1
            deliver(t)
            ri += 1
        while nxt < n and arrivals[nxt] <= t and closed is None and not completed:
            if not active:
                t0 = t
            active.append(nxt)
            adm.append((t, nxt))
            nxt += 1
            if len(active) == K:
                seal(t, "size")
        if complete_at is not None and not completed and nxt == n and t >= complete_at:
            completed, completed_at = True, t
            if active and closed is None:
                seal(t, "complete")
    return out, adm, completed_at
