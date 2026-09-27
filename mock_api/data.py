"""Ground-truth data behind the mock API.

The server serves this data through an unreliable API; tests compare what the
pipeline lands against this ground truth. `advance()` simulates time passing
between runs: new bookings arrive and older appointments change status, which
is exactly what makes incremental loading from a date-based API hard.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

SERVICES = ["IV Hydration", "Cryotherapy", "Infrared Sauna", "Body Scan", "Weight Consult", "NAD+ Drip"]
PRICES = {"IV Hydration": 179.0, "Cryotherapy": 69.0, "Infrared Sauna": 49.0,
          "Body Scan": 99.0, "Weight Consult": 149.0, "NAD+ Drip": 449.0}


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat() + "Z"


@dataclass
class Dataset:
    start: date
    end: date
    seed: int = 7
    n_centers: int = 12
    n_guests: int = 3000
    appts_per_center_day: float = 4.0
    horizon_days: int = 14          # bookings exist this far into the future
    drift_from: date | None = None  # appointments on/after this date carry a new field
    centers: list[dict] = field(default_factory=list)
    guests: dict[str, dict] = field(default_factory=dict)
    appointments: dict[str, dict] = field(default_factory=dict)
    invoices: dict[str, dict] = field(default_factory=dict)
    clock: datetime = field(init=False)

    def __post_init__(self):
        self.rng = random.Random(self.seed)
        self.clock = datetime.combine(self.end, datetime.min.time()) + timedelta(hours=23)
        self._next = {"g": 0, "a": 0, "i": 0}
        self.centers = [
            {"id": f"CTR-{i:03d}", "name": f"Lumen Wellness #{i}", "franchisee_id": f"FR-{(i % 5) + 1:02d}",
             "timezone": self.rng.choice(["America/Chicago", "America/Denver", "America/Los_Angeles"]),
             "opened_on": str(self.start - timedelta(days=self.rng.randint(30, 700)))}
            for i in range(1, self.n_centers + 1)
        ]
        for _ in range(self.n_guests):
            self._new_guest(self._random_time(self.start - timedelta(days=200), self.end))
        day = self.start
        while day <= self.end + timedelta(days=self.horizon_days):
            for c in self.centers:
                for _ in range(self._poisson(self.appts_per_center_day)):
                    booked = min(self._random_time(day - timedelta(days=10), day), self.clock)
                    self._new_appointment(c["id"], day, booked_at=booked)
            day += timedelta(days=1)

    # --- helpers -------------------------------------------------------------
    def _poisson(self, lam: float) -> int:
        n, p, L = 0, 1.0, 2.718281828 ** -lam
        while True:
            p *= self.rng.random()
            if p < L:
                return n
            n += 1

    def _random_time(self, a: date, b: date) -> datetime:
        span = max(1, (b - a).days)
        return datetime.combine(a, datetime.min.time()) + timedelta(
            days=self.rng.randrange(span), minutes=self.rng.randrange(8 * 60, 20 * 60))

    def _id(self, kind: str, prefix: str) -> str:
        self._next[kind] += 1
        return f"{prefix}-{self._next[kind]:07d}"

    def _new_guest(self, created: datetime) -> dict:
        gid = self._id("g", "GST")
        g = {"id": gid, "center_id": self.rng.choice(self.centers)["id"], "first_name": "Guest",
             "last_name": gid[-4:], "email": f"guest{gid[-5:]}@example.com",
             "created_at": iso(created), "updated_at": iso(created)}
        self.guests[gid] = g
        return g

    def _new_appointment(self, center_id: str, day: date, booked_at: datetime) -> dict:
        aid = self._id("a", "APT")
        service = self.rng.choice(SERVICES)
        starts = datetime.combine(day, datetime.min.time()) + timedelta(minutes=self.rng.randrange(8 * 60, 19 * 60, 15))
        a = {"id": aid, "center_id": center_id, "guest_id": self.rng.choice(list(self.guests)),
             "service": service, "start_time": iso(starts), "status": "booked",
             "price": PRICES[service], "updated_at": iso(booked_at)}
        if self.drift_from and day >= self.drift_from:
            a["booking_channel"] = self.rng.choice(["online", "phone", "walk_in"])
        self.appointments[aid] = a
        if starts < self.clock:
            self._resolve(a, when=min(self.clock, starts + timedelta(hours=2)))
        return a

    def _resolve(self, a: dict, when: datetime) -> None:
        """A booked appointment's time has passed: it happened, was cancelled, or was a no-show."""
        a["status"] = self.rng.choices(["completed", "cancelled", "no_show"], [0.85, 0.09, 0.06])[0]
        a["updated_at"] = iso(when)
        if a["status"] == "completed":
            self._invoice(a)

    def _invoice(self, a: dict) -> None:
        iid = self._id("i", "INV")
        self.invoices[iid] = {"id": iid, "appointment_id": a["id"], "center_id": a["center_id"],
                              "invoice_date": a["start_time"][:10], "total": a["price"],
                              "updated_at": a["updated_at"]}

    # --- time passing ----------------------------------------------------------
    def advance(self, days: int = 3) -> dict:
        """Move the clock forward. Past bookings resolve, some completed visits get
        price adjustments, new bookings arrive and new guests sign up."""
        changes = {"resolved": 0, "adjusted": 0, "new_appointments": 0, "new_guests": 0}
        old_horizon = self.end + timedelta(days=self.horizon_days)
        self.end += timedelta(days=days)
        self.clock = datetime.combine(self.end, datetime.min.time()) + timedelta(hours=23)

        for a in list(self.appointments.values()):
            starts = datetime.fromisoformat(a["start_time"].rstrip("Z"))
            if a["status"] == "booked" and starts < self.clock:
                self._resolve(a, when=min(self.clock, starts + timedelta(hours=2)))
                changes["resolved"] += 1

        recent_done = [a for a in self.appointments.values() if a["status"] == "completed"
                       and a["start_time"][:10] >= str(self.end - timedelta(days=10))]
        for a in self.rng.sample(recent_done, k=min(len(recent_done), max(1, len(recent_done) // 30))):
            a["price"] = round(a["price"] * 0.9, 2)  # discount applied after the visit
            a["updated_at"] = iso(self.clock - timedelta(hours=self.rng.randrange(1, 24)))
            inv = next(i for i in self.invoices.values() if i["appointment_id"] == a["id"])
            inv["total"], inv["updated_at"] = a["price"], a["updated_at"]
            changes["adjusted"] += 1

        day = old_horizon + timedelta(days=1)
        while day <= self.end + timedelta(days=self.horizon_days):
            for c in self.centers:
                for _ in range(self._poisson(self.appts_per_center_day)):
                    self._new_appointment(c["id"], day, booked_at=self.clock - timedelta(hours=self.rng.randrange(1, 48)))
                    changes["new_appointments"] += 1
            day += timedelta(days=1)

        for _ in range(max(1, len(self.guests) // 100)):
            self._new_guest(self.clock - timedelta(hours=self.rng.randrange(1, 70)))
            changes["new_guests"] += 1
        return changes
