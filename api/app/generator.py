import random
from datetime import datetime, timedelta
from typing import List

from app.models import Allowance, AwardInterpretation, Break, Shift


def _datetime_to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _generate_break(shift_start: datetime, expected_length_min: int = 23) -> Break:
    # Breaks start between [2, 3] hours from shift start and last on average 23 minutes.
    break_start = shift_start + timedelta(minutes=random.randint(2 * 60, 3 * 60))
    break_finish = break_start + timedelta(
        minutes=random.gauss(mu=expected_length_min, sigma=5)
    )
    return Break(
        paid=random.random() > 0.5,
        start=_datetime_to_epoch_ms(break_start),
        finish=_datetime_to_epoch_ms(break_finish),
    )


def _generate_allowance() -> Allowance:
    return Allowance(
        value=random.choice([0.5, 0.75, 1.0, 1.5]), cost=random.randint(10, 500) / 10
    )


def _generate_award_interpretation(for_date: datetime) -> AwardInterpretation:
    return AwardInterpretation(
        date=str(for_date.date()),
        units=random.choice([0.5, 0.75, 1.0, 1.5]),
        cost=random.randint(10, 1000) / 10,
    )


def _generate_shift(
    for_date: datetime,
    break_probability: float = 0.7,
    max_allowances: int = 3,
    max_award_interpretations: int = 3,
) -> Shift:
    # Shift can start between [7, 10] am on every 15th minute.
    shift_start = for_date + timedelta(
        minutes=random.choice(range(7 * 60, 10 * 60, 15))
    )

    # Shift can finish between [start + 8, start + 12] hours on every 15th minute.
    shift_finish = shift_start + timedelta(
        minutes=random.choice(range(8 * 60, 12 * 60, 15))
    )

    breaks: List[Break]
    if random.random() < break_probability:
        breaks = [_generate_break(shift_start=shift_start)]
    else:
        breaks = []

    allowances: List[Allowance] = [
        _generate_allowance() for _ in range(random.randint(0, max_allowances))
    ]

    award_interpretations: List[AwardInterpretation] = [
        _generate_award_interpretation(for_date)
        for _ in range(random.randint(0, max_award_interpretations))
    ]

    return Shift(
        date=str(for_date.date()),
        start=_datetime_to_epoch_ms(shift_start),
        finish=_datetime_to_epoch_ms(shift_finish),
        breaks=breaks,
        allowances=allowances,
        award_interpretations=award_interpretations,
    )


def generate_shifts(days: int = 360) -> List[Shift]:
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = today - timedelta(days=days)

    shifts: List[Shift] = [
        _generate_shift(for_date=current_date)
        for current_date in (start_date + timedelta(days=i) for i in range(days))
    ]

    return shifts
