from typing import List

import uvicorn
from fastapi import FastAPI, Query

from app.generator import generate_shifts
from app.models import NavigationLinks, Response, Shift

SHIFTS: List[Shift] = generate_shifts(days=360)

app = FastAPI(title="Shifts API")


@app.get(
    "/api/shifts",
    summary="Get shifts information",
    description="Returns paginated shifts response. Each response contains "
    "navigation links with 'prev' and 'next' fields. When response does not contain "
    "'prev' field, then results correspond to the first page. When response does not "
    "contain 'next' field, than there are no more results.",
    response_model=Response,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "start": 0,
                        "limit": 7,
                        "size": 1,
                        "links": {
                            "base": "http://localhost:8000",
                            "prev": "/api/shifts?start=0&limit=7",
                            "next": "/api/shifts?start=14&limit=7",
                        },
                        "results": [
                            {
                                "id": "0438ff1e-5160-4cdf-bc18-2d84b96f556d",
                                "date": "2021-01-01",
                                "start": 1609484400000,
                                "finish": 1609527600000,
                                "breaks": [
                                    {
                                        "id": "c1ccfad9-50f6-417d-a52e-afb0d18f6763",
                                        "start": 1609498800000,
                                        "finish": 1609502400000,
                                        "paid": False,
                                    }
                                ],
                                "allowances": [
                                    {
                                        "id": "7cf3d3a8-1ac1-4616-afa8-dcf3ae4b0474",
                                        "value": 1.0,
                                        "cost": 11.8,
                                    },
                                    {
                                        "id": "76fa2b79-a4ad-4a5d-8fc1-ef19cdc3d7af",
                                        "value": 1.0,
                                        "cost": 15.0,
                                    },
                                ],
                                "award_interpretations": [
                                    {
                                        "id": "81b2430d-60be-40b0-ba66-b736027e4572",
                                        "date": "2021-01-01",
                                        "units": 1.0,
                                        "cost": 8.43,
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        }
    },
)
def get_shifts(
    start: int = Query(
        0,
        ge=0,
        description="Page start index. Pages are zero-indexed.",
    ),
    limit: int = Query(
        7,
        ge=1,
        le=30,
        description="Results size per page. Default value corresponds to "
        "the one week of shifts data.",
    ),
) -> Response:
    results = SHIFTS[start : start + limit]

    prev_link = (
        f"/api/shifts?start={start - limit}&limit={limit}"
        if start - limit >= 0
        else None
    )
    next_link = (
        f"/api/shifts?start={start + limit}&limit={limit}"
        if start + limit < len(SHIFTS)
        else None
    )
    links = NavigationLinks(prev=prev_link, next=next_link)

    response = Response(
        start=start,
        limit=limit,
        size=len(results),
        results=results,
        links=links,
    )
    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
