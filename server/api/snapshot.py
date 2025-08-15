from fastapi import APIRouter, HTTPException, Query
from futu import OpenQuoteContext

router = APIRouter()

@router.get("/snapshot")
def get_snapshot(codes: str = Query(..., description="Comma separated list of stock codes")):
    codes_list = [c.strip() for c in codes.split(',') if c.strip()]
    try:
        with OpenQuoteContext(host='127.0.0.1', port=11111) as ctx:
            ret, data = ctx.get_market_snapshot(codes_list)
            if ret != 0:
                raise HTTPException(status_code=502, detail=data)
            return {"data": data.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
