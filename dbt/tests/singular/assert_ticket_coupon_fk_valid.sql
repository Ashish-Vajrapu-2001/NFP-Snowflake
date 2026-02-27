SELECT child.FLIGHT_NUMBER, child.FLIGHT_DATE
FROM {{ ref('stg_amadeus__ticket_coupon') }} child
LEFT JOIN {{ ref('stg_aims__flight_leg') }} parent
    ON child.FLIGHT_NUMBER = parent.FLIGHT_NUMBER
    AND child.FLIGHT_DATE = parent.FLIGHT_DATE
WHERE child.FLIGHT_NUMBER IS NOT NULL
  AND parent.FLIGHT_ID IS NULL