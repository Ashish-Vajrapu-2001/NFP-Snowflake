SELECT child.FLIGHT_ID
FROM {{ ref('stg_champ__air_waybill') }} child
LEFT JOIN {{ ref('stg_aims__flight_leg') }} parent
    ON child.FLIGHT_ID = parent.FLIGHT_ID
WHERE child.FLIGHT_ID IS NOT NULL
  AND parent.FLIGHT_ID IS NULL