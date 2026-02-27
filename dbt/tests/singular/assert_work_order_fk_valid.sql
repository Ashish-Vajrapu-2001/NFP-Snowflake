SELECT child.AIRCRAFT_REG
FROM {{ ref('stg_amos__work_order') }} child
LEFT JOIN {{ ref('snp_amos__aircraft_fleet') }} parent
    ON child.AIRCRAFT_REG = parent.AIRCRAFT_REG
    AND parent.dbt_is_current = TRUE
WHERE child.AIRCRAFT_REG IS NOT NULL
  AND parent.AIRCRAFT_REG IS NULL