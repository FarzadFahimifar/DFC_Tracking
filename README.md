# DFC_Tracking
  SELECT
  *,
IF
  (Tote
    OR Tote_2_Deep
    OR Shelf
    OR Floor_Shelf
    OR Rack_Full_Pallet
    OR Rack_Level_1,
    'NO',
    'Yes' ) AS Bulk,
  CASE
    WHEN UNIT_VOLUME=0 OR Total_Shipped_Qty=0 THEN 'Missing Value'
    WHEN Tote THEN 'Tote'
    WHEN Tote_2_Deep THEN 'Tote_2_Deep'
    WHEN Shelf THEN 'Shelf'
    WHEN Floor_Shelf THEN 'Floor_Shelf'
    WHEN Rack_Full_Pallet THEN 'Rack_Full_Pallet'
    WHEN Rack_Level_1 THEN 'Rack_Level_1'
  ELSE
  'Bulk'
END
  AS Loc_type,
FROM (
  SELECT
    O_FACILITY_ALIAS_ID AS Source_SFC,
    DESTINATION_SITE AS DFC,
    OLPN_SHIPPED_DTTM AS OLPN_SHIPPED_DATE,
    CHECKIN_DTTM AS Appointment,
    GATE_OUT,
    TRAILER_NO,
    A.Article,
    Article_Desc,
     c.LocName as Article_Bin,
    ifnull(Total_Stock,
      0) AS Total_DFC_OH,
    CAST(ifnull(Total_Stock,
        0)>0 AS INT64) AS Instock,
    D28_Flag,
    Dept,
    
    OLPN_ESTIMATED_WEIGHT,
    M.UNIT_LENGTH,
    M.UNIT_WIDTH,
    M.UNIT_HEIGHT,
    M.UNIT_WEIGHT,
    M.UNIT_VOLUME,
    SUM(Shipped_Qty) AS Total_Shipped_Qty,
  IF
    (LEAST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>8.5
      OR GREATEST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>10
      OR UNIT_LENGTH* UNIT_WIDTH*UNIT_HEIGHT*SUM(Shipped_Qty)>765
      OR UNIT_WEIGHT*SUM(Shipped_Qty)>31
      OR UNIT_WEIGHT>50,
      FALSE,
      TRUE) AS Tote,
  IF
    (LEAST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>8.5
      OR GREATEST(UNIT_LENGTH,UNIT_WIDTH,UNIT_HEIGHT)>18
      OR (UNIT_LENGTH* UNIT_WIDTH*UNIT_HEIGHT*SUM(Shipped_Qty))>1530
      OR UNIT_WEIGHT>50,
      FALSE,
      TRUE) AS Tote_2_Deep,
  IF
    (LEAST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>12
      OR GREATEST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>38
      OR (UNIT_LENGTH* UNIT_WIDTH*UNIT_HEIGHT*SUM(Shipped_Qty))>5472
      OR (UNIT_WEIGHT*SUM(Shipped_Qty))>125
      OR (UNIT_WEIGHT>50),
      FALSE,
      TRUE) AS Shelf,
  IF
    (LEAST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>12
      OR GREATEST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>42
      OR (UNIT_LENGTH* UNIT_WIDTH*UNIT_HEIGHT*SUM(Shipped_Qty))>13608,
      FALSE,
      TRUE) AS Floor_Shelf,
  IF
    (LEAST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>40
      OR GREATEST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>53
      OR (UNIT_LENGTH* UNIT_WIDTH*UNIT_HEIGHT*SUM(Shipped_Qty))>101760
      OR (UNIT_WEIGHT*SUM(Shipped_Qty))>2500
      OR (UNIT_WEIGHT>50),
      FALSE,
      TRUE) AS Rack_Full_Pallet,
  IF
    (LEAST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>40
      OR GREATEST(UNIT_LENGTH, UNIT_WIDTH,UNIT_HEIGHT)>76
      OR (UNIT_LENGTH* UNIT_WIDTH*UNIT_HEIGHT*SUM(Shipped_Qty))>145920,
      FALSE,
      TRUE) AS Rack_Level_1,
     
  FROM
    `analytics-ca-scm-thd.DFC_ASSORTMENT.DFC_IB_SFC_OB_TRACKING_TBL` A
    left join 
    `analytics-ca-scm-thd.FARZAD.DFC_Capacity_V4` c
    on
    c.Article=A.Article
    and c.Site=A.DESTINATION_SITE
    left join
    `analytics-ca-scm-thd.FARZAD.Marm_EA` M
    on
    M.Article=A.Article
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19
  )
LIMIT
  100000
