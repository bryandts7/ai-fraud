#standardSQL
WITH

  -- 0) All Data from Event Table
  all_events AS (
    {UNIONED_TABLES}
  ),
  
  
  -- 1) IP activity features
  ip_activity AS (
    SELECT
      ip,
      AVG(minutesActive) AS ipAvgMinutesActive,
      APPROX_QUANTILES(minutesActive, 1000)[OFFSET(500)] AS ipMinutesActivePerDay50,
      APPROX_QUANTILES(minutesActive, 1000)[OFFSET(900)] AS ipMinutesActivePerDay90,
      APPROX_QUANTILES(minutesActive, 1000)[OFFSET(990)] AS ipMinutesActivePerDay99
    FROM (
      SELECT
        ip,
        kv18 as appId,
        COUNT(*) AS minutesActive
      FROM all_events
      WHERE 
        kv18 IS NOT NULL
        AND deviceType LIKE '%mobile%'
      GROUP BY 
        ip, appId, 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), 'UTC')
    )
    GROUP BY ip
  ),

  -- 2) Device activity and fraud metrics
  device_metrics AS (
    SELECT
      ip,
      MIN(minutesActive) AS deviceIdMinMinutesActive,
      AVG(minutesActive) AS deviceIdAvgMinutesActive,
      APPROX_QUANTILES(minutesActive, 1000)[OFFSET(500)] AS deviceIdMinutesActivePerDay50,
      APPROX_QUANTILES(minutesActive, 1000)[OFFSET(900)] AS deviceIdMinutesActivePerDay90,
      APPROX_QUANTILES(minutesActive, 1000)[OFFSET(990)] AS deviceIdMinutesActivePerDay99,
      SUM(total)  AS total,
      AVG(total) AS totalPerDevice,
    FROM (
      SELECT
        ip,
        userId,
        COUNT(*) AS minutesActive,
        SUM(total) as total,
      FROM (
        SELECT
          ip,
          COALESCE(kv19, kv20, kv21, kv22, visitorId) as userId,
          FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), 'UTC') AS minute,
          COUNT(*) AS total
        FROM all_events
        WHERE 
          kv18 IS NOT NULL
          AND deviceType LIKE '%mobile%'
        GROUP BY ip, userId, minute
      )
      GROUP BY ip, userId
    )
    GROUP BY ip
  ),

  -- 3) Technical and behavioral features
  technical_features AS (
    SELECT
      ip,
      AVG(CASE WHEN b3 = TRUE THEN 1 ELSE 0 END) AS b3,
      APPROX_QUANTILES(CAST(eventTime AS INT64) - CAST(sessionTime AS INT64), 100)[OFFSET(49)] AS ttlQ50,
      APPROX_QUANTILES(CAST(eventTime AS INT64) - CAST(sessionTime AS INT64), 100)[OFFSET(98)] AS ttlQ99,
      COUNT(DISTINCT ip) AS totalIPs,
      SUM(CASE WHEN s24 IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS xDeviceUaImpressionRatio,
      SUM(CASE WHEN kv4 IS NOT NULL AND ip != kv4 THEN 1 ELSE 0 END) / COUNT(*) AS ipMismatchRatio,
      SUM(CASE WHEN kv4 IS NOT NULL AND s18 LIKE '%|%' AND NOT s18 LIKE CONCAT('%', kv4, '%') THEN 1 ELSE 0 END) / COUNT(*) AS noKv4InXffRatio,
      SUM(CASE WHEN kv19 IS NULL AND kv20 IS NULL AND kv21 IS NULL AND kv22 IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS nullDeviceIdRatio,
      COUNT(DISTINCT IFNULL(kv19, IFNULL(kv20, IFNULL(kv21, kv22)))) AS totalDevices
    FROM (
      SELECT
        kv4,
        s18,
        kv19,
        kv20,
        kv21,
        kv22,
        s24,
        impressions,
        ip,
        CASE WHEN b3 = TRUE THEN TRUE ELSE FALSE END AS b3,
        COALESCE(kv19, kv20, kv21, kv22, visitorId) as userId,
        visitorId,
        CAST(eventTime AS BIGNUMERIC) AS eventTime,
        CAST(sessionTime AS BIGNUMERIC) AS sessionTime,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), 'UTC') AS second,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), 'UTC') AS minute,
        FORMAT_TIMESTAMP('%Y-%m-%d %H', TIMESTAMP_MICROS(CAST(eventTime AS INT64) * 1000), 'UTC') AS hour
      FROM all_events
      WHERE 
        kv18 IS NOT NULL
        AND deviceType LIKE '%mobile%'
    )
    GROUP BY ip
  ),

  -- 4) App distribution per device
  app_distribution AS (
    SELECT
      ip,
      MIN(totalApps) AS minAppsPerDevice,
      MAX(totalApps) AS maxAppsPerDevice,
      AVG(totalApps) AS avgAppsPerDevice,
      APPROX_QUANTILES(totalApps, 1000)[OFFSET(500)] AS totalAppsQ50,
      APPROX_QUANTILES(totalApps, 1000)[OFFSET(900)] AS totalAppsQ90,
      APPROX_QUANTILES(totalApps, 1000)[OFFSET(990)] AS totalAppsQ99
    FROM (
      SELECT
        DATE(TIMESTAMP_MICROS(CAST(adInstanceTime AS INT64))) AS date,
        COALESCE(kv19, kv20, kv21, kv22, visitorId) AS deviceId,
        ip,
        kv18,
        COUNT(DISTINCT kv18) OVER (PARTITION BY COALESCE(kv19, kv20, kv21, kv22, visitorId)) AS totalApps
      FROM all_events
      WHERE 
        kv18 IS NOT NULL
        AND deviceType LIKE '%mobile%'
      GROUP BY date, deviceId, ip, kv18, kv19, kv20, kv21, kv22, visitorId
    )
    GROUP BY ip
  )

SELECT
  ia.ip,
  
  -- Traffic volume metrics
  dm.total,
  dm.totalPerDevice,
  
  -- IP activity patterns
  ia.ipAvgMinutesActive,
  ia.ipMinutesActivePerDay50,
  ia.ipMinutesActivePerDay90,
  ia.ipMinutesActivePerDay99,
  
  -- Device activity patterns  
  dm.deviceIdMinMinutesActive,
  dm.deviceIdAvgMinutesActive,
  dm.deviceIdMinutesActivePerDay50,
  dm.deviceIdMinutesActivePerDay90,
  dm.deviceIdMinutesActivePerDay99,
  
  -- Technical and behavioral indicators
  tf.b3,
  tf.ttlQ50,
  tf.ttlQ99,
  tf.xDeviceUaImpressionRatio,
  tf.ipMismatchRatio,
  tf.noKv4InXffRatio,
  tf.nullDeviceIdRatio,
  CAST(tf.totalDevices AS BIGNUMERIC) / tf.totalIPs AS deviceToIpRatio,
  
  -- App distribution metrics
  ad.minAppsPerDevice,
  ad.maxAppsPerDevice,
  ad.avgAppsPerDevice,
  ad.totalAppsQ50,
  ad.totalAppsQ90,
  ad.totalAppsQ99

FROM ip_activity ia
LEFT JOIN device_metrics dm ON ia.ip = dm.ip
LEFT JOIN technical_features tf ON ia.ip = tf.ip
LEFT JOIN app_distribution ad ON ia.ip = ad.ip

WHERE dm.total > 5

ORDER BY ia.ip;