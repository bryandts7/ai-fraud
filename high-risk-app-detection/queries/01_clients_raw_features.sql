#standardSQL
WITH
  ---------------------------------------------------------------------------
  -- all_events: Filter down to mobile events with a minimum total impressions
  -- - Only include rows where:
  --     • deviceType is mobile
  --     • appId (kv18) is present
  --     • event date is between start_date and end_date
  --     • the app has more than 10 total impressions in that window
  ---------------------------------------------------------------------------
  all_events AS (
    SELECT *
    FROM `pixalate.com:pixalate.MEDIAPLANNER_RAW.EventAugmented_{DATE}`
    WHERE
      deviceType LIKE 'mobile'
      AND kv18 IS NOT NULL
  ),

  ---------------------------------------------------------------------------
  -- device_hourly_counts: Count unique devices per app, OS, and hour
  -- - osName: normalized to lowercase, mapping 'mac' → 'ios'
  -- - hour: truncated timestamp to the hour
  -- - hourlyDevices: distinct visitorId count in each hour
  ---------------------------------------------------------------------------
  device_hourly_counts AS (
    SELECT
      kv18                                                    AS appId,
      SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
      TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(eventTime AS INT64)), HOUR) AS hour,
      COUNT(DISTINCT visitorId)                                         AS hourlyDevices
    FROM all_events
    GROUP BY appId, osName, hour
  ),

  ---------------------------------------------------------------------------
  -- device_uniformity: Measure how uniformly devices appear across hours
  -- - deviceUniformityRatio = hours with activity / total hours in window
  ---------------------------------------------------------------------------
  device_uniformity AS (
    SELECT
      appId,
      osName,
      SAFE_DIVIDE(
        COUNTIF(hourlyDevices > 0),
        TIMESTAMP_DIFF(MAX(hour), MIN(hour), HOUR) + 1
      ) AS deviceUniformityRatio
    FROM device_hourly_counts
    GROUP BY appId, osName
  ),

  ---------------------------------------------------------------------------
  -- imp_features: Hourly impression & fraud aggregates per app & OS
  -- - totalImpressions, totalFraudImpressions, fraudRate
  -- - spikeRatio: peak hour vs. average impressions
  -- - hoursActive: count of hours with any impressions
  -- - impUniformityRatio: similar to deviceUniformity but for impressions
  ---------------------------------------------------------------------------
  imp_features AS (
    SELECT
      appId,
      osName,
      SUM(hourlyImpressions)                                                 AS totalImpressions,
      SUM(fraudImpressions)                                                  AS totalFraudImpressions,
      SAFE_DIVIDE(SUM(fraudImpressions), NULLIF(SUM(hourlyImpressions), 0))  AS fraudRate,
      SAFE_DIVIDE(MAX(hourlyImpressions), NULLIF(AVG(hourlyImpressions), 0)) AS spikeRatio,
      COUNTIF(hourlyImpressions > 0)                                         AS hoursActive,
      SAFE_DIVIDE(
        COUNTIF(hourlyImpressions > 0),
        TIMESTAMP_DIFF(MAX(hour), MIN(hour), HOUR) + 1
      )                                                                      AS impUniformityRatio
    FROM (
      SELECT
        kv18 AS appId,
        SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
        TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(eventTime AS INT64)), HOUR) AS hour,
        SUM(impressions) AS hourlyImpressions,
        SUM(IF(fraudTypeDev IS NOT NULL, impressions, 0)) AS fraudImpressions
      FROM all_events
      GROUP BY appId, osName, hour
    )
    GROUP BY appId, osName
  ),

  ---------------------------------------------------------------------------
  -- device_overview: Device-level activity & IP variety per app & OS
  -- 1) per_app_device: compute per-device metrics
  -- 2) with_app_counts: count how many distinct apps each device sees
  -- 3) final: aggregate statistics across devices
  ---------------------------------------------------------------------------
  device_overview AS (
    WITH per_app_device AS (
      SELECT
        visitorId   AS deviceId,
        kv18        AS appId,
        SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
        SUM(impressions)                 AS deviceImps,
        COUNT(DISTINCT FORMAT_TIMESTAMP('%%Y-%%m-%%d %%H:%%M',
                     TIMESTAMP_MILLIS(CAST(eventTime AS INT64))
        ))                               AS minutesActive,
        COUNT(DISTINCT ip)               AS distinctIps
      FROM all_events
      GROUP BY deviceId, appId, osName
    ),
    with_app_counts AS (
      SELECT
        p.*,
        COUNT(DISTINCT appId) OVER (PARTITION BY deviceId, osName) AS appsPerDevice
      FROM per_app_device p
    )
    SELECT
      appId,
      osName,
      COUNT(DISTINCT deviceId)                                        AS uniqueDevices,
      STDDEV_SAMP(deviceImps)                                         AS deviceImpsStdDev,
      SAFE_DIVIDE(STDDEV_SAMP(deviceImps), NULLIF(AVG(deviceImps),0)) AS deviceImpCV,
      APPROX_QUANTILES(deviceImps, 100)[OFFSET(50)]                   AS impPerDeviceQ50,
      APPROX_QUANTILES(deviceImps, 100)[OFFSET(90)]                   AS impPerDeviceQ90,
      APPROX_QUANTILES(deviceImps, 100)[OFFSET(99)]                   AS impPerDeviceQ99,
      AVG(minutesActive)                                              AS deviceIdAvgMinutes,
      APPROX_QUANTILES(minutesActive, 1001)[OFFSET(500)]              AS deviceIdMinutesActivePerDay50,
      APPROX_QUANTILES(minutesActive, 1001)[OFFSET(900)]              AS deviceIdMinutesActivePerDay90,
      APPROX_QUANTILES(minutesActive, 1001)[OFFSET(990)]              AS deviceIdMinutesActivePerDay99,
      APPROX_QUANTILES(distinctIps, 100)[OFFSET(50)]                  AS ipsPerDeviceQ50,
      APPROX_QUANTILES(distinctIps, 100)[OFFSET(90)]                  AS ipsPerDeviceQ90,
      APPROX_QUANTILES(distinctIps, 100)[OFFSET(99)]                  AS ipsPerDeviceQ99,
      APPROX_QUANTILES(appsPerDevice, 100)[OFFSET(50)]                AS appsPerDeviceQ50,
      APPROX_QUANTILES(appsPerDevice, 100)[OFFSET(90)]                AS appsPerDeviceQ90,
      APPROX_QUANTILES(appsPerDevice, 100)[OFFSET(99)]                AS appsPerDeviceQ99
    FROM with_app_counts
    GROUP BY appId, osName
  ),

  ---------------------------------------------------------------------------
  -- ip_hourly_counts: Count distinct IPs per app, OS, and hour
  ---------------------------------------------------------------------------
  ip_hourly_counts AS (
    SELECT
      kv18 AS appId,
      SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
      TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(eventTime AS INT64)), HOUR) AS hour,
      COUNT(DISTINCT ip) AS hourlyIps
    FROM all_events
    GROUP BY appId, osName, hour
  ),

  ---------------------------------------------------------------------------
  -- ip_uniformity: Measure uniformity of IP usage across hours
  ---------------------------------------------------------------------------
  ip_uniformity AS (
    SELECT
      appId,
      osName,
      SAFE_DIVIDE(
        COUNTIF(hourlyIps > 0),
        TIMESTAMP_DIFF(MAX(hour), MIN(hour), HOUR) + 1
      ) AS ipUniformityRatio
    FROM ip_hourly_counts
    GROUP BY appId, osName
  ),

  ---------------------------------------------------------------------------
  -- ip_summary: Per-IP impression distribution and quality metrics
  -- - ipImpCV: coefficient of variation of impressions per IP
  -- - ipMismatchRatio, nullDeviceIdRatio, ttl quantiles
  ---------------------------------------------------------------------------
  ip_summary AS (
    WITH per_app_ip AS (
      SELECT
        kv18 AS appId,
        SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
        ip,
        SUM(impressions) AS ipImps
      FROM all_events
      GROUP BY appId, osName, ip
    ),
    raw_metrics AS (
      SELECT
        kv18 AS appId,
        SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
        SUM(impressions) AS totalImpressions,
        COUNT(DISTINCT ip) AS uniqueIps,
        SUM(IF(kv4 IS NOT NULL AND ip <> kv4, 1, 0)) / COUNT(*) AS ipMismatchRatio,
        SUM(IF(visitorId IS NULL, 1, 0)) / COUNT(*) AS nullDeviceIdRatio,
        APPROX_QUANTILES(eventTime - sessionTime, 100)[OFFSET(50)] AS ttlQ50,
        APPROX_QUANTILES(eventTime - sessionTime, 100)[OFFSET(90)] AS ttlQ90
      FROM all_events
      GROUP BY appId, osName
    )
    SELECT
      r.appId,
      r.osName,
      r.totalImpressions,
      r.uniqueIps,
      SAFE_DIVIDE(r.totalImpressions, r.uniqueIps) AS impsPerIp,
      STDDEV_SAMP(p.ipImps) AS ipImpsStdDev,
      SAFE_DIVIDE(STDDEV_SAMP(p.ipImps), NULLIF(AVG(p.ipImps), 0)) AS ipImpCV,
      r.ipMismatchRatio,
      r.nullDeviceIdRatio,
      r.ttlQ50,
      r.ttlQ90
    FROM raw_metrics r
    LEFT JOIN per_app_ip p
      ON r.appId = p.appId
     AND r.osName = p.osName
    GROUP BY
      r.appId, r.osName,
      r.totalImpressions, r.uniqueIps,
      r.ipMismatchRatio, r.nullDeviceIdRatio,
      r.ttlQ50, r.ttlQ90
  ),

  ---------------------------------------------------------------------------
  -- ip_overview: Distribution of devices per IP for each app & OS
  ---------------------------------------------------------------------------
  ip_overview AS (
    SELECT
      appId,
      osName,
      APPROX_QUANTILES(device_count, 100)[OFFSET(50)] AS devicesPerIpQ50,
      APPROX_QUANTILES(device_count, 100)[OFFSET(90)] AS devicesPerIpQ90,
      APPROX_QUANTILES(device_count, 100)[OFFSET(99)] AS devicesPerIpQ99
    FROM (
      SELECT
        kv18 AS appId,
        SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
        ip,
        COUNT(DISTINCT visitorId) AS device_count
      FROM all_events
      GROUP BY appId, osName, ip
    )
    GROUP BY appId, osName
  ),

  ---------------------------------------------------------------------------
  -- ip_minutes: Active minutes per IP per day
  ---------------------------------------------------------------------------
  ip_minutes AS (
    SELECT
      appId,
      osName,
      AVG(minutesActive)                                            AS ipAvgMinutesActive,
      APPROX_QUANTILES(minutesActive, 1001)[OFFSET(500)]            AS ipMinutesActivePerDay50,
      APPROX_QUANTILES(minutesActive, 1001)[OFFSET(900)]            AS ipMinutesActivePerDay90,
      APPROX_QUANTILES(minutesActive, 1001)[OFFSET(990)]            AS ipMinutesActivePerDay99
    FROM (
      SELECT
        ip,
        kv18 AS appId,
        SPLIT(REPLACE(LOWER(os), 'mac', 'ios'), ' ')[OFFSET(0)] AS osName,
        COUNT(DISTINCT FORMAT_TIMESTAMP('%%Y-%%m-%%d %%H:%%M',
                TIMESTAMP_MILLIS(CAST(eventTime AS INT64))
        )) AS minutesActive
      FROM all_events
      GROUP BY ip, appId, osName
    )
    GROUP BY appId, osName
  ),

  ---------------------------------------------------------------------------
  -- LatestCrawl: Most recent crawl status per seller domain
  ---------------------------------------------------------------------------
  LatestCrawl AS (
    SELECT
      NET.HOST(adDomain) AS sellerDomain,
      statusCode
    FROM `pixalate.com:pixalate.MEDIAPLANNER_CRAWLER.Crawled_sellers_20250502`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY NET.HOST(adDomain)
      ORDER BY created DESC
    ) = 1
  ),

  ---------------------------------------------------------------------------
  -- seller_insights: Ads.txt & domain authorization metrics per app
  ---------------------------------------------------------------------------
  seller_insights AS (
    SELECT
      m.appId,
      COUNT(DISTINCT m.sellerDomain) AS totalSellerDomains,
      COUNT(DISTINCT CASE WHEN m.adsTxtSellerInfo = 'DIRECT' THEN m.sellerDomain END)   AS directSellerDomains,
      COUNT(DISTINCT CASE WHEN m.adsTxtSellerInfo = 'RESELLER' THEN m.sellerDomain END) AS resellerSellerDomains,
      COUNT(DISTINCT CASE WHEN lc.statusCode = 200 THEN m.sellerDomain END)             AS authorizedSellerDomains,
      COUNT(DISTINCT CASE WHEN lc.statusCode != 200 OR lc.sellerDomain IS NULL THEN m.sellerDomain END) AS unauthorizedSellerDomains
    FROM `pixalate.com:pixalate.GLOBAL.MobileAdsTxtSellers_20250525` m
    LEFT JOIN `pixalate.com:pixalate.MEDIAPLANNER_SELLER.SellerJsonInsightStats_20250511` s
      ON m.pixSellerName = s.pixSellerName
    LEFT JOIN LatestCrawl lc
      ON lc.sellerDomain = m.sellerDomain
    GROUP BY m.appId
  ),

  ---------------------------------------------------------------------------
  -- app_metadata: Basic app store metadata per app
  ---------------------------------------------------------------------------
  app_metadata AS (
    SELECT
      appId,
      MAX(userRatingCountForCurrentVersion) AS userRatingCountForCurrentVersion,
      MAX(downloadsMin)                 AS downloadsMin,
      MAX(downloadsMax)                 AS downloadsMax
    FROM `pixalate.com:pixalate.MEDIAPLANNER_REPORTS.www_MobileAppMrtSearch_{LAST_MONTH}`
    WHERE appId IS NOT NULL
    GROUP BY appId
  ),

  ---------------------------------------------------------------------------
  -- historical_fraud: Prior fraud stats from past reports per app
  ---------------------------------------------------------------------------
  historical_fraud AS (
    SELECT
      appId,
      MAX(givtSivtImpressions) AS historicalFraudImpressions,
      MAX(givtSivtRate)        AS historicalFraudRate
    FROM `pixalate.com:pixalate.MEDIAPLANNER_REPORTS.www_MobileAppMrtSearch_{LAST_MONTH}`
    WHERE appId IS NOT NULL
    GROUP BY appId
  )

-- Final aggregation: join all feature sets together per app & OS
SELECT
  f.appId,
  f.osName,

  -- impression features
  f.totalImpressions,
  f.totalFraudImpressions,
  f.fraudRate,
  f.hoursActive,
  f.spikeRatio,
  f.impUniformityRatio,

  -- device overview metrics
  d.uniqueDevices,
  u.deviceUniformityRatio,
  d.deviceImpsStdDev,
  d.deviceImpCV,
  d.impPerDeviceQ50,
  d.impPerDeviceQ90,
  d.impPerDeviceQ99,
  d.deviceIdMinutesActivePerDay50,
  d.deviceIdMinutesActivePerDay90,
  d.deviceIdMinutesActivePerDay99,
  d.ipsPerDeviceQ50        AS ipPerDeviceQ50,
  d.ipsPerDeviceQ90        AS ipPerDeviceQ90,
  d.ipsPerDeviceQ99        AS ipPerDeviceQ99,
  d.appsPerDeviceQ50        AS appsPerDeviceQ50,
  d.appsPerDeviceQ90        AS appsPerDeviceQ90,
  d.appsPerDeviceQ99        AS appsPerDeviceQ99,
  SAFE_DIVIDE(d.deviceIdAvgMinutes, NULLIF(f.hoursActive, 0)) AS deviceAvgMinPerHour,

  -- IP summary metrics
  i.uniqueIps,
  i.impsPerIp,
  i.ipMismatchRatio,
  i.nullDeviceIdRatio,
  i.ttlQ50,
  i.ttlQ90,
  i.ipImpsStdDev,
  i.ipImpCV,
  u2.ipUniformityRatio,

  -- devices per IP overview
  iv.devicesPerIpQ50       AS devicePerIpQ50,
  iv.devicesPerIpQ90       AS devicePerIpQ90,
  iv.devicesPerIpQ99       AS devicePerIpQ99,

  -- IP activity minutes
  m.ipMinutesActivePerDay50,
  m.ipMinutesActivePerDay90,
  m.ipMinutesActivePerDay99,
  SAFE_DIVIDE(m.ipAvgMinutesActive, NULLIF(f.hoursActive, 0))        AS ipAvgMinutesPerHour,
  SAFE_DIVIDE(m.ipAvgMinutesActive, NULLIF(d.deviceIdAvgMinutes, 0)) AS ipDeviceMinutesRatio,

  -- seller and ads.txt insights
  s.totalSellerDomains,
  s.directSellerDomains,
  s.resellerSellerDomains,
  s.authorizedSellerDomains,
  s.unauthorizedSellerDomains,

  -- app metadata ratios
  am.downloadsMin,
  am.downloadsMax,
  SAFE_DIVIDE(am.downloadsMax, NULLIF(i.uniqueIps, 0))                            AS downloadsPerIp,
  SAFE_DIVIDE(am.downloadsMax, NULLIF(d.uniqueDevices, 0))                        AS downloadsPerDevice,
  SAFE_DIVIDE(am.userRatingCountForCurrentVersion, NULLIF(f.totalImpressions, 0)) AS userRatingPerImpression,
  SAFE_DIVIDE(am.userRatingCountForCurrentVersion, NULLIF(d.uniqueDevices, 0))    AS userRatingPerDevice,
  SAFE_DIVIDE(am.userRatingCountForCurrentVersion, NULLIF(i.uniqueIps, 0))        AS userRatingPerIp,
  SAFE_DIVIDE(am.downloadsMax, NULLIF(am.userRatingCountForCurrentVersion, 0))    AS downloadsPerUserRating,

  -- historical fraud stats
  h.historicalFraudImpressions,
  h.historicalFraudRate

FROM imp_features              f
LEFT JOIN device_overview     d  ON f.appId = d.appId   AND f.osName = d.osName
LEFT JOIN device_uniformity   u  ON f.appId = u.appId   AND f.osName = u.osName
LEFT JOIN ip_summary          i  ON f.appId = i.appId   AND f.osName = i.osName
LEFT JOIN ip_uniformity       u2 ON f.appId = u2.appId  AND f.osName = u2.osName
LEFT JOIN ip_overview         iv ON f.appId = iv.appId  AND f.osName = iv.osName
LEFT JOIN ip_minutes          m  ON f.appId = m.appId   AND f.osName = m.osName
LEFT JOIN seller_insights     s  ON f.appId = s.appId
LEFT JOIN app_metadata        am ON f.appId = am.appId
LEFT JOIN historical_fraud    h  ON f.appId = h.appId

WHERE
  f.totalImpressions > 10

-- Order results by overall activity volume
ORDER BY f.totalImpressions DESC;