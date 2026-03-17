SELECT
    s.date,
    sp.ga_session_id,
    sp.continent,
    sp.country,
    sp.device,
    sp.browser,
    sp.mobile_model_name,
    sp.operating_system,
    sp.language,
    sp.name AS traffic_source,
    sp.channel AS traffic_channel,
    acs.account_id,
    acc.is_unsubscribed,
    acc.is_verified,
    p.category,
    p.name AS product_name,
    p.price,
    p.short_description
FROM `data-analytics-mate.DA.session_params` AS sp
LEFT JOIN `data-analytics-mate.DA.session` AS s
    ON sp.ga_session_id = s.ga_session_id
LEFT JOIN `data-analytics-mate.DA.account_session` AS acs
    ON sp.ga_session_id = acs.ga_session_id
LEFT JOIN `data-analytics-mate.DA.account` AS acc
    ON acs.account_id = acc.id
LEFT JOIN `data-analytics-mate.DA.order` AS o
    ON sp.ga_session_id = o.ga_session_id
LEFT JOIN `data-analytics-mate.DA.product` AS p
    ON o.item_id = p.item_id
