

-- Total Tickets
SELECT COUNT(*) as total_tickets FROM gold_fact_tickets;


-- SLA Breach Rate %
SELECT 
    ROUND(SUM(CASE WHEN is_sla_first_breached THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as first_response_breach_rate,
    ROUND(SUM(CASE WHEN is_sla_resolution_breached THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as resolution_breach_rate
FROM gold_fact_tickets;


-- Average Times (Latency)
SELECT 
    ROUND(AVG(first_response_min), 2) as avg_first_response_time_min,
    ROUND(AVG(resolution_min), 2) as avg_resolution_time_min
FROM gold_fact_tickets;


-- Reopen Rate %
SELECT 
    ROUND(SUM(CASE WHEN is_reopened THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as reopen_rate_pct
FROM gold_fact_tickets;


-- Refund Amount & Revenue Impact
SELECT 
    SUM(refund_amount) as total_refund_amount,
    SUM(revenue_impact) as total_revenue_impact
FROM gold_fact_tickets;


-- Tickets By City/Region
SELECT 
    r.city_name,
    r.region_name,
    COUNT(t.ticket_id) as ticket_count
FROM gold_fact_tickets t
LEFT JOIN gold_regions r ON t.region_id = r.region_id
GROUP BY 1, 2
ORDER BY 3 DESC;


-- Tickets By Restaurant
SELECT 
    rest.restaurant_name,
    COUNT(t.ticket_id) as ticket_count
FROM gold_fact_tickets t
LEFT JOIN gold_restaurants rest ON t.restaurant_id = rest.restaurant_id
GROUP BY 1
ORDER BY 2 DESC;


-- Tickets By Driver
SELECT 
    d.driver_name,
    COUNT(t.ticket_id) as ticket_count
FROM gold_fact_tickets t
LEFT JOIN gold_drivers d ON t.driver_id = d.driver_id
GROUP BY 1
ORDER BY 2 DESC;


-- Complaint Rate Per 1,000 Orders
SELECT 
    (SELECT COUNT(*) FROM gold_fact_tickets) * 1000.0 / 
    (SELECT COUNT(*) FROM gold_fact_orders) as complaint_rate_per_1000_orders;


-- Revenue Loss By Reason Category
SELECT 
    reas.reason_category_name,
    SUM(t.revenue_impact) as total_loss
FROM gold_fact_tickets t
LEFT JOIN gold_reasons reas ON t.reason_id = reas.reason_id
GROUP BY 1
ORDER BY 2 DESC;
