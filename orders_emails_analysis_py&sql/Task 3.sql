SELECT distinct(customer_instance_id) AS customer_id, 
COUNT(DISTINCT(email_name)) AS total_emails_sent,
sum(qty_opens) as times_open
FROM emails
GROUP BY customer_id
ORDER BY times_open DESC;