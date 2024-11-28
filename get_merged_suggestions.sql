SELECT 
    pd.title AS `Group Title`,
    COUNT(pdl.product_id) AS `Products Count`,
    MAX(pd.updated_at) AS `Updated At`
FROM 
    product_duplicate pd
JOIN 
    product_duplicate_details pdl 
ON 
    pd.id = pdl.product_duplicate_id
GROUP BY 
    pd.title
ORDER BY 
    `Updated At` DESC;