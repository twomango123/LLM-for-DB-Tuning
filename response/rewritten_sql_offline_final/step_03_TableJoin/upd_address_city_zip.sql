UPDATE addresses
SET city = CASE WHEN city LIKE '%Metropols%' THEN 'Metropolis' ELSE city END,
    zip_postcode = CASE WHEN zip_postcode LIKE '9021O' THEN '90210' ELSE zip_postcode END
ORDER BY address_id DESC
LIMIT 1;
