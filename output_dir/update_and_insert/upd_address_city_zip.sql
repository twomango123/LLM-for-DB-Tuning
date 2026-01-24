-- Correct a common typo in the newest address's city or zip
UPDATE addresses
SET city = CASE WHEN city LIKE '%Metropols%' THEN 'Metropolis' ELSE city END,
    zip_postcode = CASE WHEN zip_postcode LIKE '9021O' THEN '90210' ELSE zip_postcode END
WHERE address_id = (SELECT MAX(address_id) FROM addresses);

