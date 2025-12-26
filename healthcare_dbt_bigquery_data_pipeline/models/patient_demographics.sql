{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
        "field":"age_group",
        "data_type": "string"
    }
)}}

WITH age_groups AS (
    SELECT 
        patient_id,
        CASE
            WHEN age < 18 THEN 'Child'
            WHEN age BETWEEN 18 AND 34 THEN 'Young Adult'
            WHEN age BETWEEN 35 AND 64 THEN 'Adult'
            ELSE 'Senior'
        END AS age_group,
        gender,
        insurance_type
    FROM
        {{ source('healthcare_data', 'patient_data_external') }}
)
SELECT 
    patient_id,
    age_group,
    gender,
    insurance_type,
    COUNT(*) AS patient_count
FROM 
    age_groups
GROUP BY
    patient_id, age_group, gender, insurance_type
ORDER BY
    age_group