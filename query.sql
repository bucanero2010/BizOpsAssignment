-- Keeping initial query for granular analysis, storing in a temp table
CREATE TEMP TABLE delivered_work AS (

    WITH devices AS (

        SELECT
                task_list_response_id
            ,   LISTAGG(DISTINCT description, ', ') WITHIN GROUP (ORDER BY description) AS devices

        FROM mysql_task_list_response_devices

        GROUP BY task_list_response_id

    )

    SELECT
            a.id AS account_id
        ,   pt.id AS task_id
        ,   TRUNC(pt.created_at)::TIMESTAMP AS created_at
        ,   CASE
                WHEN uc.job_title LIKE '%Testing Manager%' THEN 'TM'
                WHEN uc.job_title LIKE '%Testing Coordinator%' THEN 'TC'
                WHEN uc.job_title LIKE '%Engagement Manager%' THEN 'EM'
                WHEN uc.user_type_id = 3 THEN 'TL'
                WHEN uc.user_type_id = 1 THEN 'Client'
                ELSE 'Other'
            END AS creator_role
        ,   pt.is_done
        ,   CASE
                WHEN pt.is_done = 0 THEN 'allocated, not completed'
                WHEN pt.is_done = 1 AND pt.is_approved = 0 THEN 'completed, not approved'
            END AS is_done_str
        ,   (CASE
                WHEN pt.foreign_key_type = 'time_reporting' THEN pt.starts_at
                ELSE pt.done_at
            END)::TIMESTAMP AS done_at_utc
        ,   pt.starts_at::TIMESTAMP AS started_at_utc
        ,   pt.due_at::TIMESTAMP AS due_at
        ,   pt.is_approved
        ,   TRUNC(pt.approved_at)::TIMESTAMP AS approved_at
        ,   pt.is_billable
        ,   pt.assigned_to_user_id
        ,   ua.user_country AS assignee_country
        ,   ua.user_type AS assignee_user_type
        ,   tms.team_name
        ,   ttc.title AS task_type_category
        ,   pt.foreign_key_type AS task_origin
        ,   pt.hours AS hours
        ,   r.start_date::TIMESTAMP AS r_start_date
        ,   r.end_date::TIMESTAMP AS r_end_date
        ,   d.devices

    FROM mysql_accounts AS a
        LEFT JOIN mysql_project_tasks pt
            ON pt.account_id = a.id
        LEFT JOIN mysql_projects AS p
            ON p.id = pt.project_id
        LEFT JOIN mysql_task_types AS tt
            ON tt.id = pt.task_type_id
        LEFT JOIN mysql_task_type_categories AS ttc
            ON ttc.id = tt.task_type_category_id
        LEFT JOIN sheets_users AS ua
            ON ua.id = pt.assigned_to_user_id
        LEFT JOIN sheets_users AS ua2
            ON ua2.id = pt.approved_by
        LEFT JOIN sheets_users AS uc
            ON uc.id = pt.created_by
        LEFT JOIN mysql_runs AS r
            ON pt.test_cycle_id = r.id
        LEFT JOIN mysql_task_list_responses AS tlr
            ON tlr.id = pt.foreign_key
                AND pt.foreign_key_type = 'task_list_response'
        LEFT JOIN mysql_task_lists AS tl
            ON tl.id = tlr.task_list_id
        LEFT JOIN devices AS d
            ON tlr.id = d.task_list_response_id
        LEFT JOIN mysql_primary_tms AS tms
            ON a.id = tms.account_id

    WHERE a.id = 1078
        AND (p.managed_by_user = 0 OR pt.project_id IS NULL)
        AND tt.task_type_category_id NOT IN (5, 9, 15)  -- exclude Internal, Other and Reimbursement categories
        AND (pt.approved_at >= '2024-01-01' OR pt.is_approved = 0)

);


WITH order_staging AS (

    -- Unpivoting the Order Type column, to be able to join with the delivered_work table
    SELECT
            account_id
        ,   order_form_id
        ,   start_date
        ,   end_date
        ,   lookup_months
        ,   hours_usage
        ,   'Type X' AS order_type
        ,   type_x_hours_included AS hours_included
        ,   type_x_rate AS rate
        ,   type_x_overage_fee AS overage_fee
    
    FROM sheets_order_forms

    UNION ALL

    SELECT
            account_id
        ,   order_form_id
        ,   start_date
        ,   end_date
        ,   lookup_months
        ,   hours_usage
        ,   'Type 1' AS order_type
        ,   type_1_hours_included AS hours_included
        ,   type_1_rate AS rate
        ,   type_1_overage_fee AS overage_fee
    
    FROM sheets_order_forms

    UNION ALL

    SELECT
            account_id
        ,   order_form_id
        ,   start_date
        ,   end_date
        ,   lookup_months
        ,   hours_usage
        ,   'Type 2' AS order_type
        ,   type_2_hours_included AS hours_included
        ,   type_2_rate AS rate
        ,   type_2_overage_fee AS overage_fee
    
    FROM sheets_order_forms

    UNION ALL

    SELECT
            account_id
        ,   order_form_id
        ,   start_date
        ,   end_date
        ,   lookup_months
        ,   hours_usage
        ,   'Type 3' AS order_type
        ,   type_3_hours_included AS hours_included
        ,   NULL AS rate
        ,   type_3_overage_fee AS overage_fee
    
    FROM sheets_order_forms

    UNION ALL

    SELECT
            account_id
        ,   order_form_id
        ,   start_date
        ,   end_date
        ,   lookup_months
        ,   hours_usage
        ,   'Type 4' AS order_type
        ,   type_4_hours_included AS hours_included
        ,   type_4_rate AS rate
        ,   NULL AS overage_fee
    
    FROM sheets_order_forms

    UNION ALL

    SELECT
            account_id
        ,   order_form_id
        ,   start_date
        ,   end_date
        ,   lookup_months
        ,   hours_usage
        ,   'Type 5' AS order_type
        ,   type_5_hours_included AS hours_included
        ,   NULL AS rate
        ,   NULL AS overage_fee
    
    FROM sheets_order_forms
)

,   numbers AS (

    -- Table that has a list of numbers, intermediate steps to explode the Orders by month
    SELECT (ROW_NUMBER() OVER () - 1)::INTEGER AS n

    FROM  mysql_project_tasks -- Assuming this table has many rows, any table can be used

    LIMIT 50 -- Assuming projects don't last for more than 4 years

)

,   orders_monthly AS (

    -- Unnesting the lookup_months column to get month granularity
    SELECT
            s.account_id
        ,   DATE_TRUNC(
                    'month'
                ,   '1899-12-30'::DATE + 
                        TRIM(
                            SPLIT_PART(s.lookup_months, ',', (n.n + 1)::INTEGER)
                        )::INTEGER
            ) AS order_month -- Getting individual month and converting to date format
        ,   CASE 
                WHEN s.lookup_months IS NULL OR TRIM(s.lookup_months) = '' THEN 0
                ELSE REGEXP_COUNT(TRIM(BOTH ',' FROM s.lookup_months), ',') + 1
            END AS month_count
        ,   s.order_type
        ,   s.hours_usage
        -- Using hours_usage assign hours to months accordingly, assuming uniform work for Per Term usage
        ,   CASE
                WHEN s.hours_usage = 'Monthly' THEN s.hours_included
                WHEN s.hours_usage = 'Per term' THEN ROUND(s.hours_included / month_count, 2)::DECIMAL(10,2)
            END AS hours_included_monthly
        ,   s.rate
        ,   s.overage_fee

    FROM order_staging s
    CROSS JOIN numbers n
    
    WHERE n.n < REGEXP_COUNT(s.lookup_months, ',') + 1
      AND TRIM(SPLIT_PART(s.lookup_months, ',', (n.n + 1)::INTEGER)) <> ''

)

,   delivered_work_monthly AS (

    -- Staging delivered_work so it matches the orders data grain
    SELECT
            account_id
        ,   DATE_TRUNC('month', approved_at) AS delivered_month -- Using approved at as the main date
        ,   task_type_category
        ,   COUNT(DISTINCT task_id) AS tasks_delivered
        ,   SUM(hours) AS delivered_hours

    FROM delivered_work

    -- For simplicity, assuming only approved, billable and done hours are considered in the reporting
    WHERE approved_at IS NOT NULL
        AND is_billable = 1
        AND is_done = 1

    GROUP BY 1, 2, 3    

)

,   ordered_and_delivered AS (
    -- Merging data
    SELECT
        
        -- Dimensions
            COALESCE(o.account_id, d.account_id) AS account_id
        ,   COALESCE(o.order_month, d.delivered_month) AS month
        ,   COALESCE(o.order_type, d.task_type_category) AS task_type
        ,   COALESCE(o.hours_usage,'') AS hours_usage
        -- Ordered metrics
        ,   COALESCE(o.hours_included_monthly, 0) AS hours_included_monthly
        ,   COALESCE(o.rate, 0) AS rate
        ,   COALESCE(o.overage_fee, 0) AS overage_fee
        -- Delivered metrics
        ,   COALESCE(d.hours_delivered, 0) AS hours_delivered
        ,   COALESCE(d.tasks_delivered, 0) AS tasks_delivered

    FROM orders_monthly o
    FULL OUTER JOIN delivered_monthly d
        ON  d.account_id = o.account_id
            AND d.work_month = o.order_month
            AND d.order_type = o.order_type

)

SELECT
        account_id
    ,   month
    ,   task_type
    ,   hours_included_monthly
    ,   rate
    ,   overage_fee
    ,   hours_delivered
    ,   tasks_delivered
    -- Derived metrics
    ,   LEAST(hours_delivered, hours_included_monthly) AS included_hours_billed
    ,   GREATEST(hours_delivered - hours_included_monthly, 0) AS overage_hours_billed
    ,   included_hours_billed * rate AS included_revenue
    ,   CASE
            WHEN
                hours_usage = 'Monthly' -- Assuming overage is just billable when monthly
                AND overage_fee IS NOT NULL
                AND overage_hours_billed > 0
                THEN overage_hours_billed * overage_fee
            ELSE 0
        END AS overage_revenue

FROM ordered_and_delivered;