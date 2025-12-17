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
            WHEN pt.is_done = 1 AND pt.is_approved = 0
            THEN 'completed, not approved'
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
;