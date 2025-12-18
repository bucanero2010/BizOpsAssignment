from yoyo import step

steps = [
    step("""
    -- Schema based in Order Form sheet as of Dec 2025
    -- Changes may be needed if the sheet structure changes

    CREATE TABLE IF NOT EXISTS sheets_order_forms (
        account_id INTEGER,
        order_form_id INTEGER,
        start_date DATE,
        end_date DATE,
        lookup_months VARCHAR(1024),
        type VARCHAR(100),
        hours_usage VARCHAR(100),
        type_x_hours_included DECIMAL(10,2),
        type_x_hourly_rate DECIMAL(10,2),
        type_x_overage_fee DECIMAL(10,2),
        type_1_hours_included DECIMAL(10,2),
        type_1_rate DECIMAL(10,2),
        type_1_overage_fee DECIMAL(10,2),
        type_3_hours_included DECIMAL(10,2),
        type_5_hours_included DECIMAL(10,2),
        type_2_hours_included DECIMAL(10,2),
        type_2_rate DECIMAL(10,2),
        type_2_overage_fee DECIMAL(10,2),
        type_4_hours_included DECIMAL(10,2),
        type_4_rate DECIMAL(10,2),
        type_3_overage_fee DECIMAL(10,2)
    );
    """, """
    DROP TABLE IF EXISTS sheets_order_form;
    """)
]
