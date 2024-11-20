CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS shifts (
    shift_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shift_date DATE NOT NULL,
    shift_start TIMESTAMP NOT NULL,
    shift_finish TIMESTAMP NOT NULL,
    shift_cost NUMERIC(13, 4)
);

CREATE TABLE IF NOT EXISTS breaks (
    break_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shift_id UUID NOT NULL,
    break_start TIMESTAMP NOT NULL,
    break_finish TIMESTAMP NOT NULL,
    is_paid BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (shift_id)
        REFERENCES shifts (shift_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS allowances (
    allowance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shift_id UUID NOT NULL,
    allowance_value FLOAT NOT NULL,
    allowance_cost NUMERIC(13, 4) NOT NULL,
    FOREIGN KEY (shift_id)
        REFERENCES shifts (shift_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS award_interpretations (
    award_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shift_id UUID NOT NULL,
    award_date DATE NOT NULL,
    award_units FLOAT NOT NULL,
    award_cost NUMERIC(13, 4) NOT NULL,
    FOREIGN KEY (shift_id)
        REFERENCES shifts (shift_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS kpis (
    kpi_id SERIAL PRIMARY KEY,
    kpi_name VARCHAR(255) NOT NULL,
    kpi_date DATE NOT NULL,
    kpi_value NUMERIC(8, 2) NOT NULL,
    UNIQUE (kpi_name, kpi_date)
)