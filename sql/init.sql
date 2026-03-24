-- ============================================================
--  Oracle Health EHR Tables
--  Database: demo_db  |  User: demo
-- ============================================================

CREATE TABLE IF NOT EXISTS patients (
    patient_id        SERIAL PRIMARY KEY,
    mrn               VARCHAR(20) UNIQUE NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    date_of_birth     DATE,
    gender            VARCHAR(10),
    ssn               VARCHAR(20),
    phone             VARCHAR(20),
    email             VARCHAR(150),
    address           TEXT,
    insurance_id      VARCHAR(50),
    created_at        TIMESTAMP DEFAULT NOW(),
    updated_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS adt_events (
    event_id          SERIAL PRIMARY KEY,
    patient_id        INT REFERENCES patients(patient_id),
    event_type        VARCHAR(20) NOT NULL,
    event_timestamp   TIMESTAMP DEFAULT NOW(),
    facility_id       VARCHAR(20),
    ward              VARCHAR(50),
    bed_number        VARCHAR(20),
    attending_doctor  VARCHAR(100),
    reason            TEXT,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lab_results (
    result_id         SERIAL PRIMARY KEY,
    patient_id        INT REFERENCES patients(patient_id),
    order_id          VARCHAR(50),
    test_code         VARCHAR(20),
    test_name         VARCHAR(150),
    result_value      VARCHAR(100),
    unit              VARCHAR(50),
    reference_range   VARCHAR(100),
    abnormal_flag     VARCHAR(10),
    collected_at      TIMESTAMP,
    resulted_at       TIMESTAMP DEFAULT NOW(),
    ordering_provider VARCHAR(100),
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vitals (
    vital_id          SERIAL PRIMARY KEY,
    patient_id        INT REFERENCES patients(patient_id),
    heart_rate        INT,
    systolic_bp       INT,
    diastolic_bp      INT,
    temperature       DECIMAL(4,1),
    spo2              INT,
    respiratory_rate  INT,
    recorded_at       TIMESTAMP DEFAULT NOW(),
    recorded_by       VARCHAR(100),
    created_at        TIMESTAMP DEFAULT NOW()
);

-- Seed data
INSERT INTO patients (mrn, first_name, last_name, date_of_birth, gender, ssn, phone, email, address, insurance_id) VALUES
('MRN-001', 'James',   'Wilson',   '1965-03-12', 'M', '123-45-6789', '555-0101', 'j.wilson@email.com',   '123 Oak St, Austin TX',   'INS-100001'),
('MRN-002', 'Maria',   'Garcia',   '1978-07-24', 'F', '234-56-7890', '555-0102', 'm.garcia@email.com',   '456 Elm Ave, Dallas TX',  'INS-100002'),
('MRN-003', 'Robert',  'Chen',     '1952-11-08', 'M', '345-67-8901', '555-0103', 'r.chen@email.com',     '789 Pine Rd, Houston TX', 'INS-100003'),
('MRN-004', 'Sarah',   'Johnson',  '1990-02-15', 'F', '456-78-9012', '555-0104', 's.johnson@email.com',  '321 Maple Dr, Plano TX',  'INS-100004'),
('MRN-005', 'Michael', 'Thompson', '1945-09-30', 'M', '567-89-0123', '555-0105', 'm.thompson@email.com', '654 Cedar Ln, Irving TX', 'INS-100005');

INSERT INTO adt_events (patient_id, event_type, facility_id, ward, bed_number, attending_doctor, reason) VALUES
(1, 'ADMIT',     'FAC-001', 'Cardiology', 'BED-101', 'Dr. Patel',  'Chest pain evaluation'),
(2, 'ADMIT',     'FAC-001', 'ICU',        'BED-205', 'Dr. Smith',  'Respiratory distress'),
(3, 'TRANSFER',  'FAC-001', 'Med-Surg',   'BED-312', 'Dr. Lee',    'Post-op monitoring'),
(4, 'ADMIT',     'FAC-002', 'Maternity',  'BED-410', 'Dr. Rivera', 'Labor and delivery'),
(5, 'ADMIT',     'FAC-001', 'Cardiology', 'BED-102', 'Dr. Patel',  'Irregular heartbeat');

INSERT INTO lab_results (patient_id, order_id, test_code, test_name, result_value, unit, reference_range, abnormal_flag, collected_at, ordering_provider) VALUES
(1, 'ORD-001', 'TROP',  'Troponin I',  '0.08', 'ng/mL', '0.00-0.04', 'H', NOW() - INTERVAL '2 hours', 'Dr. Patel'),
(2, 'ORD-002', 'ABG',   'Blood Gas',   '7.31', 'pH',    '7.35-7.45', 'L', NOW() - INTERVAL '1 hour',  'Dr. Smith'),
(3, 'ORD-003', 'HBA1C', 'HbA1c',       '8.2',  '%',     '4.0-5.6',   'H', NOW() - INTERVAL '3 hours', 'Dr. Lee'),
(4, 'ORD-004', 'CBC',   'WBC Count',   '11.2', 'K/uL',  '4.5-11.0',  'H', NOW() - INTERVAL '30 min',  'Dr. Rivera'),
(5, 'ORD-005', 'BNP',   'BNP',         '450',  'pg/mL', '0-100',     'H', NOW() - INTERVAL '1 hour',  'Dr. Patel');

INSERT INTO vitals (patient_id, heart_rate, systolic_bp, diastolic_bp, temperature, spo2, respiratory_rate, recorded_by) VALUES
(1, 102, 158, 95,  98.6, 96, 18, 'Nurse Adams'),
(2, 118, 90,  60,  101.2, 88, 28, 'Nurse Brown'),
(3, 78,  130, 82,  98.8, 99, 16, 'Nurse Clark'),
(4, 88,  120, 78,  98.4, 99, 17, 'Nurse Davis'),
(5, 95,  142, 90,  99.1, 97, 20, 'Nurse Evans');

-- Enable CDC publication for Debezium
CREATE PUBLICATION ehr_pub FOR TABLE patients, adt_events, lab_results, vitals;