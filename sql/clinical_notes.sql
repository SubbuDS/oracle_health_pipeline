-- clinical_notes.sql
-- Creates and seeds the clinical_notes table in PostgreSQL
-- Run: docker exec <postgres_container> psql -U demo -d ehr_db -f /sql/clinical_notes.sql

CREATE TABLE IF NOT EXISTS clinical_notes (
    note_id      SERIAL PRIMARY KEY,
    patient_id   INTEGER,
    note_type    VARCHAR(50),
    authored_by  VARCHAR(100),
    authored_at  TIMESTAMP DEFAULT NOW(),
    note_text    TEXT
);

INSERT INTO clinical_notes (patient_id, note_type, authored_by, note_text) VALUES
(1, 'ADMISSION', 'Dr. Patel', '61yo male chest pain radiating to left arm onset 3 hours ago. History of hypertension, hyperlipidemia. Troponin 0.08 on arrival. ECG shows ST depression V4-V6. Started aspirin, heparin drip. Pain 7/10. Patient anxious but cooperative.'),
(1, 'PROGRESS', 'Dr. Patel', 'Cath lab: 90% proximal LAD stenosis. Drug-eluting stent placed. Door-to-balloon 52 minutes. Post-procedure troponin 2.4 trending up. Transferred cardiac step-down. Dual antiplatelet started. Cardiac rehab referral placed.'),
(2, 'ADMISSION', 'Dr. Smith', '47yo female acute respiratory distress. SpO2 88% room air, 91% on 4L NC. COPD known, 30 pack-year smoking. ABG pH 7.31 pCO2 58 respiratory acidosis. Bronchodilators started. Family very distressed. If no improvement 2 hours will discuss intubation.'),
(2, 'NURSING', 'Nurse Chen', 'Patient increasingly anxious. SpO2 fluctuating 88-92%. Repositioned high Fowler. Bilateral expiratory wheezes. Using accessory muscles. Notified Dr. Smith of deterioration 14:30. Family requesting to speak with attending urgently.'),
(2, 'PROGRESS', 'Dr. Smith', 'ICU day 2. SpO2 improved 94% high-flow 12L. Avoided intubation overnight. Better response to NIV. More comfortable this morning. Continue respiratory support, wean O2 as tolerated.'),
(3, 'ADMISSION', 'Dr. Johnson', '73yo male AKI on CKD stage 3. Creatinine 2.8 baseline 1.4. Decreased urine output 2 days. Recent NSAID use for back pain likely cause. Foley placed 30ml/hr. IV fluids cautiously given cardiac history. Nephrology consult requested.'),
(3, 'PROGRESS', 'Dr. Johnson', 'Day 2. Creatinine trending down 2.8 to 2.1 to 1.8. Urine output improving. Patient ambulating hallway with assistance. NSAID discontinued permanently. Plan discharge tomorrow if creatinine continues improving.'),
(4, 'NURSING', 'Nurse Williams', 'Post-op day 1. Patient resting, pain 4/10 on PCA morphine. Vital signs stable. Ambulated to chair once. Blood glucose 142 at 16:00, sliding scale insulin given. Patient asking about discharge timeline.'),
(5, 'DISCHARGE', 'Dr. Chen', 'Hypertensive urgency resolved. BP 138/88 on oral medication. Patient educated low sodium diet, medication compliance, daily BP monitoring. Added amlodipine 5mg to existing lisinopril. Follow up PCP 1 week. Patient verbalized understanding.');
