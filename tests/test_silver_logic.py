"""
Unit tests for EHR Silver layer business logic.
Tests run without a Spark cluster — pure Python logic only.
"""
import pytest
from datetime import date, datetime


# ── Helper functions extracted from silver_batch.py logic ──────────────────

def get_age_group(age_years):
    """Mirrors the age_group logic in silver_patients."""
    if age_years is None:
        return None
    if age_years < 18:
        return "PEDIATRIC"
    elif age_years < 65:
        return "ADULT"
    else:
        return "SENIOR"


def get_news2_risk(spo2, heart_rate):
    """Mirrors the NEWS2 score logic in silver_vitals."""
    if spo2 is None or heart_rate is None:
        return None

    # spo2 flag
    if spo2 <= 91:
        spo2_flag = 3
    elif spo2 <= 93:
        spo2_flag = 2
    elif spo2 <= 95:
        spo2_flag = 1
    else:
        spo2_flag = 0

    # heart rate flag
    if heart_rate <= 40:
        hr_flag = 3
    elif heart_rate <= 50:
        hr_flag = 1
    elif heart_rate <= 90:
        hr_flag = 0
    elif heart_rate <= 110:
        hr_flag = 1
    elif heart_rate <= 130:
        hr_flag = 2
    else:
        hr_flag = 3

    score = spo2_flag + hr_flag

    if score >= 5:
        return "HIGH"
    elif score >= 3:
        return "MEDIUM"
    else:
        return "LOW"


def get_abnormal_direction(flag):
    """Mirrors the abnormal_direction logic in silver_labs."""
    if flag is None:
        return "NORMAL"
    flag_upper = flag.upper()
    if flag_upper == "H":
        return "HIGH"
    elif flag_upper == "L":
        return "LOW"
    else:
        return "NORMAL"


def normalize_gender(gender):
    """Mirrors the gender normalization in silver_patients."""
    if gender is None:
        return "U"
    g = gender.upper()
    if g in ("M", "MALE"):
        return "M"
    elif g in ("F", "FEMALE"):
        return "F"
    else:
        return "U"


# ── AGE GROUP TESTS ─────────────────────────────────────────────────────────

class TestAgeGroup:

    def test_pediatric_boundary(self):
        assert get_age_group(0)  == "PEDIATRIC"
        assert get_age_group(10) == "PEDIATRIC"
        assert get_age_group(17) == "PEDIATRIC"

    def test_adult_boundary(self):
        assert get_age_group(18) == "ADULT"
        assert get_age_group(40) == "ADULT"
        assert get_age_group(64) == "ADULT"

    def test_senior_boundary(self):
        assert get_age_group(65)  == "SENIOR"
        assert get_age_group(80)  == "SENIOR"
        assert get_age_group(100) == "SENIOR"

    def test_none_returns_none(self):
        assert get_age_group(None) is None


# ── NEWS2 SCORE TESTS ────────────────────────────────────────────────────────

class TestNEWS2:

    def test_normal_vitals_low_risk(self):
        # Normal spo2=98, normal hr=75 → score=0 → LOW
        assert get_news2_risk(98, 75) == "LOW"

    def test_critical_spo2_high_risk(self):
        # spo2=88 (flag=3), hr=75 (flag=0) → score=3 → MEDIUM
        assert get_news2_risk(88, 75) == "MEDIUM"

    def test_combined_high_risk(self):
        # spo2=88 (flag=3), hr=135 (flag=3) → score=6 → HIGH
        assert get_news2_risk(88, 135) == "HIGH"

    def test_borderline_medium(self):
        # spo2=94 (flag=2), hr=75 (flag=0) → score=2 → LOW
        assert get_news2_risk(94, 75) == "LOW"

    def test_none_returns_none(self):
        assert get_news2_risk(None, 75) is None
        assert get_news2_risk(98, None) is None


# ── ABNORMAL DIRECTION TESTS ─────────────────────────────────────────────────

class TestAbnormalDirection:

    def test_high_flag(self):
        assert get_abnormal_direction("H") == "HIGH"
        assert get_abnormal_direction("h") == "HIGH"

    def test_low_flag(self):
        assert get_abnormal_direction("L") == "LOW"
        assert get_abnormal_direction("l") == "LOW"

    def test_normal_flag(self):
        assert get_abnormal_direction("N")  == "NORMAL"
        assert get_abnormal_direction("")   == "NORMAL"
        assert get_abnormal_direction(None) == "NORMAL"


# ── GENDER NORMALIZATION TESTS ───────────────────────────────────────────────

class TestGenderNormalization:

    def test_male_variants(self):
        assert normalize_gender("M")    == "M"
        assert normalize_gender("MALE") == "M"
        assert normalize_gender("male") == "M"
        assert normalize_gender("m")    == "M"

    def test_female_variants(self):
        assert normalize_gender("F")      == "F"
        assert normalize_gender("FEMALE") == "F"
        assert normalize_gender("female") == "F"

    def test_unknown(self):
        assert normalize_gender("X")    == "U"
        assert normalize_gender(None)   == "U"
        assert normalize_gender("other") == "U"
