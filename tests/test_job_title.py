# tests/test_job_title.py
import pytest
from ETL.transform import _job_group, collapse_map, _job_seniority

def big_group(title: str) -> str:
    #Helper: fine -> big using your collapse_map (8 buckets).
    fine = _job_group(title)
    return collapse_map.get(fine, "other")

@pytest.mark.parametrize("title,fine,big", [
    ("Python Backend Engineer",  "backend_engineer", "software_engineering"),
    ("React Frontend Developer", "frontend_engineer","software_engineering"),
    ("Fullstack Developer",      "fullstack_engineer","software_engineering"),
    ("IT Infra Lead",            "devops_sre_cloud", "infra_cloud"),
    ("Business Analyst",         "data_analyst_bi",  "data"),
    ("QA Automation Engineer",   "qa_testing",       "qa_testing"),
])
def test_grouping(title, fine, big):
    #Detailed group should match, and collapse to correct big bucket.
    assert _job_group(title) == fine
    assert big_group(title) == big

@pytest.mark.parametrize("title,sen", [
    ("Senior Backend Engineer", "senior"),
    ("Junior QA Tester",       "junior"),
    ("Tech Lead (Platform)",   "lead"),
    ("Engineering Manager",    "manager"),
    ("Intern Data Analyst",    "intern"),
])
def test_seniority(title, sen):
    #Seniority patterns should be detected correctly.
    assert _job_seniority(title) == sen
