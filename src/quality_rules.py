"""
HL7 Data Quality Rules

Defines data quality expectations and validation rules for the DLT pipeline.
Uses Delta Live Tables @dlt.expect decorators for declarative quality enforcement.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


class QualityAction(Enum):
    """Actions to take when quality expectations fail."""
    WARN = "warn"
    DROP = "drop"
    FAIL = "fail"


@dataclass
class QualityRule:
    """Definition of a data quality rule."""
    name: str
    description: str
    constraint: str
    action: QualityAction = QualityAction.WARN
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


class HL7QualityRules:
    """Collection of quality rules for HL7 data validation."""
    
    @staticmethod
    def get_msh_rules() -> List[QualityRule]:
        """Quality rules for MSH segment."""
        return [
            QualityRule(
                name="msh_message_type_not_null",
                description="Message type must be present",
                constraint="message_type IS NOT NULL",
                action=QualityAction.DROP,
                tags=["required", "msh"]
            ),
            QualityRule(
                name="msh_control_id_not_null",
                description="Message control ID must be present",
                constraint="message_control_id IS NOT NULL",
                action=QualityAction.DROP,
                tags=["required", "msh"]
            ),
            QualityRule(
                name="msh_sending_facility_not_null",
                description="Sending facility should be present",
                constraint="sending_facility IS NOT NULL",
                action=QualityAction.WARN,
                tags=["recommended", "msh"]
            ),
            QualityRule(
                name="msh_valid_version",
                description="HL7 version should be 2.x",
                constraint="version_id LIKE '2.%'",
                action=QualityAction.WARN,
                tags=["format", "msh"]
            ),
            QualityRule(
                name="msh_datetime_not_future",
                description="Message datetime should not be in the future",
                constraint="message_datetime <= current_timestamp()",
                action=QualityAction.WARN,
                tags=["temporal", "msh"]
            ),
        ]
    
    @staticmethod
    def get_pid_rules() -> List[QualityRule]:
        """Quality rules for PID segment."""
        return [
            QualityRule(
                name="pid_patient_id_not_null",
                description="Patient ID must be present",
                constraint="patient_id IS NOT NULL AND patient_id != ''",
                action=QualityAction.DROP,
                tags=["required", "pid"]
            ),
            QualityRule(
                name="pid_patient_name_not_null",
                description="Patient name should be present",
                constraint="patient_name_family IS NOT NULL OR patient_name_given IS NOT NULL",
                action=QualityAction.WARN,
                tags=["recommended", "pid"]
            ),
            QualityRule(
                name="pid_valid_sex",
                description="Sex should be a valid code",
                constraint="sex IS NULL OR sex IN ('M', 'F', 'U', 'O', 'A', 'N')",
                action=QualityAction.WARN,
                tags=["format", "pid"]
            ),
            QualityRule(
                name="pid_dob_not_future",
                description="Date of birth should not be in the future",
                constraint="date_of_birth IS NULL OR date_of_birth <= current_date()",
                action=QualityAction.WARN,
                tags=["temporal", "pid"]
            ),
            QualityRule(
                name="pid_dob_reasonable",
                description="Date of birth should be within reasonable range (150 years)",
                constraint="date_of_birth IS NULL OR date_of_birth >= date_sub(current_date(), 54750)",
                action=QualityAction.WARN,
                tags=["temporal", "pid"]
            ),
            QualityRule(
                name="pid_valid_zip_format",
                description="ZIP code should be valid format (5 or 9 digits)",
                constraint="address_zip IS NULL OR address_zip RLIKE '^[0-9]{5}(-[0-9]{4})?$'",
                action=QualityAction.WARN,
                tags=["format", "pid"]
            ),
        ]
    
    @staticmethod
    def get_pv1_rules() -> List[QualityRule]:
        """Quality rules for PV1 segment."""
        return [
            QualityRule(
                name="pv1_patient_class_not_null",
                description="Patient class should be present",
                constraint="patient_class IS NOT NULL",
                action=QualityAction.WARN,
                tags=["recommended", "pv1"]
            ),
            QualityRule(
                name="pv1_valid_patient_class",
                description="Patient class should be valid code",
                constraint="patient_class IS NULL OR patient_class IN ('I', 'O', 'E', 'P', 'R', 'B', 'C', 'N', 'U')",
                action=QualityAction.WARN,
                tags=["format", "pv1"]
            ),
            QualityRule(
                name="pv1_admit_before_discharge",
                description="Admit datetime should be before discharge datetime",
                constraint="discharge_datetime IS NULL OR admit_datetime IS NULL OR admit_datetime <= discharge_datetime",
                action=QualityAction.WARN,
                tags=["temporal", "pv1"]
            ),
            QualityRule(
                name="pv1_admit_not_future",
                description="Admit datetime should not be in the future",
                constraint="admit_datetime IS NULL OR admit_datetime <= current_timestamp()",
                action=QualityAction.WARN,
                tags=["temporal", "pv1"]
            ),
        ]
    
    @staticmethod
    def get_obx_rules() -> List[QualityRule]:
        """Quality rules for OBX segment."""
        return [
            QualityRule(
                name="obx_observation_id_not_null",
                description="Observation ID must be present",
                constraint="observation_id IS NOT NULL AND observation_id != ''",
                action=QualityAction.DROP,
                tags=["required", "obx"]
            ),
            QualityRule(
                name="obx_value_type_not_null",
                description="Value type should be present",
                constraint="value_type IS NOT NULL",
                action=QualityAction.WARN,
                tags=["recommended", "obx"]
            ),
            QualityRule(
                name="obx_valid_value_type",
                description="Value type should be valid HL7 code",
                constraint="value_type IS NULL OR value_type IN ('NM', 'ST', 'TX', 'CE', 'DT', 'TM', 'TS', 'FT', 'ED', 'SN', 'CWE')",
                action=QualityAction.WARN,
                tags=["format", "obx"]
            ),
            QualityRule(
                name="obx_numeric_has_units",
                description="Numeric observations should have units",
                constraint="value_type != 'NM' OR units IS NOT NULL",
                action=QualityAction.WARN,
                tags=["completeness", "obx"]
            ),
            QualityRule(
                name="obx_valid_result_status",
                description="Result status should be valid HL7 code",
                constraint="observation_result_status IS NULL OR observation_result_status IN ('C', 'D', 'F', 'I', 'N', 'O', 'P', 'R', 'S', 'U', 'W', 'X')",
                action=QualityAction.WARN,
                tags=["format", "obx"]
            ),
        ]
    
    @staticmethod
    def get_dg1_rules() -> List[QualityRule]:
        """Quality rules for DG1 segment."""
        return [
            QualityRule(
                name="dg1_code_not_null",
                description="Diagnosis code must be present",
                constraint="diagnosis_code IS NOT NULL AND diagnosis_code != ''",
                action=QualityAction.DROP,
                tags=["required", "dg1"]
            ),
            QualityRule(
                name="dg1_valid_coding_system",
                description="Coding system should be specified",
                constraint="diagnosis_coding_system IS NOT NULL",
                action=QualityAction.WARN,
                tags=["recommended", "dg1"]
            ),
            QualityRule(
                name="dg1_icd10_format",
                description="ICD-10 codes should match format",
                constraint="diagnosis_coding_system != 'ICD10' OR diagnosis_code RLIKE '^[A-Z][0-9]{2}(\\.[0-9A-Z]{1,4})?$'",
                action=QualityAction.WARN,
                tags=["format", "dg1"]
            ),
        ]
    
    @staticmethod
    def get_all_rules() -> Dict[str, List[QualityRule]]:
        """Get all quality rules organized by segment."""
        return {
            "MSH": HL7QualityRules.get_msh_rules(),
            "PID": HL7QualityRules.get_pid_rules(),
            "PV1": HL7QualityRules.get_pv1_rules(),
            "OBX": HL7QualityRules.get_obx_rules(),
            "DG1": HL7QualityRules.get_dg1_rules(),
        }
    
    @staticmethod
    def get_rules_by_action(action: QualityAction) -> List[QualityRule]:
        """Get all rules with a specific action type."""
        all_rules = HL7QualityRules.get_all_rules()
        result = []
        for rules in all_rules.values():
            result.extend([r for r in rules if r.action == action])
        return result
    
    @staticmethod
    def get_rules_by_tag(tag: str) -> List[QualityRule]:
        """Get all rules with a specific tag."""
        all_rules = HL7QualityRules.get_all_rules()
        result = []
        for rules in all_rules.values():
            result.extend([r for r in rules if tag in r.tags])
        return result


def generate_dlt_expectations(rules: List[QualityRule]) -> Dict[str, str]:
    """
    Generate DLT expectation dictionary from quality rules.
    
    This can be used with @dlt.expect_all() or @dlt.expect_all_or_drop()
    """
    return {rule.name: rule.constraint for rule in rules}


def generate_dlt_expect_decorator_code(segment: str) -> str:
    """
    Generate Python code for DLT expect decorators.
    
    This is a helper for generating boilerplate code.
    """
    rules = HL7QualityRules.get_all_rules().get(segment, [])
    
    code_lines = []
    for rule in rules:
        if rule.action == QualityAction.DROP:
            code_lines.append(f'@dlt.expect_or_drop("{rule.name}", "{rule.constraint}")')
        elif rule.action == QualityAction.FAIL:
            code_lines.append(f'@dlt.expect_or_fail("{rule.name}", "{rule.constraint}")')
        else:
            code_lines.append(f'@dlt.expect("{rule.name}", "{rule.constraint}")')
    
    return "\n".join(code_lines)
