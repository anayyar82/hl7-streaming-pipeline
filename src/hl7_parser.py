"""
HL7 v2.x Message Parser

Parses HL7 messages into structured segments for processing in Delta Live Tables.
Supports common message types: ADT, ORU, ORM, SIU
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import re


class HL7Parser:
    """Parser for HL7 v2.x messages."""
    
    SEGMENT_DELIMITER = "\r"
    FIELD_DELIMITER = "|"
    COMPONENT_DELIMITER = "^"
    SUBCOMPONENT_DELIMITER = "&"
    REPETITION_DELIMITER = "~"
    
    def __init__(self):
        self.segment_parsers = {
            "MSH": self._parse_msh,
            "PID": self._parse_pid,
            "PV1": self._parse_pv1,
            "OBR": self._parse_obr,
            "OBX": self._parse_obx,
            "DG1": self._parse_dg1,
            "IN1": self._parse_in1,
            "NK1": self._parse_nk1,
            "EVN": self._parse_evn,
            "ORC": self._parse_orc,
        }
    
    def parse(self, raw_message: str) -> Dict[str, Any]:
        """
        Parse a complete HL7 message into structured segments.
        
        Args:
            raw_message: Raw HL7 message string
            
        Returns:
            Dictionary containing parsed segments and metadata
        """
        if not raw_message:
            return {"error": "Empty message", "raw": raw_message}
        
        raw_message = raw_message.replace("\n", "\r").replace("\r\r", "\r")
        segments = raw_message.strip().split(self.SEGMENT_DELIMITER)
        
        result = {
            "raw_message": raw_message,
            "segment_count": len(segments),
            "parsed_at": datetime.utcnow().isoformat(),
            "segments": {},
            "errors": []
        }
        
        for segment in segments:
            if not segment.strip():
                continue
                
            segment_type = segment[:3]
            
            try:
                if segment_type in self.segment_parsers:
                    parsed = self.segment_parsers[segment_type](segment)
                    
                    if segment_type in ["OBX", "DG1", "IN1", "NK1"]:
                        if segment_type not in result["segments"]:
                            result["segments"][segment_type] = []
                        result["segments"][segment_type].append(parsed)
                    else:
                        result["segments"][segment_type] = parsed
                else:
                    if "OTHER" not in result["segments"]:
                        result["segments"]["OTHER"] = []
                    result["segments"]["OTHER"].append({
                        "segment_type": segment_type,
                        "raw": segment
                    })
            except Exception as e:
                result["errors"].append({
                    "segment_type": segment_type,
                    "error": str(e),
                    "raw": segment
                })
        
        if "MSH" in result["segments"]:
            result["message_type"] = result["segments"]["MSH"].get("message_type")
            result["message_control_id"] = result["segments"]["MSH"].get("message_control_id")
            result["sending_facility"] = result["segments"]["MSH"].get("sending_facility")
        
        return result
    
    def _get_field(self, fields: List[str], index: int, default: str = "") -> str:
        """Safely get a field by index."""
        try:
            return fields[index] if index < len(fields) else default
        except (IndexError, TypeError):
            return default
    
    def _get_component(self, field: str, index: int, default: str = "") -> str:
        """Get a component from a field."""
        components = field.split(self.COMPONENT_DELIMITER)
        return self._get_field(components, index, default)
    
    def _parse_datetime(self, hl7_datetime: str) -> Optional[str]:
        """Parse HL7 datetime format (YYYYMMDDHHMMSS) to ISO format."""
        if not hl7_datetime:
            return None
        
        hl7_datetime = hl7_datetime.split("+")[0].split("-")[0]
        
        formats = [
            ("%Y%m%d%H%M%S", 14),
            ("%Y%m%d%H%M", 12),
            ("%Y%m%d", 8),
        ]
        
        for fmt, length in formats:
            if len(hl7_datetime) >= length:
                try:
                    dt = datetime.strptime(hl7_datetime[:length], fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
        
        return hl7_datetime
    
    def _parse_msh(self, segment: str) -> Dict[str, Any]:
        """Parse MSH (Message Header) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        message_type_field = self._get_field(fields, 8)
        message_type = self._get_component(message_type_field, 0)
        trigger_event = self._get_component(message_type_field, 1)
        
        return {
            "segment_type": "MSH",
            "encoding_characters": self._get_field(fields, 1),
            "sending_application": self._get_field(fields, 2),
            "sending_facility": self._get_field(fields, 3),
            "receiving_application": self._get_field(fields, 4),
            "receiving_facility": self._get_field(fields, 5),
            "message_datetime": self._parse_datetime(self._get_field(fields, 6)),
            "security": self._get_field(fields, 7),
            "message_type": message_type,
            "trigger_event": trigger_event,
            "message_type_full": f"{message_type}_{trigger_event}" if trigger_event else message_type,
            "message_control_id": self._get_field(fields, 9),
            "processing_id": self._get_field(fields, 10),
            "version_id": self._get_field(fields, 11),
        }
    
    def _parse_pid(self, segment: str) -> Dict[str, Any]:
        """Parse PID (Patient Identification) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        patient_id_field = self._get_field(fields, 3)
        patient_name_field = self._get_field(fields, 5)
        patient_address_field = self._get_field(fields, 11)
        
        return {
            "segment_type": "PID",
            "set_id": self._get_field(fields, 1),
            "patient_id_external": self._get_field(fields, 2),
            "patient_id": self._get_component(patient_id_field, 0),
            "patient_id_authority": self._get_component(patient_id_field, 3),
            "alternate_patient_id": self._get_field(fields, 4),
            "patient_name_family": self._get_component(patient_name_field, 0),
            "patient_name_given": self._get_component(patient_name_field, 1),
            "patient_name_middle": self._get_component(patient_name_field, 2),
            "patient_name_suffix": self._get_component(patient_name_field, 3),
            "date_of_birth": self._parse_datetime(self._get_field(fields, 7)),
            "sex": self._get_field(fields, 8),
            "patient_alias": self._get_field(fields, 9),
            "race": self._get_field(fields, 10),
            "address_street": self._get_component(patient_address_field, 0),
            "address_city": self._get_component(patient_address_field, 2),
            "address_state": self._get_component(patient_address_field, 3),
            "address_zip": self._get_component(patient_address_field, 4),
            "address_country": self._get_component(patient_address_field, 5),
            "phone_home": self._get_field(fields, 13),
            "phone_business": self._get_field(fields, 14),
            "language": self._get_field(fields, 15),
            "marital_status": self._get_field(fields, 16),
            "religion": self._get_field(fields, 17),
            "patient_account_number": self._get_field(fields, 18),
            "ssn": self._get_field(fields, 19),
            "drivers_license": self._get_field(fields, 20),
            "ethnicity": self._get_field(fields, 22),
            "death_indicator": self._get_field(fields, 30),
        }
    
    def _parse_pv1(self, segment: str) -> Dict[str, Any]:
        """Parse PV1 (Patient Visit) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        attending_doctor_field = self._get_field(fields, 7)
        assigned_location_field = self._get_field(fields, 3)
        
        return {
            "segment_type": "PV1",
            "set_id": self._get_field(fields, 1),
            "patient_class": self._get_field(fields, 2),
            "assigned_location_point_of_care": self._get_component(assigned_location_field, 0),
            "assigned_location_room": self._get_component(assigned_location_field, 1),
            "assigned_location_bed": self._get_component(assigned_location_field, 2),
            "assigned_location_facility": self._get_component(assigned_location_field, 3),
            "admission_type": self._get_field(fields, 4),
            "preadmit_number": self._get_field(fields, 5),
            "prior_location": self._get_field(fields, 6),
            "attending_doctor_id": self._get_component(attending_doctor_field, 0),
            "attending_doctor_family": self._get_component(attending_doctor_field, 1),
            "attending_doctor_given": self._get_component(attending_doctor_field, 2),
            "hospital_service": self._get_field(fields, 10),
            "readmission_indicator": self._get_field(fields, 13),
            "admit_source": self._get_field(fields, 14),
            "ambulatory_status": self._get_field(fields, 15),
            "vip_indicator": self._get_field(fields, 16),
            "admitting_doctor": self._get_field(fields, 17),
            "patient_type": self._get_field(fields, 18),
            "visit_number": self._get_field(fields, 19),
            "financial_class": self._get_field(fields, 20),
            "discharge_disposition": self._get_field(fields, 36),
            "admit_datetime": self._parse_datetime(self._get_field(fields, 44)),
            "discharge_datetime": self._parse_datetime(self._get_field(fields, 45)),
        }
    
    def _parse_obr(self, segment: str) -> Dict[str, Any]:
        """Parse OBR (Observation Request) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        universal_service_field = self._get_field(fields, 4)
        ordering_provider_field = self._get_field(fields, 16)
        
        return {
            "segment_type": "OBR",
            "set_id": self._get_field(fields, 1),
            "placer_order_number": self._get_field(fields, 2),
            "filler_order_number": self._get_field(fields, 3),
            "universal_service_id": self._get_component(universal_service_field, 0),
            "universal_service_text": self._get_component(universal_service_field, 1),
            "universal_service_coding_system": self._get_component(universal_service_field, 2),
            "priority": self._get_field(fields, 5),
            "requested_datetime": self._parse_datetime(self._get_field(fields, 6)),
            "observation_datetime": self._parse_datetime(self._get_field(fields, 7)),
            "observation_end_datetime": self._parse_datetime(self._get_field(fields, 8)),
            "collection_volume": self._get_field(fields, 9),
            "collector_identifier": self._get_field(fields, 10),
            "specimen_action_code": self._get_field(fields, 11),
            "ordering_provider_id": self._get_component(ordering_provider_field, 0),
            "ordering_provider_family": self._get_component(ordering_provider_field, 1),
            "ordering_provider_given": self._get_component(ordering_provider_field, 2),
            "result_status": self._get_field(fields, 25),
            "reason_for_study": self._get_field(fields, 31),
        }
    
    def _parse_obx(self, segment: str) -> Dict[str, Any]:
        """Parse OBX (Observation/Result) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        observation_id_field = self._get_field(fields, 3)
        
        return {
            "segment_type": "OBX",
            "set_id": self._get_field(fields, 1),
            "value_type": self._get_field(fields, 2),
            "observation_id": self._get_component(observation_id_field, 0),
            "observation_text": self._get_component(observation_id_field, 1),
            "observation_coding_system": self._get_component(observation_id_field, 2),
            "observation_sub_id": self._get_field(fields, 4),
            "observation_value": self._get_field(fields, 5),
            "units": self._get_field(fields, 6),
            "reference_range": self._get_field(fields, 7),
            "abnormal_flags": self._get_field(fields, 8),
            "probability": self._get_field(fields, 9),
            "nature_of_abnormal_test": self._get_field(fields, 10),
            "observation_result_status": self._get_field(fields, 11),
            "observation_datetime": self._parse_datetime(self._get_field(fields, 14)),
            "producer_id": self._get_field(fields, 15),
            "responsible_observer": self._get_field(fields, 16),
        }
    
    def _parse_dg1(self, segment: str) -> Dict[str, Any]:
        """Parse DG1 (Diagnosis) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        diagnosis_code_field = self._get_field(fields, 3)
        
        return {
            "segment_type": "DG1",
            "set_id": self._get_field(fields, 1),
            "diagnosis_coding_method": self._get_field(fields, 2),
            "diagnosis_code": self._get_component(diagnosis_code_field, 0),
            "diagnosis_description": self._get_component(diagnosis_code_field, 1),
            "diagnosis_coding_system": self._get_component(diagnosis_code_field, 2),
            "diagnosis_datetime": self._parse_datetime(self._get_field(fields, 5)),
            "diagnosis_type": self._get_field(fields, 6),
            "major_diagnostic_category": self._get_field(fields, 7),
            "diagnostic_related_group": self._get_field(fields, 8),
            "drg_approval_indicator": self._get_field(fields, 9),
            "drg_grouper_review_code": self._get_field(fields, 10),
            "diagnosis_priority": self._get_field(fields, 15),
            "diagnosing_clinician": self._get_field(fields, 16),
        }
    
    def _parse_in1(self, segment: str) -> Dict[str, Any]:
        """Parse IN1 (Insurance) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        insurance_company_id_field = self._get_field(fields, 3)
        
        return {
            "segment_type": "IN1",
            "set_id": self._get_field(fields, 1),
            "insurance_plan_id": self._get_field(fields, 2),
            "insurance_company_id": self._get_component(insurance_company_id_field, 0),
            "insurance_company_name": self._get_field(fields, 4),
            "insurance_company_address": self._get_field(fields, 5),
            "insurance_company_contact": self._get_field(fields, 6),
            "group_number": self._get_field(fields, 8),
            "group_name": self._get_field(fields, 9),
            "insured_group_emp_id": self._get_field(fields, 10),
            "insured_group_emp_name": self._get_field(fields, 11),
            "plan_effective_date": self._parse_datetime(self._get_field(fields, 12)),
            "plan_expiration_date": self._parse_datetime(self._get_field(fields, 13)),
            "authorization_info": self._get_field(fields, 14),
            "plan_type": self._get_field(fields, 15),
            "insured_name": self._get_field(fields, 16),
            "insured_relationship": self._get_field(fields, 17),
            "insured_dob": self._parse_datetime(self._get_field(fields, 18)),
            "policy_number": self._get_field(fields, 36),
        }
    
    def _parse_nk1(self, segment: str) -> Dict[str, Any]:
        """Parse NK1 (Next of Kin) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        name_field = self._get_field(fields, 2)
        address_field = self._get_field(fields, 4)
        
        return {
            "segment_type": "NK1",
            "set_id": self._get_field(fields, 1),
            "name_family": self._get_component(name_field, 0),
            "name_given": self._get_component(name_field, 1),
            "relationship": self._get_field(fields, 3),
            "address_street": self._get_component(address_field, 0),
            "address_city": self._get_component(address_field, 2),
            "address_state": self._get_component(address_field, 3),
            "address_zip": self._get_component(address_field, 4),
            "phone_home": self._get_field(fields, 5),
            "phone_business": self._get_field(fields, 6),
            "contact_role": self._get_field(fields, 7),
        }
    
    def _parse_evn(self, segment: str) -> Dict[str, Any]:
        """Parse EVN (Event Type) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        return {
            "segment_type": "EVN",
            "event_type_code": self._get_field(fields, 1),
            "recorded_datetime": self._parse_datetime(self._get_field(fields, 2)),
            "planned_event_datetime": self._parse_datetime(self._get_field(fields, 3)),
            "event_reason_code": self._get_field(fields, 4),
            "operator_id": self._get_field(fields, 5),
            "event_occurred": self._parse_datetime(self._get_field(fields, 6)),
        }
    
    def _parse_orc(self, segment: str) -> Dict[str, Any]:
        """Parse ORC (Common Order) segment."""
        fields = segment.split(self.FIELD_DELIMITER)
        
        ordering_provider_field = self._get_field(fields, 12)
        
        return {
            "segment_type": "ORC",
            "order_control": self._get_field(fields, 1),
            "placer_order_number": self._get_field(fields, 2),
            "filler_order_number": self._get_field(fields, 3),
            "placer_group_number": self._get_field(fields, 4),
            "order_status": self._get_field(fields, 5),
            "response_flag": self._get_field(fields, 6),
            "quantity_timing": self._get_field(fields, 7),
            "parent_order": self._get_field(fields, 8),
            "transaction_datetime": self._parse_datetime(self._get_field(fields, 9)),
            "entered_by": self._get_field(fields, 10),
            "verified_by": self._get_field(fields, 11),
            "ordering_provider_id": self._get_component(ordering_provider_field, 0),
            "ordering_provider_family": self._get_component(ordering_provider_field, 1),
            "ordering_provider_given": self._get_component(ordering_provider_field, 2),
            "enterer_location": self._get_field(fields, 13),
            "order_effective_datetime": self._parse_datetime(self._get_field(fields, 15)),
        }


def parse_hl7_message(raw_message: str) -> Dict[str, Any]:
    """
    Convenience function for parsing HL7 messages.
    
    Args:
        raw_message: Raw HL7 message string
        
    Returns:
        Parsed message dictionary
    """
    parser = HL7Parser()
    return parser.parse(raw_message)
