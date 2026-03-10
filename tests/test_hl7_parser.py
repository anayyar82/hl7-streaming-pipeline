"""
Unit tests for HL7 Parser

Tests parsing functionality for various HL7 message types and segments.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.hl7_parser import HL7Parser, parse_hl7_message


class TestHL7Parser:
    """Test cases for HL7Parser class."""
    
    @pytest.fixture
    def parser(self):
        """Create parser instance."""
        return HL7Parser()
    
    @pytest.fixture
    def sample_adt_a01(self):
        """Sample ADT A01 message."""
        return (
            "MSH|^~\\&|SENDING_APP|SENDING_FACILITY|RECEIVING_APP|RECEIVING_FACILITY|"
            "20240115120000||ADT^A01|MSG00001|P|2.5.1\r"
            "EVN|A01|20240115120000\r"
            "PID|1||PAT001^^^HOSPITAL^MR||DOE^JOHN^MICHAEL||19800515|M|||"
            "123 MAIN ST^^ANYTOWN^CA^90210||555-123-4567|||M\r"
            "PV1|1|I|ICU^101^A^HOSPITAL||||1234^SMITH^JANE|||MED||||||||"
            "V00001|||||||||||||||||||||||||20240115100000"
        )
    
    @pytest.fixture
    def sample_oru_r01(self):
        """Sample ORU R01 message with lab results."""
        return (
            "MSH|^~\\&|LAB|HOSPITAL|EMR|HOSPITAL|20240115130000||ORU^R01|MSG00002|P|2.5.1\r"
            "PID|1||PAT002^^^HOSPITAL^MR||SMITH^JANE||19750320|F\r"
            "OBR|1|ORD001|FIL001|80053^COMPREHENSIVE METABOLIC PANEL|||20240115080000\r"
            "OBX|1|NM|2345-7^GLUCOSE^LN||95|mg/dL|70-100|N|||F|||20240115090000\r"
            "OBX|2|NM|2160-0^CREATININE^LN||1.2|mg/dL|0.7-1.3|N|||F|||20240115090000\r"
            "OBX|3|NM|3094-0^BUN^LN||18|mg/dL|7-20|N|||F|||20240115090000"
        )
    
    def test_parse_empty_message(self, parser):
        """Test parsing empty message."""
        result = parser.parse("")
        assert result["error"] == "Empty message"
    
    def test_parse_none_message(self, parser):
        """Test parsing None message."""
        result = parser.parse(None)
        assert result["error"] == "Empty message"
    
    def test_parse_msh_segment(self, parser, sample_adt_a01):
        """Test MSH segment parsing."""
        result = parser.parse(sample_adt_a01)
        
        assert "MSH" in result["segments"]
        msh = result["segments"]["MSH"]
        
        assert msh["sending_application"] == "SENDING_APP"
        assert msh["sending_facility"] == "SENDING_FACILITY"
        assert msh["receiving_application"] == "RECEIVING_APP"
        assert msh["receiving_facility"] == "RECEIVING_FACILITY"
        assert msh["message_type"] == "ADT"
        assert msh["trigger_event"] == "A01"
        assert msh["message_control_id"] == "MSG00001"
        assert msh["processing_id"] == "P"
        assert msh["version_id"] == "2.5.1"
    
    def test_parse_pid_segment(self, parser, sample_adt_a01):
        """Test PID segment parsing."""
        result = parser.parse(sample_adt_a01)
        
        assert "PID" in result["segments"]
        pid = result["segments"]["PID"]
        
        assert pid["patient_id"] == "PAT001"
        assert pid["patient_id_authority"] == "HOSPITAL"
        assert pid["patient_name_family"] == "DOE"
        assert pid["patient_name_given"] == "JOHN"
        assert pid["patient_name_middle"] == "MICHAEL"
        assert pid["sex"] == "M"
        assert pid["address_street"] == "123 MAIN ST"
        assert pid["address_city"] == "ANYTOWN"
        assert pid["address_state"] == "CA"
        assert pid["address_zip"] == "90210"
    
    def test_parse_pv1_segment(self, parser, sample_adt_a01):
        """Test PV1 segment parsing."""
        result = parser.parse(sample_adt_a01)
        
        assert "PV1" in result["segments"]
        pv1 = result["segments"]["PV1"]
        
        assert pv1["patient_class"] == "I"
        assert pv1["assigned_location_point_of_care"] == "ICU"
        assert pv1["assigned_location_room"] == "101"
        assert pv1["assigned_location_bed"] == "A"
        assert pv1["attending_doctor_id"] == "1234"
        assert pv1["attending_doctor_family"] == "SMITH"
        assert pv1["visit_number"] == "V00001"
    
    def test_parse_obx_segments(self, parser, sample_oru_r01):
        """Test OBX segment parsing (multiple)."""
        result = parser.parse(sample_oru_r01)
        
        assert "OBX" in result["segments"]
        obx_list = result["segments"]["OBX"]
        
        assert len(obx_list) == 3
        
        glucose = obx_list[0]
        assert glucose["observation_id"] == "2345-7"
        assert glucose["observation_text"] == "GLUCOSE"
        assert glucose["observation_value"] == "95"
        assert glucose["units"] == "mg/dL"
        assert glucose["reference_range"] == "70-100"
        assert glucose["abnormal_flags"] == "N"
        assert glucose["value_type"] == "NM"
    
    def test_parse_obr_segment(self, parser, sample_oru_r01):
        """Test OBR segment parsing."""
        result = parser.parse(sample_oru_r01)
        
        assert "OBR" in result["segments"]
        obr = result["segments"]["OBR"]
        
        assert obr["placer_order_number"] == "ORD001"
        assert obr["filler_order_number"] == "FIL001"
        assert obr["universal_service_id"] == "80053"
        assert obr["universal_service_text"] == "COMPREHENSIVE METABOLIC PANEL"
    
    def test_metadata_extraction(self, parser, sample_adt_a01):
        """Test message-level metadata extraction."""
        result = parser.parse(sample_adt_a01)
        
        assert result["message_type"] == "ADT"
        assert result["message_control_id"] == "MSG00001"
        assert result["sending_facility"] == "SENDING_FACILITY"
        assert result["segment_count"] > 0
        assert "parsed_at" in result
    
    def test_datetime_parsing(self, parser):
        """Test HL7 datetime parsing."""
        assert parser._parse_datetime("20240115120000") is not None
        assert parser._parse_datetime("202401151200") is not None
        assert parser._parse_datetime("20240115") is not None
        assert parser._parse_datetime("") is None
        assert parser._parse_datetime(None) is None
    
    def test_convenience_function(self, sample_adt_a01):
        """Test parse_hl7_message convenience function."""
        result = parse_hl7_message(sample_adt_a01)
        
        assert "MSH" in result["segments"]
        assert result["message_type"] == "ADT"


class TestHL7ParserEdgeCases:
    """Test edge cases and error handling."""
    
    @pytest.fixture
    def parser(self):
        return HL7Parser()
    
    def test_malformed_segment(self, parser):
        """Test handling of malformed segments."""
        message = "MSH|^~\\&|APP||||\rBADSEGMENT\rPID|1||PAT001"
        result = parser.parse(message)
        
        assert "OTHER" in result["segments"] or len(result["errors"]) > 0 or "PID" in result["segments"]
    
    def test_missing_fields(self, parser):
        """Test handling of messages with missing fields."""
        message = "MSH|^~\\&|APP\rPID|1"
        result = parser.parse(message)
        
        assert result is not None
        assert len(result["errors"]) == 0 or "MSH" in result["segments"]
    
    def test_newline_variations(self, parser):
        """Test handling of different line endings."""
        message_cr = "MSH|^~\\&|APP||||\rPID|1||PAT001"
        message_lf = "MSH|^~\\&|APP||||\nPID|1||PAT001"
        message_crlf = "MSH|^~\\&|APP||||\r\nPID|1||PAT001"
        
        result_cr = parser.parse(message_cr)
        result_lf = parser.parse(message_lf)
        result_crlf = parser.parse(message_crlf)
        
        assert "MSH" in result_cr["segments"]
        assert "MSH" in result_lf["segments"]
        assert "MSH" in result_crlf["segments"]


class TestHL7ParserMessageTypes:
    """Test different HL7 message types."""
    
    @pytest.fixture
    def parser(self):
        return HL7Parser()
    
    def test_adt_a08_update(self, parser):
        """Test ADT A08 (Update Patient Information)."""
        message = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A08|MSG003|P|2.5\r"
            "PID|1||PAT003||JOHNSON^MARY||19900101|F"
        )
        result = parser.parse(message)
        
        assert result["segments"]["MSH"]["message_type"] == "ADT"
        assert result["segments"]["MSH"]["trigger_event"] == "A08"
    
    def test_orm_o01_order(self, parser):
        """Test ORM O01 (Order Message)."""
        message = (
            "MSH|^~\\&|CPOE|HOSPITAL|||20240115||ORM^O01|MSG004|P|2.5\r"
            "PID|1||PAT004||WILLIAMS^BOB||19850610|M\r"
            "ORC|NW|ORD004||||||20240115\r"
            "OBR|1|ORD004||CBC^Complete Blood Count"
        )
        result = parser.parse(message)
        
        assert result["segments"]["MSH"]["message_type"] == "ORM"
        assert "ORC" in result["segments"]
        assert "OBR" in result["segments"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
