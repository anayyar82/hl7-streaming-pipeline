"""
Unit tests for HL7 Message Splitter

Tests the splitting logic that separates multi-message files
into individual HL7 messages for parallel processing.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def split_hl7_messages(raw_content):
    """
    Local copy of the splitter UDF logic for unit testing.
    (The actual UDF lives in 01_bronze.py as a Spark UDF.)
    """
    if not raw_content:
        return []

    content = raw_content.replace("\x0b", "").replace("\x1c\x0d", "\n").replace("\x1c", "\n")
    content = content.replace("\r\n", "\n").replace("\r", "\n")

    lines = content.split("\n")

    messages = []
    current_message_lines = []

    for line in lines:
        stripped = line.strip()

        if not stripped:
            if current_message_lines:
                messages.append("\r".join(current_message_lines))
                current_message_lines = []
            continue

        if stripped.startswith("MSH|") and current_message_lines:
            messages.append("\r".join(current_message_lines))
            current_message_lines = [stripped]
        else:
            current_message_lines.append(stripped)

    if current_message_lines:
        messages.append("\r".join(current_message_lines))

    return [m for m in messages if m.startswith("MSH|")]


class TestSingleMessage:
    """Files containing exactly one HL7 message."""

    def test_single_message(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\r"
            "PID|1||PAT001||DOE^JOHN||19800515|M\r"
            "PV1|1|I|ICU^101^A"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 1
        assert result[0].startswith("MSH|")
        assert "PID|1||PAT001" in result[0]

    def test_single_message_with_trailing_newlines(self):
        raw = "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\nPID|1||PAT001\n\n\n"
        result = split_hl7_messages(raw)
        assert len(result) == 1


class TestBlankLineSeparated:
    """Messages separated by blank lines."""

    def test_two_messages_blank_line(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "PID|1||PAT001||DOE^JOHN||19800515|M\n"
            "\n"
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG002|P|2.5\n"
            "PID|1||PAT002||SMITH^JANE||19900101|F\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 2
        assert "MSG001" in result[0]
        assert "MSG002" in result[1]

    def test_multiple_blank_lines_between_messages(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "PID|1||PAT001\n"
            "\n\n\n"
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG002|P|2.5\n"
            "PID|1||PAT002\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 2

    def test_five_messages_blank_separated(self):
        messages = []
        for i in range(5):
            messages.append(
                f"MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG{i:03d}|P|2.5\n"
                f"PID|1||PAT{i:03d}||NAME{i}^FIRST||19800101|M\n"
            )
        raw = "\n".join(messages)
        result = split_hl7_messages(raw)
        assert len(result) == 5


class TestConsecutiveMSH:
    """Messages with no separator — back-to-back MSH segments."""

    def test_two_messages_no_separator(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "PID|1||PAT001||DOE^JOHN||19800515|M\n"
            "PV1|1|I|ICU^101\n"
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG002|P|2.5\n"
            "PID|1||PAT002||SMITH^JANE||19900101|F\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 2
        assert "PAT001" in result[0]
        assert "PAT002" in result[1]

    def test_three_messages_no_separator(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "PID|1||PAT001\n"
            "MSH|^~\\&|APP|FAC|||20240115||ORU^R01|MSG002|P|2.5\n"
            "PID|1||PAT002\n"
            "OBX|1|NM|GLU||95|mg/dL\n"
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A03|MSG003|P|2.5\n"
            "PID|1||PAT003\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 3
        assert "OBX|1|NM|GLU" in result[1]


class TestMLLPFraming:
    """Messages with MLLP framing characters (\x0b start, \x1c\x0d end)."""

    def test_mllp_single_message(self):
        raw = (
            "\x0bMSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\r"
            "PID|1||PAT001\x1c\x0d"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 1
        assert result[0].startswith("MSH|")

    def test_mllp_two_messages(self):
        raw = (
            "\x0bMSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\r"
            "PID|1||PAT001\x1c\x0d"
            "\x0bMSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG002|P|2.5\r"
            "PID|1||PAT002\x1c\x0d"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 2

    def test_mllp_with_fs_only(self):
        """Some systems only use FS (\x1c) without the trailing CR."""
        raw = (
            "\x0bMSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\r"
            "PID|1||PAT001\x1c"
            "\x0bMSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG002|P|2.5\r"
            "PID|1||PAT002\x1c"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 2


class TestLineEndings:
    """Different line ending styles."""

    def test_cr_only(self):
        raw = "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\rPID|1||PAT001\rPV1|1|I"
        result = split_hl7_messages(raw)
        assert len(result) == 1
        assert "PV1|1|I" in result[0]

    def test_lf_only(self):
        raw = "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\nPID|1||PAT001\nPV1|1|I"
        result = split_hl7_messages(raw)
        assert len(result) == 1

    def test_crlf(self):
        raw = "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\r\nPID|1||PAT001\r\nPV1|1|I"
        result = split_hl7_messages(raw)
        assert len(result) == 1

    def test_mixed_endings_multi_message(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\r\n"
            "PID|1||PAT001\r\n"
            "\r\n"
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG002|P|2.5\r"
            "PID|1||PAT002\r"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 2


class TestEdgeCases:
    """Edge cases and error handling."""

    def test_empty_string(self):
        assert split_hl7_messages("") == []

    def test_none(self):
        assert split_hl7_messages(None) == []

    def test_no_msh(self):
        raw = "PID|1||PAT001||DOE^JOHN\nPV1|1|I"
        result = split_hl7_messages(raw)
        assert len(result) == 0

    def test_garbage_before_msh(self):
        raw = (
            "some random header text\n"
            "another line\n"
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "PID|1||PAT001\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 1
        assert result[0].startswith("MSH|")

    def test_msh_in_field_value_not_treated_as_separator(self):
        """MSH appearing inside a field value should NOT split the message."""
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "PID|1||PAT001||DOE^JOHN\n"
            "NTE|1||Patient note: refer to MSH protocol document\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 1

    def test_preserves_segment_order(self):
        raw = (
            "MSH|^~\\&|APP|FAC|||20240115||ADT^A01|MSG001|P|2.5\n"
            "EVN|A01|20240115\n"
            "PID|1||PAT001\n"
            "PV1|1|I|ICU^101\n"
            "DG1|1|ICD10|R07.9^CHEST PAIN\n"
        )
        result = split_hl7_messages(raw)
        assert len(result) == 1
        segments = result[0].split("\r")
        assert segments[0].startswith("MSH|")
        assert segments[1].startswith("EVN|")
        assert segments[2].startswith("PID|")
        assert segments[3].startswith("PV1|")
        assert segments[4].startswith("DG1|")


class TestSampleFiles:
    """Test with actual sample files from the test data directory."""

    @pytest.fixture
    def sample_dir(self):
        return os.path.join(os.path.dirname(__file__), "sample_messages")

    def test_multi_message_file(self, sample_dir):
        filepath = os.path.join(sample_dir, "sample_batch_multi_message.hl7")
        with open(filepath, "r") as f:
            content = f.read()
        result = split_hl7_messages(content)
        assert len(result) == 5

        msg_types = []
        for msg in result:
            lines = msg.split("\r")
            msh = lines[0]
            fields = msh.split("|")
            msg_type = fields[8] if len(fields) > 8 else ""
            msg_types.append(msg_type)

        assert msg_types[0] == "ADT^A01"
        assert msg_types[1] == "ADT^A01"
        assert msg_types[2] == "ORU^R01"
        assert msg_types[3] == "ADT^A03"
        assert msg_types[4] == "ADT^A08"

    def test_mllp_style_file(self, sample_dir):
        filepath = os.path.join(sample_dir, "sample_mllp_framed.hl7")
        with open(filepath, "r") as f:
            content = f.read()
        result = split_hl7_messages(content)
        assert len(result) == 3

    def test_single_message_file(self, sample_dir):
        filepath = os.path.join(sample_dir, "sample_adt_a01.hl7")
        with open(filepath, "r") as f:
            content = f.read()
        result = split_hl7_messages(content)
        assert len(result) == 1
        assert "MRN123456" in result[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
