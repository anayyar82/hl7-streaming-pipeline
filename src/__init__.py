"""
HL7 Streaming Pipeline Source Package

This package contains utilities for parsing and transforming HL7 v2.x messages
in a Databricks Delta Live Tables pipeline.

Primary parsing is handled by the funke library:
    from funke.parsing.functions import parse_hl7v2_msg
    from funke.parsing.hl7 import HL7v2Schema

The local HL7Parser class is provided for local testing and fallback scenarios.
"""

from .hl7_parser import HL7Parser
from .schemas import HL7Schemas
from .transformations import HL7Transformations
from .quality_rules import HL7QualityRules

__version__ = "1.0.0"
__all__ = ["HL7Parser", "HL7Schemas", "HL7Transformations", "HL7QualityRules"]
