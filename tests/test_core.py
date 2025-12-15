import pytest
from app.modules.template_parser import TemplateParser
from app.modules.compression import CompressionModule
import json

def test_parser_accuracy():
    parser = TemplateParser()
    log = "User 123 logged in from 192.168.1.1"
    result = parser.parse(log)

    # Template should generalize dynamic values
    assert result["template"] == "User * logged in from *"

    # Parameters should be extracted correctly
    assert "123" in result["parameters"]
    assert "192.168.1.1" in result["parameters"]


def test_compression_integrity():
    compressor = CompressionModule()
    # Mock data: 3 logs with 1 parameter each
    data = [
        {"template_id": "A", "parameters": ["param1"]},
        {"template_id": "A", "parameters": ["param2"]},
        {"template_id": "A", "parameters": ["param3"]}
    ]
    
    # Compress
    blocks = compressor.compress_log_block(data)
    block = blocks["A"]
    
    # Decompress
    decompressed = compressor.decompress_block(block["compressed_params_hex"])
    
    # Verify data is exactly the same
    assert decompressed == [["param1", "param2", "param3"]]