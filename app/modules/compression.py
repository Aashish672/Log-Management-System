# app/modules/compression.py

import zlib
import json
from collections import defaultdict
from typing import List, Dict, Any
import datetime


class CompressionModule:
    """
    CompressionModule groups logs by template ID and compresses their parameters
    using lightweight algorithms (zlib). It simulates columnar storage for
    space efficiency and enables faster decompression for analysis.
    """

    def compress_log_block(self, parsed_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compresses a block of parsed logs using template-based grouping and columnar storage.
        Each group is stored as a compressed block identified by its template ID.
        """
        if not parsed_logs:
            print("⚠️ No logs to compress.")
            return {}

        grouped_by_template = defaultdict(list)
        timestamps_by_template = defaultdict(list)

        # Group logs by template ID
        for log in parsed_logs:
            template_id = log.get("template_id")
            params = log.get("parameters", [])
            timestamp = log.get("timestamp", datetime.datetime.utcnow())
            if template_id:
                grouped_by_template[template_id].append(params)
                timestamps_by_template[template_id].append(str(timestamp))

        compressed_blocks = {}

        for template_id, all_params in grouped_by_template.items():
            if not all_params:
                continue

            # Transpose parameter matrix -> columnar format
            num_params = len(all_params[0])
            columns = [[] for _ in range(num_params)]

            for param_set in all_params:
                if len(param_set) == num_params:
                    for i in range(num_params):
                        columns[i].append(param_set[i])

            # Serialize columnar structure
            columnar_json = json.dumps(columns).encode("utf-8")
            compressed_data = zlib.compress(columnar_json, level=6)

            if len(columnar_json)==0:
                compression_ratio=0
            else:
                compression_ratio=round((1-(len(compressed_data)/len(columnar_json)))*100,2)

            compressed_blocks[template_id] = {
                "template_id": template_id,
                "log_count": len(all_params),
                "compression_ratio": f"{compression_ratio}%",
                "compressed_size_bytes": len(compressed_data),
                "original_size_bytes": len(columnar_json),
                "start_time": timestamps_by_template[template_id][0],
                "end_time": timestamps_by_template[template_id][-1],
                "compressed_params_hex": compressed_data.hex(),
            }

            print(
                f"🗜️ Template {template_id}: {len(all_params)} logs "
                f"→ Original {len(columnar_json)} bytes, "
                f"Compressed {len(compressed_data)} bytes ({compression_ratio}% saved)"
            )

        return compressed_blocks

    def decompress_block(self, compressed_hex: str) -> List[List[str]]:
        """
        Decompresses a previously compressed hex string back into parameter columns.
        Useful for testing or validating decompression.
        """
        try:
            compressed_bytes = bytes.fromhex(compressed_hex)
            decompressed_json = zlib.decompress(compressed_bytes)
            return json.loads(decompressed_json.decode("utf-8"))
        except Exception as e:
            print(f"❌ Decompression failed: {e}")
            return []
