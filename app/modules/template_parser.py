
import re
import hashlib
from collections import defaultdict
from typing import Dict, Any, List


class TemplateParser:
    """
    TemplateParser identifies recurring patterns in log messages by
    replacing variable components (numbers, IPs, UUIDs, etc.) with
    placeholders. It maintains a dictionary of discovered templates
    for compression and indexing.
    """

    def __init__(self):
        # template_dict maps template_id → {"template": str, "count": int}
        self.template_dict: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"template": "", "count": 0}
        )

        # Common regex rules for variable patterns
        self.patterns = [
            (r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", "<IP>"),          # IP addresses
            (r"\b[0-9a-fA-F-]{36}\b", "<UUID>"),                          # UUIDs
            (r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?\b", "<TIMESTAMP>"),  # Timestamps
            (r"/[\w\-/\.]+", "<PATH>"),                                   # File or directory paths
            (r"\b\d+\b", "<NUM>"),                                        # Numbers
            (r"\b[a-fA-F0-9]{16,}\b", "<HEX>"),                           # Long hex strings
        ]

    def _get_template_id(self, template_str: str) -> str:
        """Generate a unique ID for a given template string."""
        return hashlib.md5(template_str.encode("utf-8")).hexdigest()

    def _normalize_text(self, text: str) -> str:
        """Optional: normalize whitespace and casing for consistency."""
        text = text.strip()
        text = re.sub(r"\s+", " ", text)
        return text

    def parse(self, log_message: str) -> Dict[str, Any]:
        """
        Parses a raw log message to extract its template and parameters.
        Returns a dict containing the template ID, template string, and parameter values.
        """
        if not isinstance(log_message, str) or not log_message.strip():
            raise ValueError("Invalid log message: must be a non-empty string.")

        # Normalize message
        log_message = self._normalize_text(log_message)

        # Step 1: Replace known variable patterns with placeholders
        processed_message = log_message
        for pattern, placeholder in self.patterns:
            processed_message = re.sub(pattern, placeholder, processed_message)

        # Step 2: Tokenize and replace dynamic tokens with '*'
        tokens = processed_message.split()
        template_parts: List[str] = []
        parameters: List[str] = []

        for token in tokens:
            if re.match(r"^<.*>$", token):
                template_parts.append("*")
                parameters.append(token)
            else:
                template_parts.append(token)

        log_template = " ".join(template_parts)
        template_id = self._get_template_id(log_template)

        # Step 3: Update template dictionary
        if self.template_dict[template_id]["count"] == 0:
            self.template_dict[template_id]["template"] = log_template
            print(f"🧩 New template detected: '{log_template}' (ID: {template_id})")

        self.template_dict[template_id]["count"] += 1

        # Step 4: Extract numeric and other parameter values
        numeric_values = re.findall(r"\b\d+\b", log_message)
        ip_values = re.findall(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", log_message)
        all_params = numeric_values + ip_values

        return {
            "template_id": template_id,
            "template": log_template,
            "parameters": all_params,
            "template_frequency": self.template_dict[template_id]["count"],
        }

    def get_templates(self) -> Dict[str, Dict[str, Any]]:
        """Return all known templates."""
        return dict(self.template_dict)
