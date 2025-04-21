import re

def is_valid_schema_name(name: str) -> bool:
    # Skema valid: huruf/angka/underscore, tidak diawali angka
    return re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name) is not None
