from __future__ import annotations

from typing import Any, Mapping

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct


def dict_to_struct(data: Mapping[str, Any]) -> Struct:
    s = Struct()
    # Struct.update performs best-effort conversion (including nested mappings/lists).
    s.update(dict(data))
    return s


def struct_to_dict(s: Struct) -> dict[str, Any]:
    # MessageToDict handles nested Structs / Values.
    return json_format.MessageToDict(s, preserving_proto_field_name=True)