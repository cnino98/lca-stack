from dataclasses import dataclass

# cyclonedds is optional at import-time; DDS will be disabled if it can't load.
try:
    from cyclonedds.idl import IdlStruct  # type: ignore[import-not-found]
    from cyclonedds.idl.types import sequence  # type: ignore[import-not-found]

    # Different cyclonedds versions expose different names for an 8-bit type.
    try:
        from cyclonedds.idl.types import uint8 as _u8  # type: ignore[import-not-found]
    except Exception:
        try:
            from cyclonedds.idl.types import octet as _u8  # type: ignore[import-not-found]
        except Exception:
            from cyclonedds.idl.types import byte as _u8  # type: ignore[import-not-found]

except Exception as e:  # pragma: no cover
    _IMPORT_ERROR = e

    @dataclass
    class EnvelopeBytes:
        # Placeholder type so the module imports; DDS start will fail later (as intended).
        data: bytes

else:
    _IMPORT_ERROR = None

    @dataclass
    class EnvelopeBytes(IdlStruct):
        """DDS payload type used by LCA Stack: serialized Protobuf Envelope bytes."""
        # IMPORTANT: no `from __future__ import annotations` in this file.
        # CycloneDDS needs the actual typing objects here, not stringified annotations.
        data: sequence[_u8]  # type: ignore[misc,valid-type]


def pack_envelope_bytes(payload: bytes) -> EnvelopeBytes:
    if _IMPORT_ERROR is not None:  # pragma: no cover
        raise RuntimeError(f"cyclonedds is not available: {_IMPORT_ERROR}")
    # `sequence[_u8]` expects a list[int] with values 0..255.
    return EnvelopeBytes(data=list(payload))  # type: ignore[arg-type]


def unpack_envelope_bytes(sample: EnvelopeBytes) -> bytes:
    # On real DDS samples, sample.data is a sequence[int].
    return bytes(sample.data)  # type: ignore[arg-type]
