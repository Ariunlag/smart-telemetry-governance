from app.domain.streams.identity import normalize_topic, stream_key


def test_topic_normalization_and_stream_key_are_stable() -> None:
    assert normalize_topic(" /Plant/ Boiler/Temp/ ") == "Plant/Boiler/Temp"
    assert stream_key("Broker-A", "plant/topic", "Tenant-A") == stream_key(
        " broker-a ", "plant/topic", "tenant-a"
    )


def test_source_and_tenant_are_stream_identity_boundaries() -> None:
    assert stream_key("broker-a", "plant/topic", "tenant-a") != stream_key(
        "broker-b", "plant/topic", "tenant-a"
    )
    assert stream_key("broker-a", "plant/topic", "tenant-a") != stream_key(
        "broker-a", "plant/topic", "tenant-b"
    )
