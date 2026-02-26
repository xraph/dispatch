"""Unit tests for dispatch.frame."""

from dispatch.frame import ErrorDetail, Frame, FrameType


def test_frame_roundtrip() -> None:
    f = Frame(
        id="123",
        type=FrameType.REQUEST,
        method="job.enqueue",
        data={"name": "test"},
    )
    d = f.to_dict()
    assert d["id"] == "123"
    assert d["type"] == "request"
    assert d["method"] == "job.enqueue"
    assert d["data"] == {"name": "test"}

    f2 = Frame.from_dict(d)
    assert f2.id == f.id
    assert f2.type == f.type
    assert f2.method == f.method
    assert f2.data == f.data


def test_frame_error_roundtrip() -> None:
    f = Frame(
        id="456",
        type=FrameType.ERROR,
        correl_id="123",
        error=ErrorDetail(code=404, message="not found"),
    )
    d = f.to_dict()
    assert d["error"]["code"] == 404
    assert d["error"]["message"] == "not found"

    f2 = Frame.from_dict(d)
    assert f2.error is not None
    assert f2.error.code == 404
    assert f2.error.message == "not found"


def test_frame_omits_empty_fields() -> None:
    f = Frame(id="789", type=FrameType.PONG, correl_id="abc")
    d = f.to_dict()
    assert "method" not in d
    assert "data" not in d
    assert "error" not in d
    assert "channel" not in d
    assert "credits" not in d
    assert d["correl_id"] == "abc"
