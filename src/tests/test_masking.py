
from rbac.masking import mask_sensitive


def test_mask_password():
    original = "CREATE ROLE x LOGIN PASSWORD 'mypassword';"
    masked = mask_sensitive(original)
    assert "***MASKED***" in masked
    assert "mypassword" not in masked


def test_mask_token():
    original = "token='abcd'"
    masked = mask_sensitive(original)
    assert "***MASKED***" in masked


def test_no_mask_on_clean_text():
    assert mask_sensitive("SELECT 1") == "SELECT 1"
