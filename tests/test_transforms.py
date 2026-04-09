import json

from etl import transform_comments, transform_posts, transform_users


def test_transform_users_serializes_nested_fields_as_json() -> None:
    raw = [
        {
            "id": 1,
            "name": "Alice",
            "username": "alice",
            "email": "alice@example.com",
            "phone": "12345",
            "website": "alice.dev",
            "company": {"name": "Acme"},
            "address": {"city": "Tbilisi"},
        }
    ]

    rows = transform_users(raw)

    assert rows == [
        {
            "id": 1,
            "name": "Alice",
            "username": "alice",
            "email": "alice@example.com",
            "phone": "12345",
            "website": "alice.dev",
            "company": '{"name": "Acme"}',
            "address": '{"city": "Tbilisi"}',
        }
    ]
    assert json.loads(rows[0]["company"]) == {"name": "Acme"}
    assert json.loads(rows[0]["address"]) == {"city": "Tbilisi"}


def test_transform_users_uses_empty_json_for_missing_nested_fields() -> None:
    raw = [
        {
            "id": 2,
            "name": "Bob",
            "username": "bob",
            "email": "bob@example.com",
        }
    ]

    rows = transform_users(raw)

    assert rows[0]["phone"] is None
    assert rows[0]["website"] is None
    assert rows[0]["company"] == "{}"
    assert rows[0]["address"] == "{}"


def test_transform_posts_and_comments_map_api_keys() -> None:
    posts_raw = [{"id": 10, "userId": 3, "title": "Post", "body": "Body"}]
    comments_raw = [
        {
            "id": 100,
            "postId": 10,
            "name": "Comment",
            "email": "comment@example.com",
            "body": "Text",
        }
    ]

    posts = transform_posts(posts_raw)
    comments = transform_comments(comments_raw)

    assert posts == [{"id": 10, "user_id": 3, "title": "Post", "body": "Body"}]
    assert comments == [
        {
            "id": 100,
            "post_id": 10,
            "name": "Comment",
            "email": "comment@example.com",
            "body": "Text",
        }
    ]
