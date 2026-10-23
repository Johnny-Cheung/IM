UPDATE friend_requests
SET pending_from_user_id = from_user_id,
    pending_to_user_id = to_user_id
WHERE status = 'pending' AND pending_from_user_id IS NULL;
