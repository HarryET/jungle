-- ExampleQuery :one
SELECT * FROM accounts WHERE id = $1 LIMIT 1;

-- ExampleQuery2 :many
SELECT * FROM accounts;

-- ExampleQuery3 :exec
SELECT id, email FROM accounts;
