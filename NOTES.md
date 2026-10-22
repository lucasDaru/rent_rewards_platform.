Award and reversal each run as one Postgres transaction. Replicas share the database, so the lock and the unique key live there, not in process memory.

BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended(member_id, 0));
INSERT INTO ledger_entries (member_id, payment_id, kind, points, month)
SELECT $member, $payment, 'award', LEAST($uncapped, GREATEST($cap - COALESCE(SUM(points), 0), 0)), $month
FROM ledger_entries
WHERE member_id = $member AND month = $month AND kind = 'award'
ON CONFLICT (payment_id, kind) DO NOTHING;
INSERT INTO ledger_entries (member_id, payment_id, kind, points, month)
SELECT member_id, payment_id, 'reversal', -points, month
FROM ledger_entries WHERE payment_id = $payment AND kind = 'award'
ON CONFLICT (payment_id, kind) DO NOTHING;
COMMIT;

UNIQUE (payment_id, kind) makes the same payment insert once under redelivery. The advisory lock serializes the monthly cap per member across replicas. A reversal copies the award row's points and is excluded from the month sum, so it cannot claw back more than was earned and does not free cap room. If the award row is missing, the reversal insert writes nothing and the handler acks. A ledger error rolls back and the handler nacks. Different members lock different keys and stay parallel.
