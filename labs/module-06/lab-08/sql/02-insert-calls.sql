-- Part 3: six calls from three subscribers. INSERT ... VALUES is for labs and tests;
-- real data comes from producers such as the Lab 04 and Lab 06 apps.
INSERT INTO ${ME}_calls (caller, call_id, callee, duration_sec, roaming) VALUES ('9198450001', 'c-1', '9198451111',  40, false);
INSERT INTO ${ME}_calls (caller, call_id, callee, duration_sec, roaming) VALUES ('9198450002', 'c-2', '9198451111', 300, false);
INSERT INTO ${ME}_calls (caller, call_id, callee, duration_sec, roaming) VALUES ('9198450001', 'c-3', '447700900123', 610, true);
INSERT INTO ${ME}_calls (caller, call_id, callee, duration_sec, roaming) VALUES ('9198450003', 'c-4', '9198452222',  15, false);
INSERT INTO ${ME}_calls (caller, call_id, callee, duration_sec, roaming) VALUES ('9198450002', 'c-5', '9198450001',  95, false);
INSERT INTO ${ME}_calls (caller, call_id, callee, duration_sec, roaming) VALUES ('9198450001', 'c-6', '9198452222', 130, false);
