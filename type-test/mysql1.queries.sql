-- Example queries for mysql1 schema (device telemetry, MariaDB dialect).
-- Arguments use ? placeholders.

-- query: get_device_by_id
SELECT `id`, `label`, `notes`, `active`, `role`, `load`, `registered_at`, `max_rate`
FROM `devices`
WHERE `id` = ?;

-- query: list_active_devices
SELECT `id`, `label`, `role`, `load`
FROM `devices`
WHERE `active` = ? AND `role` = ?;

-- query: insert_device
INSERT INTO `devices` (`label`, `notes`, `active`, `role`, `load`, `max_rate`)
VALUES (?, ?, ?, ?, ?, ?);

-- query: update_device_load
UPDATE `devices`
SET `load` = ?, `active` = ?
WHERE `id` = ?;

-- query: delete_device
DELETE FROM `devices`
WHERE `id` = ?;

-- query: get_channels_for_device
SELECT `id`, `device_id`, `token`, `config`, `sample_rate`, `kind`, `last_seen`
FROM `channels`
WHERE `device_id` = ? AND `kind` IS NOT NULL;

-- query: get_firing_alert_rules
SELECT `id`, `channel_id`, `name`, `notify_on`, `threshold`, `cooldown`, `priority`, `fired`, `status`
FROM `alert_rules`
WHERE `channel_id` = ? AND `fired` = ?;

-- query: insert_alert_event
INSERT INTO `alert_events` (`rule_id`, `nonce`, `mail_on`, `start_time`, `progress_sum`, `fraction_sum`)
VALUES (?, ?, ?, ?, ?, ?);

-- query: get_channel_status_view
SELECT `id`, `device_id`, `kind`, `status`, `fired`
FROM `channel_status`
WHERE `device_id` = ?;

-- query: get_storage_items_for_channel
SELECT `id`, `channel_id`, `json`, `payload`, `workspace_id`
FROM `storage_items`
WHERE `channel_id` = ? AND `workspace_id` = ?;
