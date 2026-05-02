-- Example queries for simpleadmin schema (SQLite dialect).
-- Arguments use ? placeholders.

-- ── objects ─────────────────────────────────────────────────────────────────

-- query: get_user_content
SELECT `content` FROM `objects` WHERE `type`=? AND `name`=? AND `newest`=true;

-- query: get_max_object_id
SELECT max(`id`) as `id` FROM `objects`;

-- query: get_max_object_version
SELECT max(`version`) as `version` FROM `objects` WHERE `id` = ?;

-- query: mark_objects_not_newest
UPDATE `objects` SET `newest`=false WHERE `id` = ?;

-- query: insert_object
INSERT INTO `objects` (
    `id`, `version`, `type`, `name`, `content`, `time`, `newest`, `category`, `comment`, `author`)
    VALUES (?, ?, ?, ?, ?, datetime('now'), true, ?, ?, ?);

-- query: get_object_by_name_and_type
SELECT `id`, `type`, `content`, `version`, `name`, `category`, `comment`,
    strftime('%s', `time`) AS `time`, `author`
FROM `objects`
WHERE `type` = ? AND `name`=? AND `newest`;

-- query: get_object_by_id_and_type
SELECT `id`, `type`, `content`, `version`, `name`, `category`, `comment`,
    strftime('%s', `time`) AS `time`, `author`
FROM `objects`
WHERE `type` = ? AND `id`=? AND `newest`;

-- query: get_newest_object_by_id
SELECT `id`, `type`, `content`, `version`, `name`, `category`, `comment`,
    strftime('%s', `time`) AS `time`, `author`
FROM `objects`
WHERE `id`=? AND `newest`;

-- query: get_object_content_by_id
SELECT `content` FROM `objects` WHERE `id` = ? AND `newest`;

-- query: update_object_content
UPDATE `objects` SET `content` = ? WHERE `id` = ? AND `newest`;

-- query: get_object_version_info
SELECT `version`, strftime('%s', `time`) AS `time`, `author`
FROM `objects`
WHERE `id`=?;

-- query: get_object_id_by_name_type
SELECT `id` FROM `objects` WHERE `type`=? AND `name`=? AND `newest`;

-- query: get_object_ids_by_type
SELECT `id` FROM `objects` WHERE `type` = ? AND `newest`;

-- query: get_object_id_names_by_type
SELECT `id`, `name` FROM `objects` WHERE `type` = ? AND `newest`;

-- query: list_all_objects
SELECT `id`, `type`, `name`, `content` FROM `objects` WHERE `newest` ORDER BY `id`;

-- query: get_object_content
SELECT `content` FROM `objects` WHERE `id`=? AND `newest`;

-- query: get_object_with_history_info
SELECT `id`, `type`, `content`, `version`, `name`, `category`, `comment`,
    strftime('%s', `time`) AS `time`, `author`
FROM `objects`
WHERE `id`=? AND `newest`;

-- query: search_objects
SELECT `id`, `version`, `type`, `name`, `content`, `comment`
FROM `objects`
WHERE (`name` LIKE ? OR `content` LIKE ? OR `comment` LIKE ?) AND `newest`;

-- query: get_object_version_list
SELECT `version`, strftime('%s', `time`) AS `time`, `author`
FROM `objects`
WHERE `id`=?;

-- query: get_object_by_name_type_full
SELECT `id`, `version`, `name`, `category`, `content`, `comment`, `author`
FROM `objects`
WHERE `type` = ? AND `name`=? AND `newest`;

-- query: get_objects_with_created_time
SELECT `id`, `name`, `content`, `time`,
    (SELECT MIN(`o2`.`time`) FROM `objects` AS `o2` WHERE `o2`.`id` = `o`.`id`) AS `created`
FROM `objects` AS `o`
WHERE `type`=? AND `newest`=true;

-- query: get_non_developer_objects
SELECT `id`, `name`, `content`, `time`, `category`
FROM `objects`
WHERE `type`=? AND `category` != 'Developer' AND `newest`=true;

-- ── deployments ──────────────────────────────────────────────────────────────

-- query: get_deployments_for_host
SELECT `name`, `content`, `type`, `title` FROM `deployments` WHERE `host`=?;

-- query: upsert_deployment
REPLACE INTO `deployments` (`host`, `name`, `content`, `time`, `type`, `title`)
VALUES (?, ?, ?, datetime('now'), ?, ?);

-- query: delete_deployment
DELETE FROM `deployments` WHERE `host`=? AND `name`=?;

-- query: delete_all_deployments_for_host
DELETE FROM `deployments` WHERE `host`=?;

-- query: get_deployments_by_types
SELECT `name`, `content`, `type`, `title`, `host`
FROM `deployments`
WHERE `type` in (?, ?, ?);

-- ── messages ─────────────────────────────────────────────────────────────────

-- query: get_recent_messages
SELECT `id`, `host`, `type`, `subtype`, `message`, `url`, `time`, `dismissed`, `dismissedTime`
FROM `messages`
WHERE NOT `dismissed` OR `dismissedTime`>?;

-- query: get_message_text
SELECT `message` FROM `messages` WHERE `id`=?;

-- query: count_undismissed_messages
SELECT count(*) as `count` FROM `messages` WHERE NOT `dismissed` AND `message` IS NOT NULL;

-- query: insert_message
INSERT INTO `messages` (`host`, `type`, `message`, `time`, `dismissed`)
VALUES (?, ?, ?, ?, false);

-- ── docker_images ────────────────────────────────────────────────────────────

-- query: list_docker_images_with_deployment_stats
SELECT `docker_images`.`manifest`, `docker_images`.`id`, `docker_images`.`tag`,
    `docker_images`.`time`, `docker_images`.`project`, `docker_images`.`hash`,
    MIN(`docker_deployments`.`startTime`) AS `start`,
    MAX(`docker_deployments`.`endTime`) AS `end`,
    COUNT(`docker_deployments`.`startTime`) - COUNT(`docker_deployments`.`endTime`) AS `active`,
    `docker_images`.`pin`, `docker_images`.`used`,
    (SELECT MAX(`x`.`id`) FROM `docker_images` AS `x`
        WHERE `x`.`project`=`docker_images`.`project` AND `x`.`tag`=`docker_images`.`tag`) AS `newest`,
    EXISTS (SELECT * FROM `docker_image_tag_pins`
        WHERE `docker_image_tag_pins`.`project`=`docker_images`.`project`
        AND `docker_image_tag_pins`.`tag`=`docker_images`.`tag`) AS `tagPin`
FROM `docker_images`
LEFT JOIN `docker_deployments` ON `docker_images`.`hash` = `docker_deployments`.`hash`
WHERE `removed` IS NULL
GROUP BY `docker_images`.`id`;

-- query: soft_delete_docker_image
UPDATE `docker_images` SET `removed`=? WHERE `id`=?;

-- query: get_docker_image_by_hash
SELECT `hash`, `time` FROM `docker_images` WHERE `project`=? AND `hash`=? ORDER BY `time` DESC LIMIT 1;

-- query: get_docker_image_by_tag
SELECT `hash`, `time` FROM `docker_images` WHERE `project`=? AND `tag`=? ORDER BY `time` DESC LIMIT 1;

-- query: insert_docker_image
INSERT INTO `docker_images` (`project`, `tag`, `manifest`, `hash`, `user`, `time`, `pin`, `labels`)
VALUES (?, ?, ?, ?, ?, ?, false, ?);

-- query: get_docker_image_manifest
SELECT `manifest` FROM `docker_images`
WHERE `project`=? AND (`tag`=? OR `hash`=?)
ORDER BY `time` DESC LIMIT 1;

-- query: delete_docker_image
DELETE FROM `docker_images` WHERE `project`=? AND `tag`=? AND `hash`=?;

-- query: get_docker_image_by_id
SELECT `id`, `hash`, `time`, `project`, `user`, `tag`, `pin`, `labels`, `removed`
FROM `docker_images`
WHERE `id`=?;

-- query: get_docker_images_by_tag_project
SELECT `id`, `hash`, `time`, `project`, `user`, `tag`, `pin`, `labels`, `removed`
FROM `docker_images`
WHERE `tag` = ? AND `project`= ?;

-- query: update_docker_image_pin
UPDATE `docker_images` SET `pin`=? WHERE `id`=?;

-- ── docker_deployments ───────────────────────────────────────────────────────

-- query: get_latest_docker_deployment
SELECT `id`, `endTime`
FROM `docker_deployments`
WHERE `host`=? AND `project`=? AND `container`=?
ORDER BY `startTime` DESC LIMIT 1;

-- query: end_docker_deployment
UPDATE `docker_deployments` SET `endTime` = ? WHERE `id`=?;

-- query: insert_docker_deployment
INSERT INTO `docker_deployments` (`project`, `container`, `host`, `startTime`, `hash`, `user`, `description`)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- query: get_docker_deployment
SELECT `description`, `host`, `project`, `hash` FROM `docker_deployments` WHERE `id`=?;

-- query: delete_docker_deployments_for_container
DELETE FROM `docker_deployments` WHERE `host`=? AND `container`=?;

-- ── docker_image_tag_pins ────────────────────────────────────────────────────

-- query: insert_docker_image_tag_pin
INSERT INTO `docker_image_tag_pins` (`project`, `tag`) VALUES (?, ?);

-- query: delete_docker_image_tag_pin
DELETE FROM `docker_image_tag_pins` WHERE `project`=? AND `tag`=?;

-- ── kvp ──────────────────────────────────────────────────────────────────────

-- query: get_kvp
SELECT `value` FROM `kvp` WHERE `key` = ?;

-- query: upsert_kvp
REPLACE INTO `kvp` (`key`, `value`) VALUES (?, ?);

-- ── sessions ─────────────────────────────────────────────────────────────────

-- query: get_session
SELECT `pwd`, `otp`, `user`, `host` FROM `sessions` WHERE `sid`=?;

-- query: insert_session
INSERT INTO `sessions` (`user`, `host`, `pwd`, `otp`, `sid`) VALUES (?, ?, ?, ?, ?);

-- query: delete_session
DELETE FROM `sessions` WHERE `user`=? AND `sid`=?;

-- query: update_session_otp
UPDATE `sessions` SET `otp`=? WHERE `sid`=?;

-- query: update_session_pwd
UPDATE `sessions` SET `pwd`=? WHERE `sid`=?;

-- query: update_session_pwd_otp
UPDATE `sessions` SET `pwd`=?, `otp`=? WHERE `sid`=?;

-- query: clear_session_pwd
UPDATE `sessions` SET `pwd`=NULL WHERE `sid`=?;

-- query: clear_session_otp
UPDATE `sessions` SET `otp`=NULL WHERE `sid`=?;
