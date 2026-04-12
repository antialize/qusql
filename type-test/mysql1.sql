-- Test schema for qusql-type / MariaDB dialect.
-- Domain: device telemetry and alerting.
-- Each table is chosen to exercise a distinct set of type rules.

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

-- --------------------------------------------------------
-- `devices`
-- Covers: bigint PK, varchar, text nullable, tinyint(1) bool,
--         enum NOT NULL with DEFAULT, float, timestamp,
--         int UNSIGNED NOT NULL.
-- --------------------------------------------------------

DROP TABLE IF EXISTS `devices`;
CREATE TABLE `devices` (
  `id` bigint(20) NOT NULL,
  `label` varchar(255) NOT NULL,
  `notes` text DEFAULT NULL,
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `role` enum('sensor','relay','gateway','store') NOT NULL DEFAULT 'sensor',
  `load` float NOT NULL DEFAULT 0,
  `registered_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `max_rate` int(11) UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------
-- `channels`
-- Covers: int PK, binary(16), mediumtext NOT NULL, longblob nullable,
--         double, enum nullable, datetime nullable.
-- --------------------------------------------------------

DROP TABLE IF EXISTS `channels`;
CREATE TABLE `channels` (
  `id` int(11) NOT NULL,
  `device_id` bigint(20) NOT NULL,
  `token` binary(16) NOT NULL,
  `config` mediumtext NOT NULL,
  `raw_data` longblob DEFAULT NULL,
  `sample_rate` double NOT NULL DEFAULT 0,
  `kind` enum('counter','gauge','histogram','event') DEFAULT NULL,
  `last_seen` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------
-- `alert_rules`
-- Covers: set NOT NULL, varchar GENERATED ALWAYS AS STORED (CASE/IF),
--         smallint UNSIGNED, int UNSIGNED.
-- --------------------------------------------------------

DROP TABLE IF EXISTS `alert_rules`;
CREATE TABLE `alert_rules` (
  `id` int(11) NOT NULL,
  `channel_id` int(11) NOT NULL,
  `name` varchar(128) NOT NULL,
  `notify_on` set('error','warning','recovery') NOT NULL DEFAULT '',
  `threshold` double DEFAULT NULL,
  `cooldown` int(11) UNSIGNED NOT NULL DEFAULT 60,
  `priority` smallint(5) UNSIGNED NOT NULL DEFAULT 0,
  `fired` tinyint(1) NOT NULL DEFAULT 0,
  `status` varchar(16) GENERATED ALWAYS AS (case when `fired` then 'firing' when `threshold` is null then 'inactive' else 'ok' end) STORED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------
-- `alert_events`
-- Covers: bigint UNSIGNED, double nullable start/end times,
--         float GENERATED VIRTUAL (arithmetic),
--         enum GENERATED VIRTUAL (CASE).
-- --------------------------------------------------------

DROP TABLE IF EXISTS `alert_events`;
CREATE TABLE `alert_events` (
  `id` bigint(20) NOT NULL,
  `rule_id` int(11) NOT NULL,
  `nonce` bigint(20) UNSIGNED NOT NULL,
  `mail_on` set('error','completion') NOT NULL DEFAULT '',
  `start_time` double DEFAULT NULL,
  `end_time` double DEFAULT NULL,
  `progress_sum` double NOT NULL DEFAULT 0,
  `fraction_sum` double NOT NULL DEFAULT 1,
  `progress` float GENERATED ALWAYS AS (`progress_sum` / `fraction_sum`) VIRTUAL,
  `phase` enum('pending','active','resolved','cancelled') GENERATED ALWAYS AS (case when `start_time` is null then 'pending' when `end_time` is null then 'active' else 'resolved' end) VIRTUAL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Triggers `alert_events`
--
DROP TRIGGER IF EXISTS `alert_events_insert`;
DELIMITER $$
CREATE TRIGGER `alert_events_insert` AFTER INSERT ON `alert_events` FOR EACH ROW BEGIN
IF NEW.`rule_id` IS NOT NULL THEN
  UPDATE `alert_rules` SET `fired` = 1 WHERE `id` = NEW.`rule_id` AND NOT `fired`;
END IF;
END
$$
DELIMITER ;
DROP TRIGGER IF EXISTS `alert_events_delete`;
DELIMITER $$
CREATE TRIGGER `alert_events_delete` BEFORE DELETE ON `alert_events` FOR EACH ROW INSERT INTO `alert_events` (`rule_id`, `nonce`, `start_time`) VALUES (OLD.`rule_id`, OLD.`nonce`, OLD.`start_time`)
$$
DELIMITER ;

-- --------------------------------------------------------
-- `storage_items`
-- Covers: mediumtext NOT NULL (JSON), varbinary(512),
--         int GENERATED ALWAYS AS STORED from json_value().
-- --------------------------------------------------------

DROP TABLE IF EXISTS `storage_items`;
CREATE TABLE `storage_items` (
  `id` bigint(20) NOT NULL,
  `channel_id` int(11) NOT NULL,
  `json` mediumtext NOT NULL,
  `payload` varbinary(512) NOT NULL,
  `workspace_id` int(11) GENERATED ALWAYS AS (json_value(`json`,'$.workspaceId')) STORED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------
-- Stand-in for view `device_summary`
-- Covers: nullable view columns (no NOT NULL).
-- --------------------------------------------------------

DROP VIEW IF EXISTS `device_summary`;
CREATE TABLE `device_summary` (
`id` bigint(20)
,`label` varchar(255)
,`role` enum('sensor','relay','gateway','store')
,`active_channels` int(11)
,`last_seen` datetime
);

-- --------------------------------------------------------
-- Stand-in for view `channel_status`
-- Covers: nullable enum in view.
-- --------------------------------------------------------

DROP VIEW IF EXISTS `channel_status`;
CREATE TABLE `channel_status` (
`id` int(11)
,`device_id` bigint(20)
,`kind` enum('counter','gauge','histogram','event')
,`status` varchar(16)
,`fired` tinyint(1)
);

-- --------------------------------------------------------
-- Indexes and constraints
-- --------------------------------------------------------

ALTER TABLE `devices`
  ADD PRIMARY KEY (`id`),
  ADD KEY `role` (`role`);

ALTER TABLE `devices`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

ALTER TABLE `channels`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `token` (`token`),
  ADD KEY `device_id` (`device_id`),
  ADD KEY `kind` (`kind`);

ALTER TABLE `channels`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `alert_rules`
  ADD PRIMARY KEY (`id`),
  ADD KEY `channel_id` (`channel_id`),
  ADD KEY `status` (`status`);

ALTER TABLE `alert_rules`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `alert_events`
  ADD PRIMARY KEY (`id`),
  ADD KEY `rule_id` (`rule_id`),
  ADD KEY `phase` (`phase`);

ALTER TABLE `alert_events`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

ALTER TABLE `storage_items`
  ADD PRIMARY KEY (`id`),
  ADD KEY `channel_id` (`channel_id`),
  ADD KEY `workspace_id` (`workspace_id`);

ALTER TABLE `storage_items`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

ALTER TABLE `channels`
  ADD CONSTRAINT `fk_channels_device` FOREIGN KEY (`device_id`) REFERENCES `devices` (`id`);

ALTER TABLE `alert_rules`
  ADD CONSTRAINT `fk_alert_rules_channel` FOREIGN KEY (`channel_id`) REFERENCES `channels` (`id`);

ALTER TABLE `alert_events`
  ADD CONSTRAINT `fk_alert_events_rule` FOREIGN KEY (`rule_id`) REFERENCES `alert_rules` (`id`);

ALTER TABLE `storage_items`
  ADD CONSTRAINT `fk_storage_items_channel` FOREIGN KEY (`channel_id`) REFERENCES `channels` (`id`);

-- --------------------------------------------------------
-- Final view definitions
-- --------------------------------------------------------

DROP TABLE IF EXISTS `device_summary`;
DROP VIEW IF EXISTS `device_summary`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost`
  SQL SECURITY DEFINER VIEW `device_summary` AS
  SELECT
    `d`.`id` AS `id`,
    `d`.`label` AS `label`,
    `d`.`role` AS `role`,
    COUNT(`c`.`id`) AS `active_channels`,
    MAX(`c`.`last_seen`) AS `last_seen`
  FROM `devices` `d`
  LEFT JOIN `channels` `c` ON `c`.`device_id` = `d`.`id`
  WHERE `d`.`active` = TRUE
  GROUP BY `d`.`id`;

DROP TABLE IF EXISTS `channel_status`;
DROP VIEW IF EXISTS `channel_status`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost`
  SQL SECURITY DEFINER VIEW `channel_status` AS
  SELECT
    `c`.`id` AS `id`,
    `c`.`device_id` AS `device_id`,
    `c`.`kind` AS `kind`,
    `r`.`status` AS `status`,
    `r`.`fired` AS `fired`
  FROM `channels` `c`
  LEFT JOIN `alert_rules` `r` ON `r`.`channel_id` = `c`.`id`;
