DROP TABLE IF EXISTS `t1`;
CREATE TABLE `t1` (
    `id` int(11) NOT NULL,
    `cbool` tinyint(1) NOT NULL DEFAULT false,
    `cu8` tinyint UNSIGNED NOT NULL DEFAULT 0,
    `cu16` smallint UNSIGNED NOT NULL DEFAULT 1,
    `cu32` int UNSIGNED NOT NULL DEFAULT 2, 
    `cu64` bigint UNSIGNED NOT NULL DEFAULT 3,
    `ci8` tinyint,
    `ci16` smallint,
    `ci32` int,
    `ci64` bigint,
    `ctext` varchar(100) NOT NULL,
    `cbytes` blob,
    `cf32` float,
    `cf64` double
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `t1`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;