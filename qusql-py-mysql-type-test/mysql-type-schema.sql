DROP TABLE IF EXISTS `py_test`;
CREATE TABLE `py_test` (
    `id`            int(11) NOT NULL AUTO_INCREMENT,
    `name`          varchar(100) NOT NULL,
    `value`         int NOT NULL,
    `optional_text` text,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
