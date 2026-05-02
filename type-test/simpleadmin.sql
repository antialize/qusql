-- sql-product: sqlite

CREATE TABLE IF NOT EXISTS `objects` (
    `id` INTEGER NOT NULL,
    `version` INTEGER NOT NULL,
    `type` INTEGER NOT NULL, 
    `name` TEXT NOT NULL,
    `content` TEXT NOT NULL,
    `comment` TEXT NOT NULL,
    `time` DATETIME NOT NULL,
    `newest` BOOLEAN NOT NULL,
    `category` TEXT,
    `author` TEXT
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS `id_version` ON `objects` (id, version);
      
CREATE TABLE IF NOT EXISTS `messages` (
    `id` INTEGER NOT NULL PRIMARY KEY,
    `host` INTEGER,
    `type` TEXT,
    `subtype` TEXT, -- Always null todo delete
    `message` TEXT,
    `url` TEXT, -- Always null todo delete
    `time` REAL,
    `dismissed` BOOLEAN NOT NULL,
    `dismissedTime` REAL
) STRICT;

CREATE INDEX IF NOT EXISTS `messagesIdx` ON `messages` (dismissed, time);
CREATE INDEX IF NOT EXISTS `messagesIdx2` ON `messages` (dismissed, dismissedTime);

CREATE TABLE IF NOT EXISTS `deployments` (
    `id` INTEGER,
    `host` INTEGER NOT NULL,
    `name` TEXT NOT NULL,
    `content` TEXT NOT NULL,
    `object` INTEGER,
    `time` DATETIME NOT NULL,
    `type` INTEGER NOT NULL,
    `title` TEXT NOT NULL
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS `deployments_host_name` ON `deployments` (host, name);

CREATE TABLE IF NOT EXISTS `docker_images` (
    `id` INTEGER NOT NULL PRIMARY KEY,
    `project` TEXT NOT NULL,
    `tag` TEXT NOT NULL,
    `manifest` TEXT NOT NULL,
    `hash` TEXT NOT NULL,
    `user` TEXT NOT NULL,
    `time` REAL NOT NULL,
    `pin` BOOLEAN NOT NULL DEFAULT false,
    `labels` TEXT,
    `removed` REAL,
    `used` REAL
) STRICT;

      
CREATE TABLE IF NOT EXISTS `docker_deployments` (
    `id` INTEGER NOT NULL PRIMARY KEY,
    `project` TEXT NOT NULL,
    `container` TEXT NOT NULL,
    `host` INTEGER NOT NULL,
    `startTime` INTEGER NOT NULL,
    `endTime` INTEGER,
    `config` TEXT,
    `hash` TEXT NOT NULL,
    `user` TEXT,
    `setup` TEXT,
    `postSetup` TEXT,
    `timeout` INTEGER DEFAULT 120,
    `softTakeover` BOOLEAN NOT NULL DEFAULT false,
    `startMagic` TEXT,
    `stopTimeout` INTEGER NOT NULL DEFAULT 10,
    `usePodman` BOOLEAN NOT NULL DEFAULT false,
    `userService` BOOLEAN NOT NULL DEFAULT false,
    `deployUser` TEXT,
    `serviceFile` TEXT,
    `description` TEXT) STRICT;

CREATE TABLE IF NOT EXISTS `docker_image_tag_pins` (
    `id` INTEGER NOT NULL PRIMARY KEY,
    `project` TEXT NOT NULL, 
    `tag` TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS `docker_image_tag_pins_u` ON `docker_image_tag_pins` (`project`, `tag`);

CREATE TABLE IF NOT EXISTS `kvp` (
    `key` TEXT NOT NULL PRIMARY KEY,
    `value` TEXT NOT NULL) STRICT;

CREATE TABLE IF NOT EXISTS `sessions` (
    `id` INTEGER NOT NULL PRIMARY KEY,
    `user` TEXT NOT NULL,
    `host` TEXT NOT NULL,
    `sid` TEXT NOT NULL,
    `pwd` INTEGER,
    `otp` INTEGER) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS `sessions_sid` ON `sessions` (`sid`);
