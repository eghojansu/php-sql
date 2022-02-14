<?php

define('TEST_PROJECT', realpath(__DIR__ . '/../..'));
define('TEST_ROOT', realpath(__DIR__ . '/..'));
define('TEST_DATA', TEST_ROOT . '/_data');
define('TEST_TMP', TEST_PROJECT . '/var/tests');

is_dir(TEST_TMP) || mkdir(TEST_TMP, 0777, true);
