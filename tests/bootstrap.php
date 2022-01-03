<?php

require __DIR__.'/../vendor/autoload.php';

define('TEST_PROJECT', dirname(__DIR__));
define('TEST_ROOT', __DIR__);
define('TEST_FIXTURES', __DIR__.'/fixtures');
define('TEMP_ROOT', TEST_PROJECT . '/var/tests');

is_dir(TEMP_ROOT) || mkdir(TEMP_ROOT, 0777, true);
