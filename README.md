# Kafka PHP

[![Build Status](https://img.shields.io/travis/euskadi31/kafka-php/master.svg)](https://travis-ci.org/euskadi31/kafka-php)
[![SensioLabs Insight](https://img.shields.io/sensiolabs/i/fe9e9f06-6ae2-416b-bfba-8962143e0443.svg)](https://insight.sensiolabs.com/projects/fe9e9f06-6ae2-416b-bfba-8962143e0443)
[![Coveralls](https://img.shields.io/coveralls/euskadi31/kafka-php.svg)](https://coveralls.io/github/euskadi31/kafka-php)
[![HHVM](https://img.shields.io/hhvm/euskadi31/kafka-php.svg)](https://travis-ci.org/euskadi31/kafka-php)
[![Packagist](https://img.shields.io/packagist/v/euskadi31/kafka-php.svg)](https://packagist.org/packages/euskadi31/kafka-php)


[php-rdkafka](https://github.com/arnaud-lb/php-rdkafka) improved to manage [php-zookeeper](https://github.com/andreiz/php-zookeeper).

## Install

Add `euskadi31/kafka-php` to your `composer.json`:

    % php composer.phar require euskadi31/kafka-php:~1.0

## Usage

### Producer

```php
<?php

$cm = new Euskadi31\Kafka\ClusterMetadata\Zookeeper([
    '10.0.0.13' => 2181
]);

$producer = new Euskadi31\Kafka\Producer($cm);

$topic = $producer->newTopic('test');

$topic->produce(RD_KAFKA_PARTITION_UA, 0, 'Message payload');
```

### Consumer

```php
<?php

$cm = new Euskadi31\Kafka\ClusterMetadata\Zookeeper([
    '10.0.0.13' => 2181
]);

$conf = new RdKafka\Conf();

$consumer = new Euskadi31\Kafka\Consumer($cm, $conf);

$topic = $consumer->newTopic('test');

$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

while (true) {
    // The first argument is the partition (again).
    // The second argument is the timeout.
    $msg = $topic->consume(0, 1000);

    if (empty($msg)) {
        continue;
    }

    if ($msg->err) {
        echo $msg->errstr() . PHP_EOL;
        break;
    } else {
        echo $msg->payload . PHP_EOL;
    }
}
```

## License

kafka-php is licensed under [the MIT license](LICENSE.md).
