<?php
/**
 * This file is part of the kafka-php.
 *
 * (c) Axel Etcheverry <axel@etcheverry.biz>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Euskadi31\Kafka;

class ConsumerTest extends \PHPUnit_Framework_TestCase
{
    public function testInterface()
    {
        $consumer = new Consumer();

        $this->assertInstanceOf('\RdKafka\Consumer', $consumer);
    }

    public function testConsumerWithClusterMetadata()
    {
        $zookeeperMock = $this->getMock('Euskadi31\Kafka\ClusterMetadata\Zookeeper');

        $zookeeperMock->expects($this->once())
            ->method('getBrokers')
            ->will($this->returnValue([
                [
                    'host' => '127.0.0.1',
                    'port' => 9092
                ]
            ]));

        (new Consumer($zookeeperMock));
    }

    public function testConsumerWithClusterMetadataAndConf()
    {
        $zookeeperMock = $this->getMock('Euskadi31\Kafka\ClusterMetadata\Zookeeper');

        $zookeeperMock->expects($this->once())
            ->method('getBrokers')
            ->will($this->returnValue([
                [
                    'host' => '127.0.0.1',
                    'port' => 9092
                ]
            ]));

        $confMock = $this->getMock('\RdKafka\Conf');

        $confMock->expects($this->once())
            ->method('set')
            ->with($this->equalTo('metadata.broker.list'), $this->equalTo('127.0.0.1:9092'));

        (new Consumer($zookeeperMock, $confMock));
    }
}
