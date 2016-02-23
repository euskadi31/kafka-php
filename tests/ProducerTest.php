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

class ProducerTest extends \PHPUnit_Framework_TestCase
{
    public function testInterface()
    {
        $producer = new Producer();

        $this->assertInstanceOf('\RdKafka\Producer', $producer);
    }

    public function testProducerWithClusterMetadata()
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

        (new Producer($zookeeperMock));
    }

    public function testProducerWithClusterMetadataAndConf()
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

        (new Producer($zookeeperMock, $confMock));
    }
}
