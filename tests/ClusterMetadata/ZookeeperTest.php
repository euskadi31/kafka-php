<?php
/**
 * This file is part of the kafka-php.
 *
 * (c) Axel Etcheverry <axel@etcheverry.biz>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Euskadi31\Kafka\ClusterMetadata;

class ZookeeperTest extends \PHPUnit_Framework_TestCase
{
    public function testInterface()
    {
        $zookeeper = new Zookeeper();

        $this->assertInstanceOf('Euskadi31\Kafka\ClusterMetadata\ClusterMetadataInterface', $zookeeper);
    }

    public function testHosts()
    {
        $hosts = [
            '127.0.0.1' => 2181
        ];

        $zookeeper = new Zookeeper($hosts);

        $this->assertEquals($hosts, $zookeeper->getHosts());
        $this->assertEquals('127.0.0.1:2181', $zookeeper->getHostsString());

        $zookeeper->addHost('168.192.1.14');
        $hosts['168.192.1.14'] = 2181;

        $this->assertEquals($hosts, $zookeeper->getHosts());
        $this->assertEquals('127.0.0.1:2181,168.192.1.14:2181', $zookeeper->getHostsString());
    }

    public function testTimeout()
    {
        $zookeeper = new Zookeeper();

        $this->assertEquals(10000, $zookeeper->getTimeout());

        $zookeeper->setTimeout(123);

        $this->assertEquals(123, $zookeeper->getTimeout());
    }

    public function testZookeeperClient()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $hosts = [
            '127.0.0.1' => 2181
        ];

        $zookeeper = new Zookeeper($hosts);

        $this->assertInstanceOf('\ZooKeeper', $zookeeper->getZookeeper());

        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals($zookeeperMock, $zookeeper->getZookeeper());
    }

    public function testGetBrokers()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('getChildren')
            ->with($this->equalTo(Zookeeper::BROKER_PATH))
            ->will($this->returnValue([
                1, 2
            ]));

        $zookeeper = $this->getMock('Euskadi31\Kafka\ClusterMetadata\Zookeeper', ['getBrokerDetail']);

        $zookeeper->expects($this->exactly(2))
            ->method('getBrokerDetail')
            ->will($this->returnValue([
                'foo' => 'bar'
            ]));

        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([
            1 => [
                'foo' => 'bar'
            ],
            2 => [
                'foo' => 'bar'
            ]
        ], $zookeeper->getBrokers());
    }

    public function testGetBrokersWithBrokerNotExists()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('getChildren')
            ->with($this->equalTo(Zookeeper::BROKER_PATH))
            ->will($this->returnValue([
                1, 2
            ]));

        $zookeeper = $this->getMock('Euskadi31\Kafka\ClusterMetadata\Zookeeper', ['getBrokerDetail']);

        $zookeeper->expects($this->at(0))
            ->method('getBrokerDetail')
            ->with($this->equalTo(1))
            ->will($this->returnValue([
                'foo' => 'bar'
            ]));

        $zookeeper->expects($this->at(1))
            ->method('getBrokerDetail')
            ->with($this->equalTo(2))
            ->will($this->returnValue([]));

        //$zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([
            1 => [
                'foo' => 'bar'
            ]
        ], $zookeeper->getBrokers());
    }

    public function testGetBrokerDetail()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('exists')
            ->with($this->equalTo(sprintf(Zookeeper::BROKER_DETAIL_PATH, 1)))
            ->will($this->returnValue(true));

        $zookeeperMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo(sprintf(Zookeeper::BROKER_DETAIL_PATH, 1)))
            ->will($this->returnValue('{"foo":"bar"}'));

        $zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([
            'foo' => 'bar'
        ], $zookeeper->getBrokerDetail(1));
    }

    public function testGetBrokerDetailWithEmptyResponse()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('exists')
            ->with($this->equalTo(sprintf(Zookeeper::BROKER_DETAIL_PATH, 1)))
            ->will($this->returnValue(true));

        $zookeeperMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo(sprintf(Zookeeper::BROKER_DETAIL_PATH, 1)))
            ->will($this->returnValue(null));

        $zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([], $zookeeper->getBrokerDetail(1));
    }

    /*public function testGetPartitionState()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('exists')
            ->with($this->equalTo(sprintf(Zookeeper::PARTITION_STATE, 'test_topic', 0)))
            ->will($this->returnValue(true));

        $zookeeperMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo(sprintf(Zookeeper::PARTITION_STATE, 'test_topic', 0)))
            ->will($this->returnValue('{"foo":"bar"}'));

        $zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([
            'foo' => 'bar'
        ], $zookeeper->getPartitionState('test_topic'));
    }

    public function testGetPartitionStateWithEmptyResponse()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('exists')
            ->with($this->equalTo(sprintf(Zookeeper::PARTITION_STATE, 'test_topic', 0)))
            ->will($this->returnValue(true));

        $zookeeperMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo(sprintf(Zookeeper::PARTITION_STATE, 'test_topic', 0)))
            ->will($this->returnValue(null));

        $zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([], $zookeeper->getPartitionState('test_topic'));

    }

    public function testGetTopicDetail()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('exists')
            ->with($this->equalTo(sprintf(Zookeeper::TOPIC_PATCH, 'test_topic')))
            ->will($this->returnValue(true));

        $zookeeperMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo(sprintf(Zookeeper::TOPIC_PATCH, 'test_topic')))
            ->will($this->returnValue('{"foo":"bar"}'));

        $zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([
            'foo' => 'bar'
        ], $zookeeper->getTopicDetail('test_topic'));
    }

    public function testGetTopicDetailWithEmptyResponse()
    {
        $zookeeperMock = $this->getMockBuilder('\ZooKeeper')
            ->disableOriginalConstructor()
            ->getMock();

        $zookeeperMock->expects($this->once())
            ->method('exists')
            ->with($this->equalTo(sprintf(Zookeeper::TOPIC_PATCH, 'test_topic')))
            ->will($this->returnValue(true));

        $zookeeperMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo(sprintf(Zookeeper::TOPIC_PATCH, 'test_topic')))
            ->will($this->returnValue(null));

        $zookeeper = new Zookeeper();
        $zookeeper->setZookeeper($zookeeperMock);

        $this->assertEquals([], $zookeeper->getTopicDetail('test_topic'));
    }*/
}
