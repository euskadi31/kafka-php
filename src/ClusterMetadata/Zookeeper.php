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

/**
 * Zookeeper.
 *
 * @author Axel Etcheverry <axel@etcheverry.biz>
 */
class Zookeeper implements ClusterMetadataInterface
{
    /**
     * get all broker
     */
    const BROKER_PATH = '/brokers/ids';

    /**
     * get broker detail
     */
    const BROKER_DETAIL_PATH = '/brokers/ids/%d';

    /**
     * @var integer
     */
    protected $timeout;

    /**
     * @var \ZooKeeper
     */
    protected $zookeeper;

    /**
     * @var array
     */
    protected $hosts = [];

    /**
     *
     * @param array   $hosts
     * @param integer $timeout
     */
    public function __construct(array $hosts = [], $timeout = 10000)
    {
        $this->setHosts($hosts);
        $this->setTimeout($timeout);
    }

    /**
     * Set Zookeeper timeout
     *
     * @param integer $timeout
     * @return Euskadi31\Kafka\ClusterMetadata\Zookeeper
     */
    public function setTimeout($timeout)
    {
        $this->timeout = (int) $timeout;

        return $this;
    }

    /**
     * Get Zookeeper timeout
     *
     * @return integer
     */
    public function getTimeout()
    {
        return $this->timeout;
    }


    /**
     * Set hosts
     *
     * @param array $hosts
     * @return Euskadi31\Kafka\ClusterMetadata\Zookeeper
     */
    public function setHosts(array $hosts)
    {
        foreach ($hosts as $host => $port) {
            $this->addHost($host, $port);
        }

        return $this;
    }

    /**
     * Add host
     *
     * @param string  $host
     * @param integer $port
     * @return Euskadi31\Kafka\ClusterMetadata\Zookeeper
     */
    public function addHost($host, $port = 2181)
    {
        $this->hosts[$host] = (int) $port;

        return $this;
    }

    /**
     * Get hosts
     *
     * @return array
     */
    public function getHosts()
    {
        return $this->hosts;
    }

    /**
     * Get string of hosts
     *
     * @return string
     */
    public function getHostsString()
    {
        $hosts = [];

        foreach ($this->hosts as $host => $port) {
            $hosts[] = sprintf('%s:%d', $host, $port);
        }

        return join(',', $hosts);
    }

    /**
     * Set Zookeeper client
     *
     * @param \ZooKeeper $zookeeper
     * @return Euskadi31\Kafka\ClusterMetadata\Zookeeper
     */
    public function setZookeeper(\ZooKeeper $zookeeper)
    {
        $this->zookeeper = $zookeeper;

        return $this;
    }

    /**
     * Get zookeeper client
     *
     * @return \ZooKeeper
     */
    public function getZookeeper()
    {
        if (empty($this->zookeeper)) {
            $this->zookeeper = new \ZooKeeper($this->getHostsString(), null, (int) $this->timeout);
        }

        return $this->zookeeper;
    }


    /**
     * {@inheritDoc}
     */
    public function getBrokers()
    {
        $result = [];
        $lists  = $this->getZookeeper()->getChildren(self::BROKER_PATH);

        if (!empty($lists)) {
            foreach ($lists as $brokerId) {
                $brokerDetail = $this->getBrokerDetail($brokerId);

                if (empty($brokerDetail)) {
                    continue;
                }

                $result[$brokerId] = $brokerDetail;
            }
        }

        return $result;
    }

    /**
     * Get broker detail
     *
     * @param  integer $brokerId
     * @return array
     */
    public function getBrokerDetail($brokerId)
    {
        $result = [];
        $path   = sprintf(self::BROKER_DETAIL_PATH, (int) $brokerId);

        if ($this->getZookeeper()->exists($path)) {
            $result = $this->getZookeeper()->get($path);

            if (empty($result)) {
                return [];
            }

            $result = json_decode($result, true);
        }

        return $result;
    }
}
