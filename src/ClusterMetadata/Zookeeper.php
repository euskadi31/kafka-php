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
     * get topic detail
     */
    const TOPIC_PATCH = '/brokers/topics/%s';

    /**
     * get partition state
     */
    const PARTITION_STATE = '/brokers/topics/%s/partitions/%d/state';

    /**
     * register consumer
     */
    const REG_CONSUMER = '/consumers/%s/ids/%s';

    /**
     * list consumer
     */
    const LIST_CONSUMER = '/consumers/%s/ids';

    /**
     * partition owner
     */
    const PARTITION_OWNER = '/consumers/%s/owners/%s/%d';

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
        $lists  = $this->zookeeper->getChildren(self::BROKER_PATH);

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

        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);

            if (empty($result)) {
                return [];
            }

            $result = json_decode($result, true);
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    /*public function getPartitionState($topicName, $partitionId = 0)
    {
        $result = [];
        $path   = sprintf(self::PARTITION_STATE, (string) $topicName, (int) $partitionId);

        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);

            if (empty($result)) {
                return [];
            }

            $result = json_decode($result, true);
        }

        return $result;
    }*/

    /**
     * {@inheritDoc}
     */
    /*public function getTopicDetail($topicName)
    {
        $result = [];
        $path   = sprintf(self::TOPIC_PATCH, (string) $topicName);

        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);

            if (empty($result)) {
                return [];
            }

            $result = json_decode($result, true);
        }

        return $result;
    }*/

    /**
     * Register consumer
     *
     * @param string  $groupId
     * @param integer $consumerId
     * @param array   $topics
     * @return Euskadi31\Kafka\ClusterMetadata\Zookeeper
     */
    /*public function registerConsumer($groupId, $consumerId, $topics = [])
    {
        if (empty($topics)) {
            return;
        }

        $path       = sprintf(self::REG_CONSUMER, (string) $groupId, (string) $consumerId);
        $subData    = [];

        foreach ($topics as $topic) {
            $subData[$topic] = 1;
        }

        $data = [
            'version'       => '1',
            'pattern'       => 'white_list',
            'subscription'  => $subData,
        ];

        if (!$this->zookeeper->exists($path)) {
            $this->makeZkPath($path);
            $this->makeZkNode($path, json_encode($data));
        } else {
            $this->zookeeper->set($path, json_encode($data));
        }

        return $this;
    }*/

    /**
     * List consumer
     *
     * @param string $groupId
     * @return array
     */
    /*public function listConsumer($groupId)
    {
        $path = sprintf(self::LIST_CONSUMER, (string) $groupId);

        if (!$this->zookeeper->exists($path)) {
            return [];
        } else {
            return $this->zookeeper->getChildren($path);
        }
    }*/

    /**
     * Get consumer per topic
     *
     * @param string $groupId
     * @return array
     */
    /*public function getConsumersPerTopic($groupId)
    {
        $consumers = $this->listConsumer($groupId);

        if (empty($consumers)) {
            return [];
        }

        $topics = [];

        foreach ($consumers as $consumerId) {
            $path = sprintf(self::REG_CONSUMER, (string) $groupId, (string) $consumerId);

            if (!$this->zookeeper->exists($path)) {
                continue;
            }

            $info = $this->zookeeper->get($path);
            $info = json_decode($info, true);
            $subTopic = isset($info['subscription']) ? $info['subscription'] : [];

            foreach ($subTopic as $topic => $num) {
                $topics[$topic] = $consumerId;
            }
        }

        return $topics;
    }*/

    /**
     * Add partition owner
     *
     * @param string  $groupId
     * @param string  $topicName
     * @param integer $partitionId
     * @param string  $consumerId
     * @return Euskadi31\Kafka\ClusterMetadata\Zookeeper
     */
    /*public function addPartitionOwner($groupId, $topicName, $partitionId, $consumerId)
    {
        $path = sprintf(self::PARTITION_OWNER, (string) $groupId, $topicName, (string) $partitionId);

        if (!$this->zookeeper->exists($path)) {
            $this->makeZkPath($path);
            $this->makeZkNode($path, $consumerId);
        } else {
            $this->zookeeper->set($path, $consumerId);
        }

        return $this;
    }*/

    /**
     * Equivalent of "mkdir -p" on ZooKeeper
     *
     * @param string $path  The path to the node
     * @param mixed  $value The value to assign to each new node along the path
     * @return bool
     */
    /*protected function makeZkPath($path, $value = 0)
    {
        $parts      = explode('/', $path);
        $parts      = array_filter($parts);
        $subpath    = '';

        while (count($parts) > 1) {
            $subpath .= '/' . array_shift($parts);

            if (!$this->zookeeper->exists($subpath)) {
                $this->makeZkNode($subpath, $value);
            }
        }
    }*/

    /**
     * Create a node on ZooKeeper at the given path
     *
     * @param string $path  The path to the node
     * @param mixed  $value The value to assign to the new node
     * @return bool
     */
    /*protected function makeZkNode($path, $value)
    {
        $params = [
            [
                'perms'  => \Zookeeper::PERM_ALL,
                'scheme' => 'world',
                'id'     => 'anyone',
            ]
        ];

        return $this->zookeeper->create($path, $value, $params);
    }*/
}
