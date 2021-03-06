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

use Euskadi31\Kafka\ClusterMetadata\ClusterMetadataInterface;

/**
 * Kafka Producer.
 *
 * @author Axel Etcheverry <axel@etcheverry.biz>
 */
class Producer extends \RdKafka\Producer
{
    /**
     *
     * @param ClusterMetadataInterface|null $clusterMetadata
     * @param \RdKafka\Conf|null            $config
     */
    public function __construct(ClusterMetadataInterface $clusterMetadata = null, \RdKafka\Conf $config = null)
    {
        if (empty($config)) {
            $config = new \RdKafka\Conf();
        }

        if (!empty($clusterMetadata)) {
            $brokers = $clusterMetadata->getBrokers();
            $brokers = array_map(function($broker) {
                return sprintf('%s:%d', $broker['host'], $broker['port']);
            }, $brokers);

            $config->set('metadata.broker.list', implode(',', $brokers));
        }

        parent::__construct($config);
    }
}
