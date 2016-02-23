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
 * ClusterMetadataInterface.
 *
 * @author Axel Etcheverry <axel@etcheverry.biz>
 */
interface ClusterMetadataInterface
{
    /**
     * Get broker list from kafka metadata
     *
     * @return array
     */
    public function getBrokers();

    /**
     * @param string  $topicName
     * @param integer $partitionId
     * @return array
     */
    //public function getPartitionState($topicName, $partitionId = 0);

    /**
     * @param string $topicName
     * @return array
     */
    //public function getTopicDetail($topicName);
}
