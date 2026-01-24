from kafka import ConsumerRebalanceListener, TopicPartition, OffsetAndMetadata
# from kafka.strcut import OffsetAndMetadata

current_offset = {}

class ConsumerRebalanceListenerHandler(ConsumerRebalanceListener):
    def __init__(self, consumer):
        self.consumer = consumer 

    def get_current_offset(self):
        return current_offset
    
    def add_offset(self, topic, partition, offset):
        key = TopicPartition(topic, partition)
        current_offset[key] = OffsetAndMetadata(offset, 'commit')

    def on_partition_revoked(self, revoked):
        self.consumer.commit(self.get_current_offset())
        current_offset = {}
        

    