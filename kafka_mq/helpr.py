class Helper:

    def partitioner(self, id, maxPartition=None):
        if id is None:
            return None
        if maxPartition is None:
            maxPartition = 10
        i0 = id[-1]
        i1 = id[-2]
        partition = (ord(i0) * ord(i1)) % maxPartition
        return partition
